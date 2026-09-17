from functools import wraps
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING

from sentry_sdk.crons import capture_checkin
from sentry_sdk.crons.consts import MonitorStatus
from sentry_sdk.utils import logger, now

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
    from types import TracebackType
    from typing import (
        Any,
        Optional,
        ParamSpec,
        Type,
        TypeVar,
        Union,
        cast,
        overload,
    )

    from sentry_sdk._types import MonitorConfig

    P = ParamSpec("P")
    R = TypeVar("R")


def _capture_checkin_safely(**kwargs: "Any") -> "Optional[str]":
    """
    Capture a check-in without ever raising.

    Reporting about a scheduled task must never break the task itself,
    so any error in the reporting path is logged and swallowed.
    Returns the check-in id, or `None` if the check-in could not be captured.
    """
    try:
        return capture_checkin(**kwargs)
    except Exception:
        logger.warning(
            "[Crons] Failed to capture check-in for monitor '%s'",
            kwargs.get("monitor_slug"),
            exc_info=True,
        )
        return None


class monitor:  # noqa: N801
    """
    Decorator/context manager to capture checkin events for a monitor.

    Each run of the monitored task reports an `in_progress` check-in when it
    starts and a closing check-in when it finishes. The closing status is `ok`
    if the task returned normally and `error` if it raised. If a run stays
    `in_progress` for longer than `max_runtime` minutes, Sentry marks the
    check-in as timed out, so stuck or killed tasks are distinguishable from
    plain failures.

    The decorator is safe to use on tasks that run concurrently (threads,
    asyncio, multiple replicas): every run gets its own check-in id and never
    overwrites another run's check-in.

    Exceptions raised by the task always propagate unchanged and never prevent
    the closing check-in from being sent. Conversely, a failure in the
    reporting itself never breaks the task.

    Usage (as decorator):
    ```
    import sentry_sdk

    app = Celery()

    @app.task
    @sentry_sdk.monitor(monitor_slug='my-fancy-slug')
    def test(arg):
        print(arg)
    ```

    This does not have to be used with Celery, but if you do use it with celery,
    put the `@sentry_sdk.monitor` decorator below Celery's `@app.task` decorator.

    Usage (as context manager):
    ```
    import sentry_sdk

    def test(arg):
        with sentry_sdk.monitor(monitor_slug='my-fancy-slug'):
            print(arg)
    ```

    The context manager returns the monitor instance, so the current run's
    `check_in_id` is available for correlating the run with other data:

    ```
    with sentry_sdk.monitor(monitor_slug='my-fancy-slug') as monitor:
        do_work()
        logger.info("work done", extra={"check_in_id": monitor.check_in_id})
    ```

    To find out why a run was reported as failed, look up the check-in in
    Sentry: it shares a trace with the error events captured during the run,
    so the related exception is linked from the check-in (and vice versa).
    """

    def __init__(
        self,
        monitor_slug: "Optional[str]" = None,
        monitor_config: "Optional[MonitorConfig]" = None,
        max_runtime: "Optional[int]" = None,
    ) -> None:
        self.monitor_slug = monitor_slug
        self.monitor_config = monitor_config
        if max_runtime is not None:
            # Copy the config instead of mutating the caller's dict, which may
            # be shared between concurrent runs of the same task.
            merged_config: "MonitorConfig" = {}
            if monitor_config is not None:
                merged_config.update(monitor_config)
            merged_config["max_runtime"] = max_runtime
            self.monitor_config = merged_config

    def __enter__(self) -> "monitor":
        self.start_timestamp = now()
        self.check_in_id = _capture_checkin_safely(
            monitor_slug=self.monitor_slug,
            status=MonitorStatus.IN_PROGRESS,
            monitor_config=self.monitor_config,
        )
        return self

    def __exit__(
        self,
        exc_type: "Optional[Type[BaseException]]",
        exc_value: "Optional[BaseException]",
        traceback: "Optional[TracebackType]",
    ) -> None:
        # Read the run state defensively: even if __enter__ failed halfway,
        # the closing check-in must still be attempted.
        start_timestamp = getattr(self, "start_timestamp", None)
        duration_s = now() - start_timestamp if start_timestamp is not None else None
        check_in_id = getattr(self, "check_in_id", None)

        if exc_type is None and exc_value is None and traceback is None:
            status = MonitorStatus.OK
        else:
            status = MonitorStatus.ERROR
            logger.debug(
                "[Crons] Monitor '%s' (check-in %s) failed%s: %s",
                self.monitor_slug,
                check_in_id,
                (" after %.2fs" % duration_s if duration_s is not None else ""),
                exc_value if exc_value is not None else exc_type,
            )

        max_runtime = None
        if self.monitor_config is not None:
            max_runtime = self.monitor_config.get("max_runtime")
        if (
            max_runtime is not None
            and duration_s is not None
            and duration_s > 60 * max_runtime
        ):
            logger.debug(
                "[Crons] Monitor '%s' (check-in %s) ran for %.2fs, exceeding "
                "its max_runtime of %s minutes. Sentry will mark the check-in "
                "as timed out.",
                self.monitor_slug,
                check_in_id,
                duration_s,
                max_runtime,
            )

        _capture_checkin_safely(
            monitor_slug=self.monitor_slug,
            check_in_id=check_in_id,
            status=status,
            duration=duration_s,
            monitor_config=self.monitor_config,
        )
        # Do not return True here: an exception raised by the task must
        # propagate unchanged.

    def _new_run(self) -> "monitor":
        # Every run of the task gets its own monitor instance (and thus its
        # own check-in id), so concurrent runs of the same task never
        # overwrite each other's check-in state.
        return type(self)(
            monitor_slug=self.monitor_slug,
            monitor_config=self.monitor_config,
        )

    if TYPE_CHECKING:

        @overload
        def __call__(
            self, fn: "Callable[P, Awaitable[Any]]"
        ) -> "Callable[P, Awaitable[Any]]":
            # Unfortunately, mypy does not give us any reliable way to type check the
            # return value of an Awaitable (i.e. async function) for this overload,
            # since calling iscouroutinefunction narrows the type to Callable[P, Awaitable[Any]].
            ...

        @overload
        def __call__(self, fn: "Callable[P, R]") -> "Callable[P, R]": ...

    def __call__(
        self,
        fn: "Union[Callable[P, R], Callable[P, Awaitable[Any]]]",
    ) -> "Union[Callable[P, R], Callable[P, Awaitable[Any]]]":
        if iscoroutinefunction(fn):
            return self._async_wrapper(fn)

        else:
            if TYPE_CHECKING:
                fn = cast("Callable[P, R]", fn)
            return self._sync_wrapper(fn)

    def _async_wrapper(
        self, fn: "Callable[P, Awaitable[Any]]"
    ) -> "Callable[P, Awaitable[Any]]":
        @wraps(fn)
        async def inner(*args: "P.args", **kwargs: "P.kwargs") -> "R":
            with self._new_run():
                return await fn(*args, **kwargs)

        return inner

    def _sync_wrapper(self, fn: "Callable[P, R]") -> "Callable[P, R]":
        @wraps(fn)
        def inner(*args: "P.args", **kwargs: "P.kwargs") -> "R":
            with self._new_run():
                return fn(*args, **kwargs)

        return inner
