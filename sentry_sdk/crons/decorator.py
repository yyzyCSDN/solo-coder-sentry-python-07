import contextvars
import threading
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


class MonitorTimeoutError(Exception):
    """
    Raised by the `@sentry_sdk.monitor` decorator when a monitored job does not
    finish within the configured `timeout_s`.

    This makes the timeout observable to the caller (and to error tracking),
    while the corresponding check-in is reported with status
    `MonitorStatus.TIMEOUT`.
    """


class _CheckInState:
    """Per-invocation state for a single check-in.

    A fresh instance is created on every ``__enter__`` and tracked in a
    ContextVar (rather than on the decorator object), so the same decorator
    instance can be entered concurrently (threads, multiple copies of a job,
    overlapping runs) without two runs overwriting each other's check-in ids
    or timings.
    """

    __slots__ = (
        "check_in_id",
        "start_timestamp",
        "finished",
        "_lock",
        "_timer",
    )

    def __init__(self, check_in_id, start_timestamp):
        # type: (Optional[str], float) -> None
        self.check_in_id = check_in_id
        self.start_timestamp = start_timestamp
        self.finished = False
        self._lock = threading.Lock()
        self._timer = None  # type: Optional[threading.Timer]

    def finish(self):
        # type: () -> bool
        """Claim the right to report the terminal check-in.

        Returns ``True`` for exactly one caller, so a timeout watchdog firing
        while the job is tearing down can never double-report.
        """
        with self._lock:
            if self.finished:
                return False
            self.finished = True
            return True


# Stack of active states, isolated per thread / asyncio task by contextvars.
# An immutable tuple is used and rebound on every entry so that concurrent
# asyncio tasks sharing a copied parent context never mutate one another's
# stack: each ``set`` writes to that task's own context copy.
_active_states = contextvars.ContextVar("sentry_monitor_active_states", default=())

_TIMEOUT_MESSAGE = (
    "Monitor '{slug}' did not finish within the configured timeout of {timeout}s "
    "(elapsed: {elapsed:.3f}s)"
)


class monitor:  # noqa: N801
    """
    Decorator/context manager to capture checkin events for a monitor.

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

    The following check-in statuses are reported and can be distinguished:

    - `MonitorStatus.IN_PROGRESS` when the job starts
    - `MonitorStatus.OK` when it returns normally
    - `MonitorStatus.ERROR` when it raises an exception
    - `MonitorStatus.TIMEOUT` when it runs longer than `timeout_s` seconds

    Pass `timeout_s` to enforce a client-side timeout. For synchronous jobs a
    watchdog timer reports the timeout check-in from a background thread; it
    cannot forcibly stop the job (Python offers no safe way to interrupt
    another thread), so the reported timeout is what signals the overrun while
    the job itself keeps running to completion. For coroutines the job is
    cancelled and `MonitorTimeoutError` is raised to the awaiting caller; the
    captured error event carries the same reason. The same decorator can be
    entered by multiple overlapping copies of a job: every run tracks its own
    check-in id, so runs can never close each other's check-in.

    Reporting itself is best-effort and fully isolated from the job: a failure
    while capturing a check-in is logged and never masks the job's own
    exception. When a job fails (or times out), the exception is captured with a
    `monitor` context linking it to this check-in id, which is what makes each
    failed run explainable afterwards.
    """

    def __init__(
        self,
        monitor_slug: "Optional[str]" = None,
        monitor_config: "Optional[MonitorConfig]" = None,
        timeout_s: "Optional[float]" = None,
    ) -> None:
        self.monitor_slug = monitor_slug
        self.monitor_config = monitor_config
        self.timeout_s = timeout_s

    def __enter__(self) -> "_CheckInState":
        start_timestamp = now()

        check_in_id = None
        try:
            check_in_id = capture_checkin(
                monitor_slug=self.monitor_slug,
                status=MonitorStatus.IN_PROGRESS,
                monitor_config=self.monitor_config,
            )
        except Exception:
            # The in-progress check-in must never break the monitored job.
            # An id generated here lets the terminal check-in still close a run.
            logger.exception(
                "[Crons] Failed to capture in_progress check-in for monitor %s",
                self.monitor_slug,
            )
            check_in_id = _generate_check_in_id()

        state = _CheckInState(
            check_in_id=check_in_id,
            start_timestamp=start_timestamp,
        )

        _active_states.set(_active_states.get() + (state,))

        return state

    def __exit__(
        self,
        exc_type: "Optional[Type[BaseException]]",
        exc_value: "Optional[BaseException]",
        traceback: "Optional[TracebackType]",
    ) -> None:
        states = _active_states.get()
        state = states[-1] if states else None

        # Always detach this run's state before reporting.
        if state is not None:
            _active_states.set(states[:-1])

        # Cancel a pending watchdog. If it already fired and reported,
        # ``finish()`` returns False here and no second check-in is sent.
        timer = getattr(state, "_timer", None) if state is not None else None
        if timer is not None:
            timer.cancel()

        if state is not None and state.finish():
            self._report_terminal(state, exc_type, exc_value)

        # Never suppress the job's exception; reporting is best-effort only.
        return None

    def _report_terminal(self, state, exc_type, exc_value):
        # type: (_CheckInState, Optional[Type[BaseException]], Optional[BaseException]) -> None
        duration_s = now() - state.start_timestamp

        if exc_type is None:
            status = MonitorStatus.OK
        elif issubclass(exc_type, MonitorTimeoutError):
            status = MonitorStatus.TIMEOUT
        else:
            status = MonitorStatus.ERROR

        try:
            capture_checkin(
                monitor_slug=self.monitor_slug,
                check_in_id=state.check_in_id,
                status=status,
                duration=duration_s,
                monitor_config=self.monitor_config,
            )
        except Exception:
            logger.exception(
                "[Crons] Failed to capture %s check-in for monitor %s",
                status,
                self.monitor_slug,
            )

        # Capture the failure cause linked to this check-in, so a failed run
        # can be explained afterwards. Only for genuine failures and only for
        # `Exception` subclasses (let things like KeyboardInterrupt pass
        # untouched). Wrapped in try/except so tracking can never mask the job.
        if status in (MonitorStatus.ERROR, MonitorStatus.TIMEOUT):
            self._capture_failure(state, status, duration_s, exc_value)

    def _capture_failure(self, state, status, duration_s, exc_value):
        # type: (_CheckInState, str, float, Optional[BaseException]) -> None
        try:
            import sentry_sdk

            monitor_context = {
                "slug": self.monitor_slug,
                "check_in_id": state.check_in_id,
                "status": status,
                "duration": duration_s,
            }

            if exc_value is None:
                # Sync watchdog timeouts do not surface an exception in the
                # job's own thread; synthesize one so the cause is recorded.
                error = MonitorTimeoutError(
                    _TIMEOUT_MESSAGE.format(
                        slug=self.monitor_slug,
                        timeout=self.timeout_s,
                        elapsed=duration_s,
                    )
                )
            else:
                error = exc_value

            with sentry_sdk.new_scope() as scope:
                scope.set_context("monitor", monitor_context)
                scope.set_tag("monitor.status", status)
                sentry_sdk.capture_exception(error)
        except Exception:
            logger.exception(
                "[Crons] Failed to capture failure cause for monitor %s",
                self.monitor_slug,
            )

    def _start_watchdog(self, state):
        # type: (_CheckInState) -> None
        """Start a timer that reports a timeout check-in if the sync job runs
        past ``timeout_s``.

        The timer runs in a dedicated thread: it only reports, it cannot kill
        the job's thread (Python offers no safe way to do that). If the job
        finishes in time, ``__exit__`` cancels the timer.
        """
        timeout_s = self.timeout_s

        def _on_timeout():
            # type: () -> None
            if not state.finish():
                # The job finished and won the race while the timer was firing.
                return

            self._report_terminal(state, MonitorTimeoutError, None)

        timer = threading.Timer(timeout_s, _on_timeout)
        timer.daemon = True
        state._timer = timer  # type: ignore[attr-defined]
        timer.start()

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
            with self as state:
                if self.timeout_s is None:
                    return await fn(*args, **kwargs)

                import asyncio

                try:
                    return await asyncio.wait_for(fn(*args, **kwargs), self.timeout_s)
                except asyncio.TimeoutError:
                    # wait_for cancelled the job; raise something concrete that
                    # __exit__ recognises as a timeout (and the caller sees).
                    raise MonitorTimeoutError(
                        _TIMEOUT_MESSAGE.format(
                            slug=self.monitor_slug,
                            timeout=self.timeout_s,
                            elapsed=now() - state.start_timestamp,
                        )
                    ) from None

        return inner

    def _sync_wrapper(self, fn: "Callable[P, R]") -> "Callable[P, R]":
        @wraps(fn)
        def inner(*args: "P.args", **kwargs: "P.kwargs") -> "R":
            with self as state:
                if self.timeout_s is not None:
                    self._start_watchdog(state)
                return fn(*args, **kwargs)

        return inner


def _generate_check_in_id():
    # type: () -> str
    import uuid

    return uuid.uuid4().hex
