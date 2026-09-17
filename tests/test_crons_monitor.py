import threading
import time
from unittest import mock

import pytest

import sentry_sdk
from sentry_sdk.crons import MonitorStatus, MonitorTimeoutError, monitor
from sentry_sdk.crons.decorator import _active_states


def _check_ins(envelopes):
    """Flatten captured envelopes into check-in payload dicts."""
    out = []
    for envelope in envelopes:
        for item in envelope.items:
            payload = item.payload.json
            if payload.get("type") == "check_in":
                out.append(payload)
    return out


def _errors(envelopes):
    """Flatten captured envelopes into error-event payload dicts."""
    out = []
    for envelope in envelopes:
        for item in envelope.items:
            payload = item.payload.json
            # Error events omit an explicit type; they carry an "exception".
            if payload.get("type") in (None, "event") and "exception" in payload:
                out.append(payload)
    return out


def _wait_for(predicate, timeout=2.0, interval=0.005):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# ---------------------------------------------------------------------------
# Status reporting: start / normal end / failure / timeout are distinguishable
# ---------------------------------------------------------------------------


def test_statuses_ok_and_error_and_in_progress(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    with monitor(monitor_slug="m"):
        pass

    with pytest.raises(ValueError):
        with monitor(monitor_slug="m"):
            raise ValueError("boom")

    checkins = _check_ins(envelopes)
    statuses = [(c["check_in_id"], c["status"]) for c in checkins]

    assert statuses == [
        (checkins[0]["check_in_id"], MonitorStatus.IN_PROGRESS),
        (checkins[0]["check_in_id"], MonitorStatus.OK),
        (checkins[2]["check_in_id"], MonitorStatus.IN_PROGRESS),
        (checkins[2]["check_in_id"], MonitorStatus.ERROR),
    ]
    # The two runs must not share an id.
    assert checkins[0]["check_in_id"] != checkins[2]["check_in_id"]


def test_sync_timeout_reports_timeout_status(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    @monitor(monitor_slug="slow", timeout_s=0.05)
    def slow_job():
        # Keep running beyond the timeout; the watchdog reports from another
        # thread but must not interrupt this job.
        time.sleep(0.25)

    slow_job()

    assert _wait_for(lambda: len(_check_ins(envelopes)) >= 2)
    checkins = _check_ins(envelopes)

    assert [c["status"] for c in checkins] == [
        MonitorStatus.IN_PROGRESS,
        MonitorStatus.TIMEOUT,
    ]
    assert checkins[1]["check_in_id"] == checkins[0]["check_in_id"]
    assert checkins[1]["duration"] >= 0.05


def test_sync_timeout_is_explained_by_an_error_event(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    @monitor(monitor_slug="slow", timeout_s=0.05)
    def slow_job():
        time.sleep(0.2)

    slow_job()

    assert _wait_for(lambda: len(_errors(envelopes)) >= 1)
    (error,) = _errors(envelopes)

    assert error["exception"]["values"][0]["type"] == "MonitorTimeoutError"
    assert error["contexts"]["monitor"]["status"] == MonitorStatus.TIMEOUT
    assert error["contexts"]["monitor"]["slug"] == "slow"
    assert error["contexts"]["monitor"]["check_in_id"]
    assert error["tags"] == {"monitor.status": "timeout"}


@pytest.mark.asyncio
async def test_async_timeout_reports_timeout_and_raises(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    @monitor(monitor_slug="slow-async", timeout_s=0.05)
    async def slow_job():
        await _sleep_cancelable(0.3)

    with pytest.raises(MonitorTimeoutError):
        await slow_job()

    checkins = _check_ins(envelopes)
    assert [c["status"] for c in checkins] == [
        MonitorStatus.IN_PROGRESS,
        MonitorStatus.TIMEOUT,
    ]

    (error,) = _errors(envelopes)
    assert error["exception"]["values"][0]["type"] == "MonitorTimeoutError"
    assert error["contexts"]["monitor"]["check_in_id"] == checkins[0]["check_in_id"]


async def _sleep_cancelable(seconds):
    # Plain asyncio.sleep is cancellable, which wait_for relies on.
    await __import__("asyncio").sleep(seconds)


def test_sync_job_finishing_before_timeout_reports_ok(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    @monitor(monitor_slug="fast", timeout_s=5)
    def fast_job():
        return 1

    assert fast_job() == 1

    checkins = _check_ins(envelopes)
    assert [c["status"] for c in checkins] == [
        MonitorStatus.IN_PROGRESS,
        MonitorStatus.OK,
    ]


# ---------------------------------------------------------------------------
# Concurrent replicas must never close each other's check-in
# ---------------------------------------------------------------------------


def test_concurrent_threads_get_independent_checkin_ids(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    mon = monitor(monitor_slug="shared")
    barrier = threading.Barrier(2)
    results = {}

    def run(name, fail):
        try:
            with mon:
                state = _active_states.get()[-1]
                results[name] = state.check_in_id
                barrier.wait()
                time.sleep(0.02)
                if fail:
                    raise ValueError(name)
        except ValueError:
            # Swallow after the monitor context observed it, so the failure
            # never becomes an unhandled thread exception.
            pass

    t1 = threading.Thread(target=run, args=("a", False))
    t2 = threading.Thread(target=run, args=("b", True))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    assert results["a"] != results["b"]

    checkins = _check_ins(envelopes)
    # Exactly one in_progress + one terminal check-in per run, each under its
    # own id — no run closed the other run's check-in.
    by_id = {}
    for c in checkins:
        by_id.setdefault(c["check_in_id"], []).append(c["status"])

    assert set(by_id) == {results["a"], results["b"]}
    assert by_id[results["a"]] == [MonitorStatus.IN_PROGRESS, MonitorStatus.OK]
    assert by_id[results["b"]] == [MonitorStatus.IN_PROGRESS, MonitorStatus.ERROR]


@pytest.mark.asyncio
async def test_concurrent_asyncio_tasks_get_independent_checkin_ids(
    sentry_init, capture_envelopes
):
    sentry_init()
    envelopes = capture_envelopes()

    mon = monitor(monitor_slug="shared-async")
    ids = {}

    async def run(name):
        with mon:
            state = _active_states.get()[-1]
            ids[name] = state.check_in_id
            # Interleave with the other task while both are active.
            await __import__("asyncio").sleep(0)

    import asyncio

    await asyncio.gather(run("a"), run("b"))

    assert ids["a"] != ids["b"]
    checkins = _check_ins(envelopes)
    terminal_ids = {
        c["check_in_id"] for c in checkins if c["status"] == MonitorStatus.OK
    }
    assert terminal_ids == {ids["a"], ids["b"]}


def test_contextvar_is_clean_after_exit(sentry_init):
    sentry_init()
    with monitor(monitor_slug="m"):
        assert len(_active_states.get()) == 1
    assert _active_states.get() == ()


# ---------------------------------------------------------------------------
# Failure explainability: error event is linked to the failing check-in
# ---------------------------------------------------------------------------


def test_failure_carries_exception_and_monitor_context(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    with pytest.raises(KeyError):
        with monitor(monitor_slug="m"):
            raise KeyError("missing")

    (checkin_error,) = [
        c for c in _check_ins(envelopes) if c["status"] == MonitorStatus.ERROR
    ]
    (error,) = _errors(envelopes)

    assert error["exception"]["values"][0]["type"] == "KeyError"
    monitor_ctx = error["contexts"]["monitor"]
    assert monitor_ctx["check_in_id"] == checkin_error["check_in_id"]
    assert monitor_ctx["slug"] == "m"
    assert monitor_ctx["status"] == MonitorStatus.ERROR
    assert monitor_ctx["duration"] >= 0
    assert error["tags"] == {"monitor.status": "error"}


# ---------------------------------------------------------------------------
# Reporting failures must never affect the job
# ---------------------------------------------------------------------------


def test_reporting_failure_does_not_mask_job_exception(sentry_init, monkeypatch):
    sentry_init()

    def boom(*a, **k):
        raise RuntimeError("reporting is broken")

    monkeypatch.setattr("sentry_sdk.crons.decorator.capture_checkin", boom)

    with pytest.raises(ValueError) as excinfo:
        with monitor(monitor_slug="m"):
            raise ValueError("the real failure")

    assert str(excinfo.value) == "the real failure"


def test_reporting_failure_on_success_does_not_raise(sentry_init, monkeypatch):
    sentry_init()

    def boom(*a, **k):
        raise RuntimeError("reporting is broken")

    monkeypatch.setattr("sentry_sdk.crons.decorator.capture_checkin", boom)

    with monitor(monitor_slug="m"):
        pass  # no exception may escape


def test_capture_checkin_swallows_internal_errors(sentry_init, monkeypatch):
    sentry_init()
    monkeypatch.setattr(
        "sentry_sdk.crons.api.sentry_sdk.capture_event",
        mock.Mock(side_effect=RuntimeError("transport down")),
    )

    # Must return the id instead of raising.
    check_in_id = sentry_sdk.crons.api.capture_checkin(
        monitor_slug="m", check_in_id="fixed-id", status=MonitorStatus.OK
    )
    assert check_in_id == "fixed-id"
