import asyncio
import threading
import uuid
from unittest import mock

import pytest

import sentry_sdk
from sentry_sdk.crons import capture_checkin


@sentry_sdk.monitor(monitor_slug="abc123")
def _hello_world(name):
    return "Hello, {}".format(name)


@sentry_sdk.monitor(monitor_slug="def456")
def _break_world(name):
    1 / 0
    return "Hello, {}".format(name)


def _hello_world_contextmanager(name):
    with sentry_sdk.monitor(monitor_slug="abc123"):
        return "Hello, {}".format(name)


def _break_world_contextmanager(name):
    with sentry_sdk.monitor(monitor_slug="def456"):
        1 / 0
        return "Hello, {}".format(name)


@sentry_sdk.monitor(monitor_slug="abc123")
async def _hello_world_async(name):
    return "Hello, {}".format(name)


@sentry_sdk.monitor(monitor_slug="def456")
async def _break_world_async(name):
    1 / 0
    return "Hello, {}".format(name)


async def my_coroutine():
    return


async def _hello_world_contextmanager_async(name):
    with sentry_sdk.monitor(monitor_slug="abc123"):
        await my_coroutine()
        return "Hello, {}".format(name)


async def _break_world_contextmanager_async(name):
    with sentry_sdk.monitor(monitor_slug="def456"):
        await my_coroutine()
        1 / 0
        return "Hello, {}".format(name)


@sentry_sdk.monitor(monitor_slug="ghi789", monitor_config=None)
def _no_monitor_config():
    return


@sentry_sdk.monitor(
    monitor_slug="ghi789",
    monitor_config={
        "schedule": {"type": "crontab", "value": "0 0 * * *"},
        "failure_issue_threshold": 5,
    },
)
def _with_monitor_config():
    return


def test_decorator(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin"
    ) as fake_capture_checkin:
        result = _hello_world("Grace")
        assert result == "Hello, Grace"

        # Check for initial checkin
        fake_capture_checkin.assert_has_calls(
            [
                mock.call(
                    monitor_slug="abc123", status="in_progress", monitor_config=None
                ),
            ]
        )

        # Check for final checkin
        assert fake_capture_checkin.call_args[1]["monitor_slug"] == "abc123"
        assert fake_capture_checkin.call_args[1]["status"] == "ok"
        assert fake_capture_checkin.call_args[1]["duration"]
        assert fake_capture_checkin.call_args[1]["check_in_id"]


def test_decorator_error(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin"
    ) as fake_capture_checkin:
        with pytest.raises(ZeroDivisionError):
            result = _break_world("Grace")

        assert "result" not in locals()

        # Check for initial checkin
        fake_capture_checkin.assert_has_calls(
            [
                mock.call(
                    monitor_slug="def456", status="in_progress", monitor_config=None
                ),
            ]
        )

        # Check for final checkin
        assert fake_capture_checkin.call_args[1]["monitor_slug"] == "def456"
        assert fake_capture_checkin.call_args[1]["status"] == "error"
        assert fake_capture_checkin.call_args[1]["duration"]
        assert fake_capture_checkin.call_args[1]["check_in_id"]


def test_contextmanager(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin"
    ) as fake_capture_checkin:
        result = _hello_world_contextmanager("Grace")
        assert result == "Hello, Grace"

        # Check for initial checkin
        fake_capture_checkin.assert_has_calls(
            [
                mock.call(
                    monitor_slug="abc123", status="in_progress", monitor_config=None
                ),
            ]
        )

        # Check for final checkin
        assert fake_capture_checkin.call_args[1]["monitor_slug"] == "abc123"
        assert fake_capture_checkin.call_args[1]["status"] == "ok"
        assert fake_capture_checkin.call_args[1]["duration"]
        assert fake_capture_checkin.call_args[1]["check_in_id"]


def test_contextmanager_error(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin"
    ) as fake_capture_checkin:
        with pytest.raises(ZeroDivisionError):
            result = _break_world_contextmanager("Grace")

        assert "result" not in locals()

        # Check for initial checkin
        fake_capture_checkin.assert_has_calls(
            [
                mock.call(
                    monitor_slug="def456", status="in_progress", monitor_config=None
                ),
            ]
        )

        # Check for final checkin
        assert fake_capture_checkin.call_args[1]["monitor_slug"] == "def456"
        assert fake_capture_checkin.call_args[1]["status"] == "error"
        assert fake_capture_checkin.call_args[1]["duration"]
        assert fake_capture_checkin.call_args[1]["check_in_id"]


def test_capture_checkin_simple(sentry_init):
    sentry_init()

    check_in_id = capture_checkin(
        monitor_slug="abc123",
        check_in_id="112233",
        status=None,
        duration=None,
    )
    assert check_in_id == "112233"


def test_sample_rate_doesnt_affect_crons(sentry_init, capture_envelopes):
    sentry_init(sample_rate=0)
    envelopes = capture_envelopes()

    capture_checkin(check_in_id="112233")

    assert len(envelopes) == 1

    check_in = envelopes[0].items[0].payload.json
    assert check_in["check_in_id"] == "112233"


def test_capture_checkin_new_id(sentry_init):
    sentry_init()

    with mock.patch("uuid.uuid4") as mock_uuid:
        mock_uuid.return_value = uuid.UUID("a8098c1a-f86e-11da-bd1a-00112444be1e")
        check_in_id = capture_checkin(
            monitor_slug="abc123",
            check_in_id=None,
            status=None,
            duration=None,
        )

        assert check_in_id == "a8098c1af86e11dabd1a00112444be1e"


def test_end_to_end(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    capture_checkin(
        monitor_slug="abc123",
        check_in_id="112233",
        duration=123,
        status="ok",
    )

    check_in = envelopes[0].items[0].payload.json

    # Check for final checkin
    assert check_in["check_in_id"] == "112233"
    assert check_in["monitor_slug"] == "abc123"
    assert check_in["status"] == "ok"
    assert check_in["duration"] == 123


def test_monitor_config(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    monitor_config = {
        "schedule": {"type": "crontab", "value": "0 0 * * *"},
        "failure_issue_threshold": 5,
        "recovery_threshold": 5,
    }

    capture_checkin(monitor_slug="abc123", monitor_config=monitor_config)
    check_in = envelopes[0].items[0].payload.json

    # Check for final checkin
    assert check_in["monitor_slug"] == "abc123"
    assert check_in["monitor_config"] == monitor_config

    # Without passing a monitor_config the field is not in the checkin
    capture_checkin(monitor_slug="abc123")
    check_in = envelopes[1].items[0].payload.json

    assert check_in["monitor_slug"] == "abc123"
    assert "monitor_config" not in check_in


def test_monitor_config_with_owner(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    monitor_config = {
        "schedule": {"type": "crontab", "value": "0 0 * * *"},
        "owner": "team:6",
    }

    capture_checkin(monitor_slug="abc123", monitor_config=monitor_config)
    check_in = envelopes[0].items[0].payload.json

    assert check_in["monitor_slug"] == "abc123"
    assert check_in["monitor_config"]["owner"] == "team:6"


def test_decorator_monitor_config(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    _with_monitor_config()

    assert len(envelopes) == 2

    for check_in_envelope in envelopes:
        assert len(check_in_envelope.items) == 1
        check_in = check_in_envelope.items[0].payload.json

        assert check_in["monitor_slug"] == "ghi789"
        assert check_in["monitor_config"] == {
            "schedule": {"type": "crontab", "value": "0 0 * * *"},
            "failure_issue_threshold": 5,
        }


def test_decorator_no_monitor_config(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    _no_monitor_config()

    assert len(envelopes) == 2

    for check_in_envelope in envelopes:
        assert len(check_in_envelope.items) == 1
        check_in = check_in_envelope.items[0].payload.json

        assert check_in["monitor_slug"] == "ghi789"
        assert "monitor_config" not in check_in


def test_capture_checkin_sdk_not_initialized():
    # Tests that the capture_checkin does not raise an error when Sentry SDK is not initialized.
    # sentry_init() is intentionally omitted.
    check_in_id = capture_checkin(
        monitor_slug="abc123",
        check_in_id="112233",
        status=None,
        duration=None,
    )
    assert check_in_id == "112233"


def test_scope_data_in_checkin(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    valid_keys = [
        # Mandatory event keys
        "type",
        "event_id",
        "timestamp",
        "platform",
        # Optional event keys
        "release",
        "environment",
        "server_name",
        "sdk",
        # Mandatory check-in specific keys
        "check_in_id",
        "monitor_slug",
        "status",
        # Optional check-in specific keys
        "duration",
        "monitor_config",
        "contexts",  # an event processor adds this
    ]

    # Add some data to the scope
    sentry_sdk.add_breadcrumb(message="test breadcrumb")
    sentry_sdk.set_context("test_context", {"test_key": "test_value"})
    sentry_sdk.set_extra("test_extra", "test_value")
    sentry_sdk.set_level("warning")
    sentry_sdk.set_tag("test_tag", "test_value")

    capture_checkin(
        monitor_slug="abc123",
        check_in_id="112233",
        status="ok",
        duration=123,
    )

    (envelope,) = envelopes
    check_in_event = envelope.items[0].payload.json

    invalid_keys = []
    for key in check_in_event.keys():
        if key not in valid_keys:
            invalid_keys.append(key)

    assert len(invalid_keys) == 0, "Unexpected keys found in checkin: {}".format(
        invalid_keys
    )


@pytest.mark.asyncio
async def test_decorator_async(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin"
    ) as fake_capture_checkin:
        result = await _hello_world_async("Grace")
        assert result == "Hello, Grace"

        # Check for initial checkin
        fake_capture_checkin.assert_has_calls(
            [
                mock.call(
                    monitor_slug="abc123", status="in_progress", monitor_config=None
                ),
            ]
        )

        # Check for final checkin
        assert fake_capture_checkin.call_args[1]["monitor_slug"] == "abc123"
        assert fake_capture_checkin.call_args[1]["status"] == "ok"
        assert fake_capture_checkin.call_args[1]["duration"]
        assert fake_capture_checkin.call_args[1]["check_in_id"]


@pytest.mark.asyncio
async def test_decorator_error_async(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin"
    ) as fake_capture_checkin:
        with pytest.raises(ZeroDivisionError):
            result = await _break_world_async("Grace")

        assert "result" not in locals()

        # Check for initial checkin
        fake_capture_checkin.assert_has_calls(
            [
                mock.call(
                    monitor_slug="def456", status="in_progress", monitor_config=None
                ),
            ]
        )

        # Check for final checkin
        assert fake_capture_checkin.call_args[1]["monitor_slug"] == "def456"
        assert fake_capture_checkin.call_args[1]["status"] == "error"
        assert fake_capture_checkin.call_args[1]["duration"]
        assert fake_capture_checkin.call_args[1]["check_in_id"]


@pytest.mark.asyncio
async def test_contextmanager_async(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin"
    ) as fake_capture_checkin:
        result = await _hello_world_contextmanager_async("Grace")
        assert result == "Hello, Grace"

        # Check for initial checkin
        fake_capture_checkin.assert_has_calls(
            [
                mock.call(
                    monitor_slug="abc123", status="in_progress", monitor_config=None
                ),
            ]
        )

        # Check for final checkin
        assert fake_capture_checkin.call_args[1]["monitor_slug"] == "abc123"
        assert fake_capture_checkin.call_args[1]["status"] == "ok"
        assert fake_capture_checkin.call_args[1]["duration"]
        assert fake_capture_checkin.call_args[1]["check_in_id"]


@pytest.mark.asyncio
async def test_contextmanager_error_async(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin"
    ) as fake_capture_checkin:
        with pytest.raises(ZeroDivisionError):
            result = await _break_world_contextmanager_async("Grace")

        assert "result" not in locals()

        # Check for initial checkin
        fake_capture_checkin.assert_has_calls(
            [
                mock.call(
                    monitor_slug="def456", status="in_progress", monitor_config=None
                ),
            ]
        )

        # Check for final checkin
        assert fake_capture_checkin.call_args[1]["monitor_slug"] == "def456"
        assert fake_capture_checkin.call_args[1]["status"] == "error"
        assert fake_capture_checkin.call_args[1]["duration"]
        assert fake_capture_checkin.call_args[1]["check_in_id"]


def _recording_fake(recorded):
    """
    A fake `capture_checkin` that records (status, check_in_id) pairs and
    hands out a fresh check-in id for every opening check-in, like the real
    implementation does.
    """

    def fake(**kwargs):
        check_in_id = kwargs.get("check_in_id") or uuid.uuid4().hex
        recorded.append((kwargs["status"], check_in_id))
        return check_in_id

    return fake


def test_concurrent_replicas_do_not_clobber_checkin_ids(sentry_init):
    sentry_init()

    both_inside = threading.Barrier(2, timeout=5)

    @sentry_sdk.monitor(monitor_slug="replicated-task")
    def task():
        # Make sure both runs are inside the task (and thus have an open
        # check-in) before either of them finishes.
        both_inside.wait()

    recorded = []
    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin",
        side_effect=_recording_fake(recorded),
    ):
        threads = [threading.Thread(target=task) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)
            assert not thread.is_alive()

    opened = {
        check_in_id for status, check_in_id in recorded if status == "in_progress"
    }
    closed = {
        check_in_id for status, check_in_id in recorded if status != "in_progress"
    }

    assert len(opened) == 2
    # Each run must close its own check-in, not the other run's.
    assert closed == opened


@pytest.mark.asyncio
async def test_concurrent_async_replicas_do_not_clobber_checkin_ids(sentry_init):
    sentry_init()

    entered = 0
    both_entered = asyncio.Event()

    @sentry_sdk.monitor(monitor_slug="async-replicated-task")
    async def task():
        nonlocal entered
        entered += 1
        if entered == 2:
            both_entered.set()
        # Make sure both runs have an open check-in before either finishes.
        await both_entered.wait()

    recorded = []
    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin",
        side_effect=_recording_fake(recorded),
    ):
        await asyncio.gather(task(), task())

    opened = {
        check_in_id for status, check_in_id in recorded if status == "in_progress"
    }
    closed = {
        check_in_id for status, check_in_id in recorded if status != "in_progress"
    }

    assert len(opened) == 2
    # Each run must close its own check-in, not the other run's.
    assert closed == opened


def test_sequential_runs_get_distinct_checkin_ids(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    _hello_world("Grace")
    _hello_world("Grace")

    check_in_ids = [
        envelope.items[0].payload.json["check_in_id"] for envelope in envelopes
    ]

    # Each run's opening and closing check-ins share one id, and the two
    # runs must not reuse each other's id.
    assert check_in_ids[0] == check_in_ids[1]
    assert check_in_ids[2] == check_in_ids[3]
    assert check_in_ids[0] != check_in_ids[2]


def test_broken_reporting_does_not_break_task(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin",
        side_effect=RuntimeError("reporting is broken"),
    ):
        assert _hello_world("Grace") == "Hello, Grace"


def test_broken_reporting_does_not_mask_task_exception(sentry_init):
    sentry_init()

    with mock.patch(
        "sentry_sdk.crons.decorator.capture_checkin",
        side_effect=RuntimeError("reporting is broken"),
    ):
        with pytest.raises(ZeroDivisionError):
            _break_world("Grace")


def test_max_runtime_is_reported_in_monitor_config(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    config = {"schedule": {"type": "crontab", "value": "0 0 * * *"}}

    @sentry_sdk.monitor(monitor_slug="nightly", monitor_config=config, max_runtime=30)
    def nightly():
        return 42

    assert nightly() == 42

    # The caller's config dict must not be mutated.
    assert config == {"schedule": {"type": "crontab", "value": "0 0 * * *"}}

    # Both the opening and the closing check-in carry the max_runtime, so a
    # run that never reports back is marked as timed out (not just failed)
    # in Sentry.
    assert len(envelopes) == 2
    for envelope in envelopes:
        check_in = envelope.items[0].payload.json
        assert check_in["monitor_config"]["max_runtime"] == 30
        assert check_in["monitor_config"]["schedule"] == {
            "type": "crontab",
            "value": "0 0 * * *",
        }


def test_run_exceeding_max_runtime_still_reports(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    @sentry_sdk.monitor(monitor_slug="slow-task", max_runtime=0)
    def slow_task():
        return "done"

    assert slow_task() == "done"

    opening, closing = (envelope.items[0].payload.json for envelope in envelopes)
    assert opening["status"] == "in_progress"
    assert opening["monitor_config"]["max_runtime"] == 0
    assert closing["status"] == "ok"
    assert closing["duration"] is not None


def test_context_manager_exposes_check_in_id(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    with sentry_sdk.monitor(monitor_slug="abc123") as m:
        pass

    opening, closing = (envelope.items[0].payload.json for envelope in envelopes)
    assert m.check_in_id == opening["check_in_id"]
    assert closing["check_in_id"] == m.check_in_id


def test_failed_checkin_is_trace_linked_to_the_error(sentry_init, capture_envelopes):
    sentry_init()
    envelopes = capture_envelopes()

    try:
        _break_world("Grace")
    except ZeroDivisionError as exc:
        sentry_sdk.capture_exception(exc)

    check_ins = []
    error_events = []
    for envelope in envelopes:
        payload = envelope.items[0].payload.json
        if payload.get("type") == "check_in":
            check_ins.append(payload)
        else:
            error_events.append(payload)

    assert [check_in["status"] for check_in in check_ins] == ["in_progress", "error"]
    assert len(error_events) == 1

    # The failed check-in and the exception that caused it share a trace, so
    # the reason the run was judged as failed can be looked up in Sentry.
    failed_check_in = check_ins[1]
    assert failed_check_in["contexts"]["trace"]["trace_id"]
    assert (
        failed_check_in["contexts"]["trace"]["trace_id"]
        == error_events[0]["contexts"]["trace"]["trace_id"]
    )
