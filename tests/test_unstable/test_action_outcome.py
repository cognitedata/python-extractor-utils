"""Tests for the final status of ``Start <task>`` / ``Stop <task>`` actions (EDG-1035)."""

from collections.abc import Callable
from threading import Event, Thread
from unittest.mock import patch

from cognite.extractorutils.unstable.core import base
from cognite.extractorutils.unstable.core._dto import MAX_MESSAGE_LENGTH, ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core.tasks import ScheduledTask, TaskContext

from .test_action_dispatch import _make_action, _make_extractor, _queued_updates


def _final(extractor: base.Extractor, external_id: str) -> ActionUpdate:  # type: ignore[type-arg]
    updates = [u for u in _queued_updates(extractor) if u.external_id == external_id]  # type: ignore[arg-type]
    assert updates[0].status == ActionStatus.running
    return updates[-1]


def _run_start(target: Callable[[TaskContext], None]) -> ActionUpdate:
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="job", target=target))
    extractor._dispatch_single_action(_make_action("act-1", "Start job"))
    return _final(extractor, "act-1")


def test_start_fatal_error_reports_failed_with_message() -> None:
    def target(ctx: TaskContext) -> None:
        ctx.error("db down", details="connection refused")
        ctx.fatal("cannot continue", details="bad credentials")

    update = _run_start(target)
    assert update.status == ActionStatus.failed
    assert update.result_message == "cannot continue: bad credentials"


def test_start_fatal_message_is_truncated_to_odin_limit() -> None:
    update = _run_start(lambda ctx: ctx.fatal("boom", details="x" * 5000))
    assert update.status == ActionStatus.failed
    assert update.result_message is not None
    assert len(update.result_message) == MAX_MESSAGE_LENGTH


def test_start_error_level_only_reports_completed_with_errors() -> None:
    def target(ctx: TaskContext) -> None:
        ctx.error("first problem")
        ctx.error("second problem")
        ctx.warning("just a warning")

    update = _run_start(target)
    assert update.status == ActionStatus.completed_with_errors
    assert update.result_message == "Completed with 2 error(s): first problem"


def test_start_exception_logged_at_error_level_reports_completed_with_errors() -> None:
    def target(ctx: TaskContext) -> None:
        ctx.exception("query failed", ValueError("bad sql"))

    update = _run_start(target)
    assert update.status == ActionStatus.completed_with_errors
    assert "query failed" in (update.result_message or "")


def test_start_warning_only_reports_succeeded() -> None:
    update = _run_start(lambda ctx: ctx.warning("meh"))
    assert update.status == ActionStatus.succeeded
    assert update.result_message is None


def test_start_without_errors_reports_succeeded() -> None:
    assert _run_start(lambda ctx: None).status == ActionStatus.succeeded


def test_start_raising_task_reports_failed() -> None:
    def target(ctx: TaskContext) -> None:
        raise RuntimeError("kaboom")

    update = _run_start(target)
    assert update.status == ActionStatus.failed
    assert "kaboom" in (update.result_message or "")


def test_start_cancelled_reports_canceled_even_with_errors() -> None:
    def target(ctx: TaskContext) -> None:
        ctx.error("partial failure")
        ctx.cancellation_token.cancel()

    assert _run_start(target).status == ActionStatus.canceled


def test_scheduled_run_does_not_queue_action_updates_and_still_runs() -> None:
    extractor = _make_extractor()
    ran = Event()

    def target(ctx: TaskContext) -> None:
        ctx.fatal("scheduled failure")
        ran.set()

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="job", target=target))
    extractor._scheduler.trigger("job")
    assert ran.wait(5)
    assert _queued_updates(extractor) == []


def _start_blocking_task(extractor: base.Extractor, release: Event) -> None:  # type: ignore[type-arg]
    started = Event()

    def target(ctx: TaskContext) -> None:
        started.set()
        release.wait(10)

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="worker", target=target))
    extractor._scheduler.trigger("worker")
    assert started.wait(5)


def test_stop_stays_running_until_task_ends_then_succeeds() -> None:
    extractor = _make_extractor()
    release = Event()
    _start_blocking_task(extractor, release)

    thread = Thread(target=extractor._dispatch_single_action, args=(_make_action("act-stop", "Stop worker"),))
    thread.start()
    thread.join(0.5)
    assert thread.is_alive(), "Stop must keep waiting while the task is still running"
    statuses = [u.status for u in _queued_updates(extractor)]
    assert statuses == [ActionStatus.running]

    release.set()
    thread.join(5)
    assert not thread.is_alive()
    assert [u.status for u in _queued_updates(extractor)] == [ActionStatus.running, ActionStatus.succeeded]


def test_stop_times_out_with_failed_while_task_keeps_running() -> None:
    extractor = _make_extractor()
    release = Event()
    _start_blocking_task(extractor, release)

    with patch.object(base, "STOP_TASK_ACTION_TIMEOUT_SECONDS", 0.3):
        extractor._dispatch_single_action(_make_action("act-stop", "Stop worker"))

    update = _queued_updates(extractor)[-1]
    assert update.status == ActionStatus.failed
    assert update.result_message == "Task 'worker' did not stop within 0.3 seconds"
    release.set()


def test_stop_on_idle_task_reports_failed() -> None:
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="worker", target=lambda _: None))
    extractor._dispatch_single_action(_make_action("act-stop", "Stop worker"))
    updates = _queued_updates(extractor)
    assert [u.status for u in updates] == [ActionStatus.failed]
    assert updates[0].result_message == "Task 'worker' is not currently running"


def test_waiting_stop_is_reported_canceled_on_shutdown() -> None:
    extractor = _make_extractor()
    release = Event()
    _start_blocking_task(extractor, release)

    thread = Thread(target=extractor._dispatch_single_action, args=(_make_action("act-stop", "Stop worker"),))
    thread.start()
    thread.join(0.3)
    assert thread.is_alive()

    extractor._finish_running_actions()
    thread.join(5)
    assert not thread.is_alive()
    assert _queued_updates(extractor)[-1].status == ActionStatus.canceled
    release.set()
