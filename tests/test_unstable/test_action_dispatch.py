"""Tests for Extractor action dispatch wiring (Task 6).

Covers _handle_actions, _dispatch_single_action, and the three handler methods,
plus the start() wiring that registers the dispatcher with CheckinWorker.

All tests use a MagicMock checkin worker to avoid real API calls.
"""

import threading
from threading import Event
from unittest.mock import MagicMock

from cognite.extractorutils.unstable.core._dto import Action, ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core.actions import ActionContext, CustomAction
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.tasks import ScheduledTask, TaskContext

from .conftest import TestConfig, TestExtractor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_extractor(extractor_cls: type[TestExtractor] = TestExtractor) -> TestExtractor:
    conn = MagicMock()
    conn.integration.external_id = "test-integration"
    full_config = FullConfig(
        connection_config=conn,
        application_config=TestConfig(parameter_one=1, parameter_two="a"),
        current_config_revision=1,
    )
    return extractor_cls(full_config, MagicMock())


def _queued_updates(extractor: TestExtractor) -> list[ActionUpdate]:
    """Return all ActionUpdate objects passed to queue_action_update, in call order."""
    return [c[0][0] for c in extractor._checkin_worker.queue_action_update.call_args_list]


def _make_action(external_id: str, action_name: str) -> Action:
    return Action(external_id=external_id, action_name=action_name, status=ActionStatus.pending)


# ---------------------------------------------------------------------------
# _dispatch_single_action - routing & unknown-action handling
# ---------------------------------------------------------------------------


def test_dispatch_unrecognised_action_name_reports_failed() -> None:
    """An action name that matches nothing registered is reported as failed with a descriptive message."""
    extractor = _make_extractor()
    extractor._dispatch_single_action(_make_action("act-1", "DoesNotExist"))

    updates = _queued_updates(extractor)
    assert len(updates) == 1
    assert updates[0].status == ActionStatus.failed
    assert "DoesNotExist" in (updates[0].result_message or "")


def test_dispatch_start_prefix_for_unregistered_task_reports_failed() -> None:
    """'Start foo' where 'foo' is not a ScheduledTask is treated as an unrecognised action."""
    extractor = _make_extractor()
    extractor._dispatch_single_action(_make_action("act-1", "Start unregistered-task"))

    updates = _queued_updates(extractor)
    assert len(updates) == 1
    assert updates[0].status == ActionStatus.failed


def test_dispatch_routes_to_start_handler_for_registered_scheduled_task() -> None:
    """'Start <task>' where <task> is a ScheduledTask routes to the start handler."""
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="sync", target=lambda _: None))

    extractor._dispatch_single_action(_make_action("act-1", "Start sync"))

    updates = _queued_updates(extractor)
    # running status means the start handler was reached (not the "unrecognised" path)
    assert any(u.status == ActionStatus.running for u in updates)


def test_dispatch_routes_to_stop_handler_for_registered_scheduled_task() -> None:
    """'Stop <task>' routes to the stop handler; task not currently running → failed."""
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="sync", target=lambda _: None))

    extractor._dispatch_single_action(_make_action("act-1", "Stop sync"))

    updates = _queued_updates(extractor)
    assert len(updates) == 1
    assert updates[0].status == ActionStatus.failed
    assert "not currently running" in (updates[0].result_message or "")


def test_dispatch_routes_to_custom_handler_for_registered_custom_action() -> None:
    """A registered custom action name routes to the custom handler."""
    extractor = _make_extractor()
    invoked = []
    extractor.add_action(CustomAction(name="flush", target=lambda ctx: invoked.append(True)))

    extractor._dispatch_single_action(_make_action("act-1", "flush"))

    assert invoked == [True]
    updates = _queued_updates(extractor)
    assert any(u.status == ActionStatus.succeeded for u in updates)


# ---------------------------------------------------------------------------
# _handle_start_task_action
# ---------------------------------------------------------------------------


def test_start_task_action_already_running_reports_failed() -> None:
    """If the task is already in _running_task_tokens, the action fails without starting a second instance."""
    extractor = _make_extractor()
    task_started = Event()
    allow_finish = Event()

    def blocking(ctx: TaskContext) -> None:
        task_started.set()
        allow_finish.wait(timeout=5)

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="worker", target=blocking))
    extractor._scheduler.trigger("worker")
    task_started.wait(timeout=5)

    # Task is now running; attempt a second start via action
    extractor._dispatch_single_action(_make_action("act-dup", "Start worker"))

    allow_finish.set()

    updates = _queued_updates(extractor)
    assert any(u.status == ActionStatus.failed and "already running" in (u.result_message or "") for u in updates)


def test_start_task_action_valid_idle_task_reports_running_then_succeeded() -> None:
    """A valid, idle ScheduledTask reports running before execution and succeeded after."""
    extractor = _make_extractor()
    ran = []

    def quick(ctx: TaskContext) -> None:
        ran.append(True)

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="quick", target=quick))
    # _dispatch_single_action is synchronous for start_task (blocks until task completes)
    extractor._dispatch_single_action(_make_action("act-1", "Start quick"))

    assert ran == [True]
    updates = _queued_updates(extractor)
    statuses = [u.status for u in updates if u.external_id == "act-1"]
    assert ActionStatus.running in statuses
    assert ActionStatus.succeeded in statuses
    assert statuses.index(ActionStatus.running) < statuses.index(ActionStatus.succeeded)


def test_start_task_action_raises_reports_failed() -> None:
    """If _run_task_with_token itself raises (e.g. child token creation fails), failed is reported."""
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="boom", target=lambda _: None))

    original = extractor._run_task_with_token

    def raise_on_call(task: ScheduledTask) -> None:
        raise RuntimeError("token failure")

    extractor._run_task_with_token = raise_on_call

    extractor._dispatch_single_action(_make_action("act-err", "Start boom"))

    updates = _queued_updates(extractor)
    statuses = [u.status for u in updates if u.external_id == "act-err"]
    assert ActionStatus.running in statuses
    assert ActionStatus.failed in statuses
    failed = next(u for u in updates if u.status == ActionStatus.failed and u.external_id == "act-err")
    assert "token failure" in (failed.result_message or "")

    extractor._run_task_with_token = original


# ---------------------------------------------------------------------------
# _handle_stop_task_action
# ---------------------------------------------------------------------------


def test_stop_task_action_not_running_reports_failed() -> None:
    """Stopping a task that is not in _running_task_tokens reports failed."""
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="worker", target=lambda _: None))

    extractor._dispatch_single_action(_make_action("act-stop", "Stop worker"))

    updates = _queued_updates(extractor)
    assert len(updates) == 1
    assert updates[0].status == ActionStatus.failed
    assert "not currently running" in (updates[0].result_message or "")


def test_stop_task_action_cancels_child_token_and_reports_canceled() -> None:
    """Stopping a running task cancels its child token and reports ActionStatus.canceled."""
    extractor = _make_extractor()
    task_started = Event()
    allow_exit = Event()

    def cancellable(ctx: TaskContext) -> None:
        task_started.set()
        allow_exit.wait(timeout=5)

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="worker", target=cancellable))
    extractor._scheduler.trigger("worker")
    task_started.wait(timeout=5)

    # Snapshot the token before issuing stop
    with extractor._running_task_tokens_lock:
        token = extractor._running_task_tokens.get("worker")

    extractor._dispatch_single_action(_make_action("act-stop", "Stop worker"))

    updates = _queued_updates(extractor)
    assert any(u.status == ActionStatus.canceled and u.external_id == "act-stop" for u in updates)
    assert token is not None and token.is_cancelled

    allow_exit.set()


# ---------------------------------------------------------------------------
# _handle_custom_action
# ---------------------------------------------------------------------------


def test_custom_action_succeeds_reports_running_then_succeeded() -> None:
    """A custom action that completes without exception is reported as running → succeeded."""
    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="ping", target=lambda ctx: None))

    extractor._dispatch_single_action(_make_action("act-ping", "ping"))

    updates = _queued_updates(extractor)
    statuses = [u.status for u in updates if u.external_id == "act-ping"]
    assert statuses == [ActionStatus.running, ActionStatus.succeeded]


def test_custom_action_raises_reports_failed_with_message() -> None:
    """A custom action that raises is reported as failed with the exception message."""

    def bad_action(ctx: ActionContext) -> None:
        raise ValueError("something went wrong")

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="bad", target=bad_action))

    extractor._dispatch_single_action(_make_action("act-bad", "bad"))

    updates = _queued_updates(extractor)
    statuses = [u.status for u in updates if u.external_id == "act-bad"]
    assert ActionStatus.running in statuses
    assert ActionStatus.failed in statuses
    failed = next(u for u in updates if u.status == ActionStatus.failed)
    assert "something went wrong" in (failed.result_message or "")


def test_custom_action_receives_call_metadata_in_context() -> None:
    """call_metadata from the server-side Action is forwarded to ActionContext."""
    received_metadata: list[dict | None] = []

    def capture(ctx: ActionContext) -> None:
        received_metadata.append(ctx.call_metadata)

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="greet", target=capture))

    action = Action(
        external_id="act-meta",
        action_name="greet",
        status=ActionStatus.pending,
        call_metadata={"key": "value"},
    )
    extractor._dispatch_single_action(action)

    assert received_metadata == [{"key": "value"}]


# ---------------------------------------------------------------------------
# _handle_actions - threading
# ---------------------------------------------------------------------------


def test_handle_actions_spawns_daemon_thread_named_after_external_id() -> None:
    """Each action in _handle_actions runs on a daemon thread named 'Action-<external_id>'."""
    extractor = _make_extractor()
    captured: dict = {}
    done = Event()

    def target(ctx: ActionContext) -> None:
        t = threading.current_thread()
        captured["name"] = t.name
        captured["daemon"] = t.daemon
        done.set()

    extractor.add_action(CustomAction(name="work", target=target))
    extractor._handle_actions([_make_action("xid-42", "work")])

    done.wait(timeout=5)
    assert captured["name"] == "Action-xid-42"
    assert captured["daemon"] is True


def test_handle_actions_multiple_actions_run_concurrently() -> None:
    """Two actions are dispatched to separate threads; they overlap in time."""
    extractor = _make_extractor()
    a1_started = Event()
    a2_started = Event()
    release = Event()

    def slow(ctx: ActionContext) -> None:
        if ctx.external_id == "act-1":
            a1_started.set()
        else:
            a2_started.set()
        release.wait(timeout=5)

    extractor.add_action(CustomAction(name="op-1", target=slow))
    extractor.add_action(CustomAction(name="op-2", target=slow))

    extractor._handle_actions([
        _make_action("act-1", "op-1"),
        _make_action("act-2", "op-2"),
    ])

    # Both must start before either finishes — proves concurrent execution
    assert a1_started.wait(timeout=5), "op-1 never started"
    assert a2_started.wait(timeout=5), "op-2 never started"

    release.set()


# ---------------------------------------------------------------------------
# start() wiring
# ---------------------------------------------------------------------------


def test_start_registers_handle_actions_as_dispatcher() -> None:
    """start() calls set_action_dispatcher exactly once with _handle_actions."""
    extractor = _make_extractor()
    with extractor:
        pass

    extractor._checkin_worker.set_action_dispatcher.assert_called_once()
    registered = extractor._checkin_worker.set_action_dispatcher.call_args[0][0]
    # Verify the registered callable IS the bound _handle_actions method
    assert registered.__func__.__name__ == "_handle_actions"
    assert registered.__self__ is extractor


def test_dispatcher_registered_before_checkin_thread_starts() -> None:
    """set_action_dispatcher is called before the checkin thread is spawned in start()."""
    call_order: list[str] = []

    class _TrackingWorker:
        def set_action_dispatcher(self, fn: object) -> None:
            call_order.append("set_dispatcher")

        def run_periodic_checkin(self, *args: object, **kwargs: object) -> None:
            pass

    extractor = _make_extractor()
    extractor._checkin_worker = _TrackingWorker()  # type: ignore[assignment]

    original_thread_start = threading.Thread.start

    def tracking_start(self: threading.Thread) -> None:
        if self.name == "ExtractorCheckin":
            call_order.append("checkin_thread")
        original_thread_start(self)

    threading.Thread.start = tracking_start  # type: ignore[method-assign]
    try:
        extractor.start()
    finally:
        threading.Thread.start = original_thread_start  # type: ignore[method-assign]
        extractor.stop()

    assert call_order.index("set_dispatcher") < call_order.index("checkin_thread")
