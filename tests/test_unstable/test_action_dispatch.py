import threading
import time
from collections.abc import Callable
from threading import Event
from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.unstable.core._dto import MAX_MESSAGE_LENGTH, Action, ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core.actions import ActionContext, ActionError, CustomAction
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.tasks import ContinuousTask, ScheduledTask, TaskContext

from .conftest import TestConfig, TestExtractor


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
    return [c[0][0] for c in extractor._checkin_worker.queue_action_update.call_args_list]


def _make_action(external_id: str, action_name: str) -> Action:
    return Action(external_id=external_id, action_name=action_name, status=ActionStatus.pending)


def test_dispatch_unrecognised_action_name_reports_failed() -> None:
    extractor = _make_extractor()
    extractor._dispatch_single_action(_make_action("act-1", "DoesNotExist"))

    updates = _queued_updates(extractor)
    assert len(updates) == 1
    assert updates[0].status == ActionStatus.failed
    assert "DoesNotExist" in (updates[0].result_message or "")


@pytest.mark.parametrize(
    "register,action_name,expected_status",
    [
        pytest.param(
            lambda ext: ext.add_task(ScheduledTask.from_interval(interval="1h", name="sync", target=lambda _: None)),
            "Start sync",
            ActionStatus.running,
            id="start_task",
        ),
        pytest.param(
            lambda ext: ext.add_action(CustomAction(name="flush", target=lambda ctx: None)),
            "flush",
            ActionStatus.succeeded,
            id="custom_action",
        ),
    ],
)
def test_dispatch_routes_to_correct_handler(
    register: Callable, action_name: str, expected_status: ActionStatus
) -> None:
    extractor = _make_extractor()
    register(extractor)
    extractor._dispatch_single_action(_make_action("act-1", action_name))
    assert any(u.status == expected_status for u in _queued_updates(extractor))


def test_start_task_action_already_running_reports_failed() -> None:
    extractor = _make_extractor()
    task_started = Event()
    allow_finish = Event()

    def blocking(ctx: TaskContext) -> None:
        task_started.set()
        allow_finish.wait(timeout=5)

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="worker", target=blocking))
    extractor._scheduler.trigger("worker")
    task_started.wait(timeout=5)

    extractor._dispatch_single_action(_make_action("act-dup", "Start worker"))

    allow_finish.set()

    updates = _queued_updates(extractor)
    assert any(u.status == ActionStatus.failed and "already running" in (u.result_message or "") for u in updates)


def test_start_task_action_valid_idle_task_reports_running_then_succeeded() -> None:
    extractor = _make_extractor()
    ran = []

    def quick(ctx: TaskContext) -> None:
        ran.append(True)

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="quick", target=quick))
    extractor._dispatch_single_action(_make_action("act-1", "Start quick"))

    assert ran == [True]
    updates = _queued_updates(extractor)
    statuses = [u.status for u in updates if u.external_id == "act-1"]
    assert ActionStatus.running in statuses
    assert ActionStatus.succeeded in statuses
    assert statuses.index(ActionStatus.running) < statuses.index(ActionStatus.succeeded)


def test_stop_task_action_not_running_reports_failed() -> None:
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="worker", target=lambda _: None))

    extractor._dispatch_single_action(_make_action("act-stop", "Stop worker"))

    updates = _queued_updates(extractor)
    assert len(updates) == 1
    assert updates[0].status == ActionStatus.failed
    assert "not currently running" in (updates[0].result_message or "")


def test_stop_task_action_cancels_child_token_and_reports_canceled() -> None:
    extractor = _make_extractor()
    task_started = Event()
    allow_exit = Event()

    def cancellable(ctx: TaskContext) -> None:
        task_started.set()
        allow_exit.wait(timeout=5)

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="worker", target=cancellable))
    extractor._scheduler.trigger("worker")
    task_started.wait(timeout=5)

    with extractor._running_task_tokens_lock:
        token = extractor._running_task_tokens.get("worker")

    extractor._dispatch_single_action(_make_action("act-stop", "Stop worker"))

    updates = _queued_updates(extractor)
    assert any(u.status == ActionStatus.canceled and u.external_id == "act-stop" for u in updates)
    assert token is not None and token.is_cancelled

    allow_exit.set()


def test_launch_continuous_task_is_tracked_before_thread_starts() -> None:
    extractor = _make_extractor()
    task_started = Event()
    allow_exit = Event()

    def cancellable(ctx: TaskContext) -> None:
        task_started.set()
        allow_exit.wait(timeout=5)

    task = ContinuousTask(name="listener", target=cancellable)
    extractor.add_task(task)

    extractor._launch_continuous_task(task)
    # No race: the token is registered synchronously by _launch_continuous_task, before the
    # spawned thread even starts, so it must already be present here.
    with extractor._running_task_tokens_lock:
        token = extractor._running_task_tokens.get("listener")
    assert token is not None

    task_started.wait(timeout=5)
    allow_exit.set()


def test_stop_action_cancels_boot_launched_continuous_task() -> None:
    extractor = _make_extractor()
    task_started = Event()
    task_exited = Event()

    def cancellable(ctx: TaskContext) -> None:
        task_started.set()
        ctx.cancellation_token.wait()
        task_exited.set()

    task = ContinuousTask(name="listener", target=cancellable)
    extractor.add_task(task)
    extractor._launch_continuous_task(task)
    task_started.wait(timeout=5)

    extractor._dispatch_single_action(_make_action("act-stop", "Stop listener"))

    updates = _queued_updates(extractor)
    assert any(u.status == ActionStatus.canceled and u.external_id == "act-stop" for u in updates)
    assert task_exited.wait(timeout=5)
    deadline = time.monotonic() + 5
    while "listener" in extractor._running_task_tokens and time.monotonic() < deadline:
        time.sleep(0.005)
    assert "listener" not in extractor._running_task_tokens


def test_start_action_on_running_continuous_task_reports_failed() -> None:
    extractor = _make_extractor()
    task_started = Event()
    allow_exit = Event()

    def cancellable(ctx: TaskContext) -> None:
        task_started.set()
        allow_exit.wait(timeout=5)

    task = ContinuousTask(name="listener", target=cancellable)
    extractor.add_task(task)
    extractor._launch_continuous_task(task)
    task_started.wait(timeout=5)

    extractor._dispatch_single_action(_make_action("act-start", "Start listener"))

    updates = _queued_updates(extractor)
    assert any(
        u.status == ActionStatus.failed and "already running" in (u.result_message or "") for u in updates
    )
    allow_exit.set()


def test_start_action_relaunches_continuous_task_after_stop() -> None:
    extractor = _make_extractor()
    run_count = {"n": 0}
    started = [Event(), Event()]

    def cancellable(ctx: TaskContext) -> None:
        i = run_count["n"]
        run_count["n"] += 1
        started[i].set()
        ctx.cancellation_token.wait()

    task = ContinuousTask(name="listener", target=cancellable)
    extractor.add_task(task)
    extractor._launch_continuous_task(task)
    assert started[0].wait(timeout=5)

    extractor._dispatch_single_action(_make_action("act-stop", "Stop listener"))
    deadline = time.monotonic() + 5
    while "listener" in extractor._running_task_tokens and time.monotonic() < deadline:
        time.sleep(0.005)
    assert "listener" not in extractor._running_task_tokens

    # The real dispatch path runs this on its own dedicated thread (base.py's _handle_actions);
    # since a ContinuousTask's target runs indefinitely, _handle_start_task_action blocks for as
    # long as the task runs, so mirror that here rather than calling it on the test's own thread.
    start_thread = threading.Thread(
        target=extractor._dispatch_single_action,
        args=(_make_action("act-start", "Start listener"),),
        daemon=True,
    )
    start_thread.start()
    assert started[1].wait(timeout=5)
    assert run_count["n"] == 2

    with extractor._running_task_tokens_lock:
        token = extractor._running_task_tokens.get("listener")
    assert token is not None
    token.cancel()
    start_thread.join(timeout=5)
    assert not start_thread.is_alive()


@pytest.mark.parametrize(
    "raises,expected_final,expected_message",
    [
        (False, ActionStatus.succeeded, None),
        (True, ActionStatus.failed, "something went wrong"),
    ],
)
def test_custom_action_status_lifecycle(
    raises: bool, expected_final: ActionStatus, expected_message: str | None
) -> None:
    def target(ctx: ActionContext) -> None:
        if raises:
            raise ValueError("something went wrong")

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="act", target=target))
    extractor._dispatch_single_action(_make_action("act-1", "act"))

    updates = _queued_updates(extractor)
    statuses = [u.status for u in updates if u.external_id == "act-1"]
    assert statuses[0] == ActionStatus.running
    assert statuses[-1] == expected_final
    if expected_message:
        assert expected_message in (updates[-1].result_message or "")


def test_action_error_sets_result_metadata_and_keeps_failed_status() -> None:
    def target(ctx: ActionContext) -> None:
        raise ActionError("bad input", error_type="invalid_parameter")

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="strict", target=target))
    extractor._dispatch_single_action(_make_action("act-err", "strict"))

    updates = _queued_updates(extractor)
    failed = next(u for u in updates if u.status == ActionStatus.failed)
    assert failed.result_metadata == {"error_type": "invalid_parameter"}
    assert failed.result_message == "bad input"


def test_oversized_result_metadata_fails_action_but_keeps_valid_fields() -> None:
    def target(ctx: ActionContext) -> None:
        ctx.set_result("done", metadata={"summary": "ok", "blob": "x" * 600})

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="big-result", target=target))
    extractor._dispatch_single_action(_make_action("act-big", "big-result"))

    updates = _queued_updates(extractor)
    final = updates[-1]
    assert final.status == ActionStatus.failed
    assert final.result_metadata == {"summary": "ok"}
    assert "big-result" in (final.result_message or "")
    assert "blob" in (final.result_message or "")
    assert "512" in (final.result_message or "")


def test_oversized_action_error_metadata_drops_only_oversized_field() -> None:
    def target(ctx: ActionContext) -> None:
        raise ActionError("bad input", error_type="invalid_parameter", details="x" * 600)

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="strict-big", target=target))
    extractor._dispatch_single_action(_make_action("act-strict-big", "strict-big"))

    updates = _queued_updates(extractor)
    failed = next(u for u in updates if u.status == ActionStatus.failed)
    assert failed.result_metadata == {"error_type": "invalid_parameter"}
    assert "bad input" in (failed.result_message or "")
    assert "error_detail" in (failed.result_message or "")
    assert "512" in (failed.result_message or "")


def test_oversized_action_error_metadata_all_fields_oversized_normalizes_to_none() -> None:
    # Regression test: error_type itself is caller-supplied and unbounded (ActionError places no
    # length limit on it), so it — not just the optional error_detail — can end up oversized. When
    # every field is dropped, result_metadata must be None (field omitted from the checkin payload),
    # not {} (field sent as an empty object, an untested Integrations API edge case).
    def target(ctx: ActionContext) -> None:
        raise ActionError("bad input", error_type="x" * 600)

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="strict-huge", target=target))
    extractor._dispatch_single_action(_make_action("act-strict-huge", "strict-huge"))

    updates = _queued_updates(extractor)
    failed = next(u for u in updates if u.status == ActionStatus.failed)
    assert failed.result_metadata is None


def test_oversized_result_metadata_with_many_fields_truncates_message_instead_of_crashing() -> None:
    # Regression test: ctx.set_result() places no cap on the number or length of metadata keys, so
    # joining many oversized field names into the failure message could exceed MessageType's
    # 1000-char limit and raise a pydantic.ValidationError while constructing the ActionUpdate —
    # which, for this branch, would be swallowed by the generic `except Exception` fallback and
    # silently discard the valid metadata this whole mechanism exists to preserve.
    def target(ctx: ActionContext) -> None:
        oversized = {f"oversized_field_number_{i:03d}": "x" * 600 for i in range(45)}
        ctx.set_result("done", metadata={"summary": "ok", **oversized})

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="huge-result", target=target))
    extractor._dispatch_single_action(_make_action("act-huge", "huge-result"))

    updates = _queued_updates(extractor)
    final = updates[-1]
    assert final.status == ActionStatus.failed
    assert final.result_metadata == {"summary": "ok"}
    assert final.result_message is not None
    assert len(final.result_message) <= MAX_MESSAGE_LENGTH
    assert final.result_message.endswith("...")


def test_oversized_action_error_with_long_message_truncates_instead_of_crashing() -> None:
    # Regression test: ActionError places no length limit on its own message either, so appending
    # the "metadata field(s) dropped" note could push the combined result_message past the 1000-char
    # limit. Unlike the success-path branch, this ActionUpdate(...) call isn't nested in any further
    # try/except, so a ValidationError here would escape _handle_custom_action entirely and kill the
    # dispatch thread, leaving the action stuck at "running" forever.
    def target(ctx: ActionContext) -> None:
        raise ActionError("x" * 1200, error_type="invalid_parameter", details="y" * 600)

    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="strict-huge-msg", target=target))
    extractor._dispatch_single_action(_make_action("act-strict-huge-msg", "strict-huge-msg"))

    updates = _queued_updates(extractor)
    failed = next(u for u in updates if u.status == ActionStatus.failed)
    assert failed.result_metadata == {"error_type": "invalid_parameter"}
    assert failed.result_message is not None
    assert len(failed.result_message) <= MAX_MESSAGE_LENGTH
    assert failed.result_message.endswith("...")


def test_custom_action_receives_call_metadata_in_context() -> None:
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


def test_handle_actions_spawns_daemon_thread_named_after_external_id() -> None:
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

    extractor._handle_actions(
        [
            _make_action("act-1", "op-1"),
            _make_action("act-2", "op-2"),
        ]
    )

    assert a1_started.wait(timeout=5), "op-1 never started"
    assert a2_started.wait(timeout=5), "op-2 never started"

    release.set()


def test_start_registers_handle_actions_as_dispatcher() -> None:
    extractor = _make_extractor()
    with extractor:
        pass

    extractor._checkin_worker.set_action_dispatcher.assert_called_once()
    registered = extractor._checkin_worker.set_action_dispatcher.call_args[0][0]
    assert registered.__func__.__name__ == "_handle_actions"
    assert registered.__self__ is extractor


def test_set_result_propagates_to_succeeded_action_update() -> None:
    extractor = _make_extractor()

    def action_with_result(ctx: ActionContext) -> None:
        ctx.set_result("3 files uploaded", metadata={"total_files": "3", "uploaded_files": "3"})

    extractor.add_action(CustomAction(name="upload", target=action_with_result))
    action = Action(external_id="act-r", action_name="upload", status=ActionStatus.pending)
    extractor._dispatch_single_action(action)
    updates = [c[0][0] for c in extractor._checkin_worker.queue_action_update.call_args_list]
    succeeded = next(u for u in updates if u.status == ActionStatus.succeeded)
    assert succeeded.result_message == "3 files uploaded"
    assert succeeded.result_metadata == {"total_files": "3", "uploaded_files": "3"}


def test_set_result_without_metadata_leaves_result_metadata_none() -> None:
    extractor = _make_extractor()

    def action_message_only(ctx: ActionContext) -> None:
        ctx.set_result("done")

    extractor.add_action(CustomAction(name="simple", target=action_message_only))
    extractor._dispatch_single_action(_make_action("act-s", "simple"))
    updates = [c[0][0] for c in extractor._checkin_worker.queue_action_update.call_args_list]
    succeeded = next(u for u in updates if u.status == ActionStatus.succeeded)
    assert succeeded.result_message == "done"
    assert succeeded.result_metadata is None


def test_action_context_exposes_cdf_client_and_integration_external_id() -> None:
    extractor = _make_extractor()
    captured: dict = {}

    def capture(ctx: ActionContext) -> None:
        captured["cdf_client"] = ctx.cdf_client
        captured["integration_external_id"] = ctx.integration_external_id

    extractor.add_action(CustomAction(name="probe", target=capture))
    extractor._dispatch_single_action(_make_action("act-p", "probe"))
    assert captured["cdf_client"] is extractor.cognite_client
    assert captured["integration_external_id"] == "test-integration"
