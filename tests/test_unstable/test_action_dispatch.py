import threading
from collections.abc import Callable
from threading import Event
from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.unstable.core._dto import Action, ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core.actions import ActionContext, CustomAction
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.tasks import ScheduledTask, TaskContext

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
