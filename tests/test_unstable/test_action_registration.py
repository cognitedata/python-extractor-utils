import time
from datetime import datetime, timezone
from threading import Event
from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.unstable.core._dto import ActionType, StartupRequest
from cognite.extractorutils.unstable.core.actions import CustomAction
from cognite.extractorutils.unstable.core.base import Extractor, FullConfig
from cognite.extractorutils.unstable.core.tasks import ContinuousTask, ScheduledTask, StartupTask, TaskContext

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


def _startup_request(extractor: Extractor) -> StartupRequest:
    extractor._start_time = datetime.now(tz=timezone.utc)
    return extractor._get_startup_request()


# -- available_actions population --


def test_no_scheduled_tasks_no_custom_actions_sends_available_actions_none() -> None:
    # TestExtractor has one StartupTask; StartupTasks do not produce available_actions entries.
    extractor = _make_extractor()
    assert _startup_request(extractor).available_actions is None


def test_two_scheduled_tasks_produce_four_available_actions() -> None:
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="alpha", target=lambda _: None))
    extractor.add_task(ScheduledTask.from_interval(interval="2h", name="beta", target=lambda _: None))
    req = _startup_request(extractor)
    assert req.available_actions is not None
    assert len(req.available_actions) == 4
    assert {a.name for a in req.available_actions} == {"Start alpha", "Stop alpha", "Start beta", "Stop beta"}


@pytest.mark.parametrize(
    "action_name,expected_type,expected_task",
    [
        ("Start alpha", ActionType.start_task, "alpha"),
        ("Stop alpha", ActionType.stop_task, "alpha"),
    ],
)
def test_scheduled_task_action_entry_has_correct_type_and_task_ref(
    action_name: str, expected_type: ActionType, expected_task: str
) -> None:
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="alpha", target=lambda _: None))
    by_name = {a.name: a for a in _startup_request(extractor).available_actions}
    assert by_name[action_name].type == expected_type
    assert by_name[action_name].task == expected_task


def test_continuous_and_startup_tasks_do_not_produce_available_actions() -> None:
    extractor = _make_extractor()
    extractor.add_task(ContinuousTask(name="cont", target=lambda _: None))
    extractor.add_task(StartupTask(name="init", target=lambda _: None))
    assert _startup_request(extractor).available_actions is None


def test_scheduled_and_custom_actions_combined_ordering() -> None:
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="sync", target=lambda _: None))
    extractor.add_action(CustomAction(name="flush", target=lambda _: None))
    req = _startup_request(extractor)
    assert req.available_actions is not None
    assert len(req.available_actions) == 3
    # Scheduled task entries appear before custom actions
    names = [a.name for a in req.available_actions]
    assert names == ["Start sync", "Stop sync", "flush"]


def test_custom_action_appears_with_correct_type_and_description() -> None:
    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="flush cache", target=lambda _: None, description="Clears state"))
    actions = _startup_request(extractor).available_actions
    assert actions is not None and len(actions) == 1
    assert actions[0].name == "flush cache"
    assert actions[0].type == ActionType.custom
    assert actions[0].description == "Clears state"


# -- __init_actions__ hook and add_action --


def test_init_actions_hook_called_after_init_tasks() -> None:
    call_order: list[str] = []

    class _Ext(TestExtractor):
        def __init_tasks__(self) -> None:
            call_order.append("tasks")

        def __init_actions__(self) -> None:
            call_order.append("actions")

    _make_extractor(_Ext)
    assert call_order == ["tasks", "actions"]


def test_add_action_from_init_actions_subclass_hook() -> None:
    class _Ext(TestExtractor):
        def __init_tasks__(self) -> None:
            pass

        def __init_actions__(self) -> None:
            self.add_action(CustomAction(name="ping", target=lambda _: None))

    extractor = _make_extractor(_Ext)
    assert len(extractor._custom_actions) == 1
    assert extractor._custom_actions[0].name == "ping"


def test_multiple_add_action_calls_accumulate_in_registration_order() -> None:
    extractor = _make_extractor()
    for name in ("a1", "a2", "a3"):
        extractor.add_action(CustomAction(name=name, target=lambda _: None))
    assert [a.name for a in extractor._custom_actions] == ["a1", "a2", "a3"]


def test_add_action_raises_on_duplicate_name() -> None:
    extractor = _make_extractor()
    extractor.add_action(CustomAction(name="ping", target=lambda _: None))
    with pytest.raises(ValueError, match="ping"):
        extractor.add_action(CustomAction(name="ping", target=lambda _: None))


@pytest.mark.parametrize("conflicting_name", ["Start sync", "Stop sync"])
def test_add_action_raises_on_conflict_with_scheduled_task_action_name(conflicting_name: str) -> None:
    extractor = _make_extractor()
    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="sync", target=lambda _: None))
    with pytest.raises(ValueError, match=conflicting_name):
        extractor.add_action(CustomAction(name=conflicting_name, target=lambda _: None))


@pytest.mark.parametrize("conflicting_name", ["Start sync", "Stop sync"])
def test_add_task_raises_on_conflict_with_existing_custom_action_name(conflicting_name: str) -> None:
    extractor = _make_extractor()
    extractor.add_action(CustomAction(name=conflicting_name, target=lambda _: None))
    with pytest.raises(ValueError, match="sync"):
        extractor.add_task(ScheduledTask.from_interval(interval="1h", name="sync", target=lambda _: None))


# -- _running_task_tokens lifecycle --


def test_token_present_in_running_task_tokens_during_execution() -> None:
    extractor = _make_extractor()
    token_present: list[bool] = []
    task_running = Event()
    appended = Event()
    allow_finish = Event()

    def target(_: TaskContext) -> None:
        task_running.set()
        token_present.append("the-task" in extractor._running_task_tokens)
        appended.set()
        allow_finish.wait(timeout=5)

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="the-task", target=target))
    extractor._scheduler.trigger("the-task")
    task_running.wait(timeout=5)
    allow_finish.set()
    appended.wait(timeout=5)

    assert token_present == [True]


@pytest.mark.parametrize("raises", [False, True])
def test_token_removed_from_running_task_tokens_after_task_finishes(raises: bool) -> None:
    extractor = _make_extractor()
    done = Event()

    def target(_: TaskContext) -> None:
        done.set()
        if raises:
            raise RuntimeError("intentional")

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="the-task", target=target))
    extractor._scheduler.trigger("the-task")
    done.wait(timeout=5)
    deadline = time.monotonic() + 5
    while "the-task" in extractor._running_task_tokens and time.monotonic() < deadline:
        time.sleep(0.005)
    assert "the-task" not in extractor._running_task_tokens


def test_token_cleanup_does_not_clobber_replacement_token() -> None:
    extractor = _make_extractor()
    done = Event()
    replacement = extractor.cancellation_token.create_child_token()

    def target(_: TaskContext) -> None:
        # Simulate a subsequent invocation overwriting the entry before this run finishes
        extractor._running_task_tokens["the-task"] = replacement
        done.set()

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="the-task", target=target))
    extractor._scheduler.trigger("the-task")
    done.wait(timeout=5)
    deadline = time.monotonic() + 5
    while any(j.name == "the-task" for j in extractor._scheduler._running) and time.monotonic() < deadline:
        time.sleep(0.005)

    # Identity check must preserve the replacement — the old blind pop would remove it
    assert extractor._running_task_tokens.get("the-task") is replacement


def test_scheduled_task_token_is_child_of_extractor_cancellation_token() -> None:
    extractor = _make_extractor()
    captured: list = []
    done = Event()

    def target(_: TaskContext) -> None:
        captured.append(extractor._running_task_tokens.get("the-task"))
        done.set()

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="the-task", target=target))
    extractor._scheduler.trigger("the-task")
    done.wait(timeout=5)

    assert len(captured) == 1 and captured[0] is not None
    assert captured[0]._parent is extractor.cancellation_token
