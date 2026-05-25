from threading import Event
from unittest.mock import MagicMock

from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.tasks import ScheduledTask, TaskContext
from test_unstable.conftest import TestConfig, TestExtractor


def _make_extractor() -> TestExtractor:
    """Create a TestExtractor with a mocked connection — no real API calls."""
    full_config = FullConfig(
        connection_config=MagicMock(),
        application_config=TestConfig(parameter_one=1, parameter_two="a"),
        current_config_revision=1,
    )
    return TestExtractor(full_config, MagicMock())


def _add_and_trigger(extractor: TestExtractor) -> TaskContext:
    """
    Add a ScheduledTask, trigger it via the scheduler, and return the TaskContext
    the framework created. This exercises the actual add_task code path in base.py.
    """
    captured: list[TaskContext] = []
    done = Event()

    def capture(ctx: TaskContext) -> None:
        captured.append(ctx)
        done.set()

    extractor.add_task(ScheduledTask.from_interval(interval="1h", name="test-task", target=capture))
    extractor._scheduler.trigger("test-task")
    done.wait(timeout=5)
    return captured[0]


def test_scheduled_task_gets_child_cancellation_token() -> None:
    extractor = _make_extractor()
    ctx = _add_and_trigger(extractor)
    assert ctx.cancellation_token._parent is extractor.cancellation_token


def test_task_token_cancel_does_not_affect_extractor() -> None:
    extractor = _make_extractor()
    ctx = _add_and_trigger(extractor)

    ctx.cancellation_token.cancel()

    assert not extractor.cancellation_token.is_cancelled


def test_extractor_token_cancel_propagates_to_task_token() -> None:
    extractor = _make_extractor()
    ctx = _add_and_trigger(extractor)

    extractor.cancellation_token.cancel()

    assert ctx.cancellation_token.is_cancelled
