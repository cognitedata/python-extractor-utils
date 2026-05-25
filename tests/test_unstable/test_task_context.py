from unittest.mock import MagicMock

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.core.tasks import StartupTask, TaskContext


def _make_context(parent_token: CancellationToken) -> tuple[TaskContext, CancellationToken]:
    task = StartupTask(name="test task", target=lambda ctx: None)
    extractor = MagicMock()
    extractor.EXTERNAL_ID = "test-extractor"
    child_token = parent_token.create_child_token()
    ctx = TaskContext(task=task, extractor=extractor, cancellation_token=child_token)
    return ctx, child_token


def test_cancellation_token_not_cancelled_initially() -> None:
    parent = CancellationToken()
    ctx, _ = _make_context(parent)
    assert not ctx.cancellation_token.is_cancelled


def test_cancelling_task_token_does_not_affect_extractor_token() -> None:
    parent = CancellationToken()
    ctx, child = _make_context(parent)

    child.cancel()

    assert ctx.cancellation_token.is_cancelled
    assert not parent.is_cancelled


def test_cancelling_extractor_token_cancels_task_token() -> None:
    parent = CancellationToken()
    ctx, _ = _make_context(parent)

    parent.cancel()

    assert ctx.cancellation_token.is_cancelled


def test_task_token_is_child_of_extractor_token() -> None:
    parent = CancellationToken()
    _, child = _make_context(parent)

    assert child._parent is parent
