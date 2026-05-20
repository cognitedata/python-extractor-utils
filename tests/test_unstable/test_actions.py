from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.unstable.core.actions import ActionContext, CustomAction
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel


@pytest.fixture
def mock_extractor() -> MagicMock:
    extractor = MagicMock()
    extractor.EXTERNAL_ID = "test-extractor"
    extractor._new_error.side_effect = lambda level, description, details=None, task_name=None: MagicMock(
        spec=Error,
        level=level,
        description=description,
        details=details,
        _task_name=task_name,
    )
    return extractor


@pytest.fixture
def simple_action() -> CustomAction:
    return CustomAction(name="my action", target=lambda ctx: None)


def test_custom_action_instantiation() -> None:
    def my_action(ctx: ActionContext) -> None:
        pass

    action = CustomAction(name="my action", target=my_action, description="Does something")

    assert action.name == "my action"
    assert action.target is my_action
    assert action.description == "Does something"


def test_custom_action_without_description() -> None:
    action = CustomAction(name="my action", target=lambda ctx: None)

    assert action.description is None


def test_action_context_attributes(mock_extractor: MagicMock, simple_action: CustomAction) -> None:
    ctx = ActionContext(
        action=simple_action,
        extractor=mock_extractor,
        external_id="triggered-action-ext-id",
        call_metadata={"key": "value"},
    )

    assert ctx.external_id == "triggered-action-ext-id"
    assert ctx.call_metadata == {"key": "value"}


def test_action_context_call_metadata_none(mock_extractor: MagicMock, simple_action: CustomAction) -> None:
    ctx = ActionContext(action=simple_action, extractor=mock_extractor, external_id="ext-id")

    assert ctx.call_metadata is None


def test_action_context_logger_name(mock_extractor: MagicMock, simple_action: CustomAction) -> None:
    ctx = ActionContext(action=simple_action, extractor=mock_extractor, external_id="ext-id")

    assert ctx._logger.name == "test-extractor.action.myaction"


def test_action_context_logger_name_strips_spaces(mock_extractor: MagicMock) -> None:
    action = CustomAction(name="process data", target=lambda ctx: None)
    ctx = ActionContext(action=action, extractor=mock_extractor, external_id="ext-id")

    assert ctx._logger.name == "test-extractor.action.processdata"


def test_action_context_error_delegates_to_extractor(mock_extractor: MagicMock, simple_action: CustomAction) -> None:
    ctx = ActionContext(action=simple_action, extractor=mock_extractor, external_id="ext-id")

    ctx._new_error(level=ErrorLevel.warning, description="Something went wrong")

    mock_extractor._new_error.assert_called_once_with(
        level=ErrorLevel.warning,
        description="Something went wrong",
        details=None,
        task_name="my action",
    )


def test_action_context_error_passes_explicit_task_name(mock_extractor: MagicMock, simple_action: CustomAction) -> None:
    ctx = ActionContext(action=simple_action, extractor=mock_extractor, external_id="ext-id")

    ctx._new_error(level=ErrorLevel.error, description="Fail", details="details", task_name="custom-task")

    call_kwargs = mock_extractor._new_error.call_args.kwargs
    assert call_kwargs["task_name"] == "custom-task"


def test_action_target_is_callable() -> None:
    called: list[ActionContext] = []

    def my_action(ctx: ActionContext) -> None:
        called.append(ctx)

    action = CustomAction(name="test", target=my_action)

    assert callable(action.target)
    assert action.target is my_action
