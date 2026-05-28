import pytest
from pydantic import ValidationError

from cognite.extractorutils.unstable.core._dto import (
    Action,
    ActionStatus,
    ActionType,
    ActionUpdate,
    AvailableActionWrite,
    CheckinRequest,
    CheckinResponse,
    ExtractorInfo,
    StartupRequest,
)


def _startup_request() -> StartupRequest:
    return StartupRequest(
        external_id="my-extractor",
        extractor=ExtractorInfo(external_id="my-extractor", version="1.0.0"),
    )


@pytest.mark.parametrize(
    "action_type,task,expected_body",
    [
        (ActionType.start_task, "main", {"type": "start_task", "task": "main"}),
        (ActionType.stop_task, None, {"type": "stop_task"}),
        (ActionType.custom, None, {"type": "custom"}),
    ],
)
def test_available_action_write_serialization(
    action_type: ActionType, task: str | None, expected_body: dict[str, str]
) -> None:
    body = AvailableActionWrite(name="test", type=action_type, task=task).model_dump(mode="json", by_alias=True)
    for key, val in expected_body.items():
        assert body[key] == val


def test_available_action_write_none_fields_excluded() -> None:
    action = AvailableActionWrite(name="restart", type=ActionType.custom)
    body = action.model_dump(mode="json", by_alias=True)
    assert "description" not in body
    assert "task" not in body


@pytest.mark.parametrize("invalid_name", ["", "x" * 256])
def test_available_action_write_invalid_name_rejected(invalid_name: str) -> None:
    with pytest.raises(ValidationError, match="String should have"):
        AvailableActionWrite(name=invalid_name, type=ActionType.custom)


@pytest.mark.parametrize("status", [ActionStatus.pending, ActionStatus.cancel_pending])
def test_action_update_extractor_reserved_statuses_rejected(status: ActionStatus) -> None:
    with pytest.raises(ValidationError, match="Extractors cannot set action status"):
        ActionUpdate(external_id="act-1", status=status)


def test_action_update_camel_case_serialization() -> None:
    update = ActionUpdate(
        external_id="act-1",
        status=ActionStatus.succeeded,
        result_message="done",
    )
    body = update.model_dump(mode="json", by_alias=True)
    assert body["externalId"] == "act-1"
    assert body["status"] == "succeeded"
    assert body["resultMessage"] == "done"


def test_action_update_none_fields_excluded() -> None:
    update = ActionUpdate(external_id="act-1", status=ActionStatus.running)
    body = update.model_dump(mode="json", by_alias=True)
    assert "resultMessage" not in body
    assert "resultMetadata" not in body


def test_startup_request_available_actions_serialized() -> None:
    req = _startup_request()
    req.available_actions = [
        AvailableActionWrite(name="restart", type=ActionType.start_task, task="main"),
        AvailableActionWrite(name="ping", type=ActionType.custom),
    ]
    body = req.model_dump(mode="json", by_alias=True)
    assert "availableActions" in body
    assert len(body["availableActions"]) == 2
    assert body["availableActions"][0]["name"] == "restart"
    assert body["availableActions"][0]["type"] == "start_task"


def test_startup_request_available_actions_none_excluded() -> None:
    body = _startup_request().model_dump(mode="json", by_alias=True)
    assert "availableActions" not in body


def test_startup_request_available_actions_max_length_enforced() -> None:
    with pytest.raises(ValidationError, match="at most 100"):
        StartupRequest(
            external_id="x",
            extractor=ExtractorInfo(external_id="x"),
            available_actions=[AvailableActionWrite(name=f"a{i}", type=ActionType.custom) for i in range(101)],
        )


def test_action_camel_case_serialization() -> None:
    action = Action(external_id="act-1", action_name="restart", status=ActionStatus.running)
    body = action.model_dump(mode="json", by_alias=True)
    assert body["externalId"] == "act-1"
    assert body["actionName"] == "restart"
    assert body["status"] == "running"


def test_action_none_fields_excluded() -> None:
    action = Action(external_id="act-1", action_name="ping", status=ActionStatus.pending)
    body = action.model_dump(mode="json", by_alias=True)
    assert "callMetadata" not in body
    assert "resultMetadata" not in body


def test_action_unknown_fields_ignored() -> None:
    action = Action.model_validate(
        {"externalId": "act-1", "actionName": "ping", "status": "running", "unknownField": "unknown-value"}
    )
    assert action.external_id == "act-1"


def test_action_call_metadata_non_string_value_rejected() -> None:
    with pytest.raises(ValidationError, match="Input should be a valid string"):
        Action.model_validate(
            {"externalId": "act-1", "actionName": "ping", "status": "pending", "callMetadata": {"count": 3}}
        )


def test_checkin_request_action_updates_serialized() -> None:
    req = CheckinRequest(
        external_id="my-extractor",
        action_updates=[
            ActionUpdate(external_id="act-1", status=ActionStatus.running),
            ActionUpdate(external_id="act-2", status=ActionStatus.failed, result_message="timeout"),
        ],
    )
    body = req.model_dump(mode="json", by_alias=True)
    assert "actionUpdates" in body
    assert len(body["actionUpdates"]) == 2
    assert body["actionUpdates"][0]["status"] == "running"
    assert body["actionUpdates"][1]["resultMessage"] == "timeout"


def test_checkin_request_action_updates_none_excluded() -> None:
    body = CheckinRequest(external_id="x").model_dump(mode="json", by_alias=True)
    assert "actionUpdates" not in body


def test_checkin_request_action_updates_max_length_enforced() -> None:
    with pytest.raises(ValidationError, match="at most 100"):
        CheckinRequest(
            external_id="x",
            action_updates=[ActionUpdate(external_id=f"act-{i}", status=ActionStatus.running) for i in range(101)],
        )


def test_checkin_response_pending_actions_deserialized() -> None:
    response = CheckinResponse.model_validate(
        {
            "externalId": "my-extractor",
            "pendingActions": [
                {"externalId": "act-1", "actionName": "restart", "status": "pending"},
                {"externalId": "act-2", "actionName": "ping", "status": "cancel_pending"},
            ],
        }
    )
    assert response.pending_actions is not None
    assert len(response.pending_actions) == 2
    assert response.pending_actions[0].external_id == "act-1"
    assert response.pending_actions[0].status == ActionStatus.pending
    assert response.pending_actions[1].call_metadata is None


def test_checkin_response_unknown_fields_ignored() -> None:
    response = CheckinResponse.model_validate({"externalId": "x", "unknownField": "some-value", "anotherUnknown": 42})
    assert response.external_id == "x"


def test_checkin_response_pending_actions_none_when_absent() -> None:
    response = CheckinResponse.model_validate({"externalId": "x"})
    assert response.pending_actions is None


def test_checkin_response_pending_actions_empty_list_accepted() -> None:
    response = CheckinResponse.model_validate({"externalId": "x", "pendingActions": []})
    assert response.pending_actions == []


def test_checkin_response_action_with_call_metadata_deserialized() -> None:
    response = CheckinResponse.model_validate(
        {
            "externalId": "x",
            "pendingActions": [
                {
                    "externalId": "act-3",
                    "actionName": "configure",
                    "status": "pending",
                    "callMetadata": {"key": "value", "count": "3"},
                }
            ],
        }
    )
    assert response.pending_actions is not None
    action = response.pending_actions[0]
    assert isinstance(action.call_metadata, dict)
    assert action.call_metadata["key"] == "value"


@pytest.mark.parametrize("value", ["start_task", "stop_task", "custom"])
def test_action_type_lookup_by_value(value: str) -> None:
    assert ActionType(value).value == value


def test_action_type_invalid_value_raises() -> None:
    with pytest.raises(ValueError):
        ActionType("unknown_action")


@pytest.mark.parametrize("value", ["pending", "running", "failed", "succeeded", "cancel_pending", "canceled"])
def test_action_status_lookup_by_value(value: str) -> None:
    assert ActionStatus(value).value == value


def test_action_status_invalid_value_raises() -> None:
    with pytest.raises(ValueError):
        ActionStatus("unknown_status")
