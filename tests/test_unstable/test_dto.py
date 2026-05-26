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


class TestAvailableActionWrite:
    @pytest.mark.parametrize(
        "action_type,task,expected_body",
        [
            (ActionType.start_task, "main", {"type": "start_task", "task": "main"}),
            (ActionType.stop_task, None, {"type": "stop_task"}),
            (ActionType.custom, None, {"type": "custom"}),
        ],
    )
    def test_serialization(self, action_type: ActionType, task: str | None, expected_body: dict[str, str]) -> None:
        body = AvailableActionWrite(name="test", type=action_type, task=task).model_dump(mode="json", by_alias=True)
        for key, val in expected_body.items():
            assert body[key] == val

    def test_none_fields_excluded(self) -> None:
        action = AvailableActionWrite(name="restart", type=ActionType.custom)
        body = action.model_dump(mode="json", by_alias=True)
        assert "description" not in body
        assert "task" not in body

    @pytest.mark.parametrize("invalid_name", ["", "x" * 256])
    def test_invalid_name_rejected(self, invalid_name: str) -> None:
        with pytest.raises(ValidationError, match="String should have"):
            AvailableActionWrite(name=invalid_name, type=ActionType.custom)


class TestActionUpdate:
    @pytest.mark.parametrize("status", [ActionStatus.pending, ActionStatus.cancel_pending])
    def test_extractor_reserved_statuses_rejected(self, status: ActionStatus) -> None:
        with pytest.raises(ValidationError, match="Extractors cannot set action status"):
            ActionUpdate(external_id="act-1", status=status)

    def test_camel_case_serialization(self) -> None:
        update = ActionUpdate(
            external_id="act-1",
            status=ActionStatus.succeeded,
            result_message="done",
        )
        body = update.model_dump(mode="json", by_alias=True)
        assert body["externalId"] == "act-1"
        assert body["status"] == "succeeded"
        assert body["resultMessage"] == "done"

    def test_none_fields_excluded(self) -> None:
        update = ActionUpdate(external_id="act-1", status=ActionStatus.running)
        body = update.model_dump(mode="json", by_alias=True)
        assert "resultMessage" not in body
        assert "resultMetadata" not in body


class TestStartupRequest:
    def test_available_actions_serialized(self) -> None:
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

    def test_available_actions_none_excluded(self) -> None:
        body = _startup_request().model_dump(mode="json", by_alias=True)
        assert "availableActions" not in body

    def test_available_actions_max_length_enforced(self) -> None:
        with pytest.raises(ValidationError, match="at most 100"):
            StartupRequest(
                external_id="x",
                extractor=ExtractorInfo(external_id="x"),
                available_actions=[AvailableActionWrite(name=f"a{i}", type=ActionType.custom) for i in range(101)],
            )


class TestAction:
    def test_camel_case_serialization(self) -> None:
        action = Action(external_id="act-1", action_name="restart", status=ActionStatus.running)
        body = action.model_dump(mode="json", by_alias=True)
        assert body["externalId"] == "act-1"
        assert body["actionName"] == "restart"
        assert body["status"] == "running"

    def test_none_fields_excluded(self) -> None:
        action = Action(external_id="act-1", action_name="ping", status=ActionStatus.pending)
        body = action.model_dump(mode="json", by_alias=True)
        assert "callMetadata" not in body
        assert "resultMetadata" not in body

    def test_unknown_fields_ignored(self) -> None:
        action = Action.model_validate(
            {"externalId": "act-1", "actionName": "ping", "status": "running", "unknownField": "unknown-value"}
        )
        assert action.external_id == "act-1"

    def test_call_metadata_non_string_value_rejected(self) -> None:
        with pytest.raises(ValidationError, match="Input should be a valid string"):
            Action.model_validate(
                {"externalId": "act-1", "actionName": "ping", "status": "pending", "callMetadata": {"count": 3}}
            )


class TestCheckinRequest:
    def test_action_updates_serialized(self) -> None:
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

    def test_action_updates_none_excluded(self) -> None:
        body = CheckinRequest(external_id="x").model_dump(mode="json", by_alias=True)
        assert "actionUpdates" not in body

    def test_action_updates_max_length_enforced(self) -> None:
        with pytest.raises(ValidationError, match="at most 100"):
            CheckinRequest(
                external_id="x",
                action_updates=[ActionUpdate(external_id=f"act-{i}", status=ActionStatus.running) for i in range(101)],
            )


class TestCheckinResponse:
    def test_pending_actions_deserialized(self) -> None:
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

    def test_unknown_fields_ignored(self) -> None:
        response = CheckinResponse.model_validate(
            {"externalId": "x", "unknownField": "some-value", "anotherUnknown": 42}
        )
        assert response.external_id == "x"

    def test_pending_actions_none_when_absent(self) -> None:
        response = CheckinResponse.model_validate({"externalId": "x"})
        assert response.pending_actions is None

    def test_pending_actions_empty_list_accepted(self) -> None:
        response = CheckinResponse.model_validate({"externalId": "x", "pendingActions": []})
        assert response.pending_actions == []

    def test_action_with_call_metadata_deserialized(self) -> None:
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


class TestActionEnum:
    @pytest.mark.parametrize("value", ["start_task", "stop_task", "custom"])
    def test_action_type_lookup_by_value(self, value: str) -> None:
        assert ActionType(value).value == value

    def test_action_type_invalid_value_raises(self) -> None:
        with pytest.raises(ValueError):
            ActionType("unknown_action")

    @pytest.mark.parametrize("value", ["pending", "running", "failed", "succeeded", "cancel_pending", "canceled"])
    def test_action_status_lookup_by_value(self, value: str) -> None:
        assert ActionStatus(value).value == value

    def test_action_status_invalid_value_raises(self) -> None:
        with pytest.raises(ValueError):
            ActionStatus("unknown_status")
