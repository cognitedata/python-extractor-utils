import pytest

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
    def test_camel_case_serialization(self) -> None:
        action = AvailableActionWrite(name="restart", type=ActionType.start_task, task="main")
        body = action.model_dump(mode="json", by_alias=True)
        assert body["type"] == "start_task"
        assert body["task"] == "main"

    def test_none_fields_excluded(self) -> None:
        action = AvailableActionWrite(name="restart", type=ActionType.custom)
        body = action.model_dump(mode="json", by_alias=True)
        assert "description" not in body
        assert "task" not in body

    def test_name_min_length_enforced(self) -> None:
        with pytest.raises(Exception):
            AvailableActionWrite(name="", type=ActionType.custom)

    def test_name_max_length_enforced(self) -> None:
        with pytest.raises(Exception):
            AvailableActionWrite(name="x" * 256, type=ActionType.custom)


class TestActionUpdate:
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
        with pytest.raises(Exception):
            StartupRequest(
                external_id="x",
                extractor=ExtractorInfo(external_id="x"),
                available_actions=[AvailableActionWrite(name=f"a{i}", type=ActionType.custom) for i in range(1001)],
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


class TestCheckinResponse:
    def test_pending_actions_deserialized(self) -> None:
        response = CheckinResponse.model_validate(
            {
                "externalId": "my-extractor",
                "pendingActions": [
                    {"externalId": "act-1", "actionName": "restart", "type": "start_task", "task": "main"},
                    {"externalId": "act-2", "actionName": "ping", "type": "custom"},
                ],
            }
        )
        assert response.pending_actions is not None
        assert len(response.pending_actions) == 2
        assert response.pending_actions[0].external_id == "act-1"
        assert response.pending_actions[0].type == ActionType.start_task
        assert response.pending_actions[1].call_metadata is None

    def test_unknown_fields_ignored(self) -> None:
        response = CheckinResponse.model_validate(
            {"externalId": "x", "newFieldFromOdin": "some-value", "anotherUnknown": 42}
        )
        assert response.external_id == "x"

    def test_pending_actions_none_when_absent(self) -> None:
        response = CheckinResponse.model_validate({"externalId": "x"})
        assert response.pending_actions is None

    def test_action_with_call_metadata_deserialized(self) -> None:
        response = CheckinResponse.model_validate(
            {
                "externalId": "x",
                "pendingActions": [
                    {
                        "externalId": "act-3",
                        "actionName": "configure",
                        "type": "custom",
                        "callMetadata": {"key": "value", "count": 3},
                    }
                ],
            }
        )
        assert response.pending_actions is not None
        action = response.pending_actions[0]
        assert isinstance(action.call_metadata, dict)
        assert action.call_metadata["key"] == "value"


class TestActionEnum:
    def test_action_type_values(self) -> None:
        assert ActionType.start_task.value == "start_task"
        assert ActionType.stop_task.value == "stop_task"
        assert ActionType.custom.value == "custom"

    def test_action_status_values(self) -> None:
        assert ActionStatus.pending.value == "pending"
        assert ActionStatus.running.value == "running"
        assert ActionStatus.failed.value == "failed"
        assert ActionStatus.succeeded.value == "succeeded"
        assert ActionStatus.cancel_pending.value == "cancel_pending"
        assert ActionStatus.canceled.value == "canceled"


class TestAction:
    def test_camel_case_serialization(self) -> None:
        action = Action(external_id="act-1", action_name="restart", type=ActionType.start_task, task="main")
        body = action.model_dump(mode="json", by_alias=True)
        assert body["externalId"] == "act-1"
        assert body["actionName"] == "restart"
        assert body["type"] == "start_task"

    def test_none_fields_excluded(self) -> None:
        action = Action(external_id="act-1", action_name="ping", type=ActionType.custom)
        body = action.model_dump(mode="json", by_alias=True)
        assert "task" not in body
        assert "callMetadata" not in body
