"""
Temporary holding place for DTOs against Extraction Pipelines 2.0 until it's in the SDK.

Jira ticket: https://cognitedata.atlassian.net/browse/EDGE-493
"""

from enum import Enum
from typing import Annotated, Any, Literal, Optional

from annotated_types import Len
from humps import camelize
from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator
from typing_extensions import TypeAliasType

from cognite.extractorutils.unstable.core.errors import Error as InternalError
from cognite.extractorutils.unstable.core.errors import ErrorLevel

MAX_METADATA_VALUE_BYTES = 512
"""CDF's per-value size limit for metadata dict fields (e.g. action result metadata). The Integrations
API rejects the whole checkin request otherwise, with "Metadata values may be at most 512 bytes"."""


def oversized_metadata_fields(metadata: dict[str, str] | None) -> list[str]:
    """Return the keys of ``metadata`` whose UTF-8 encoded value exceeds ``MAX_METADATA_VALUE_BYTES``."""
    if not metadata:
        return []
    return [key for key, value in metadata.items() if len(value.encode("utf-8")) > MAX_METADATA_VALUE_BYTES]


class CogniteModel(BaseModel):
    """
    Base class for DTO classes based on pydantic.

    With a few tweaks to make it inline with the CDF API guidelines:
      * camelCase instead of snake_case when serializing/deserializing into/from JSON
      * exclude Nones from serialized JSON instead of having nulls in the response text.
    """

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        if kwargs:
            kwargs["exclude_none"] = True
        else:
            kwargs = {"exclude_none": True}
        return BaseModel.model_dump(self, *args, **kwargs)

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:  # noqa: ANN401
        return self.model_dump(*args, **kwargs)

    model_config = ConfigDict(alias_generator=camelize, populate_by_name=True, extra="forbid")


class WithExternalId(CogniteModel):
    external_id: str


MessageType = Annotated[str, StringConstraints(min_length=0, max_length=1000)]


class TaskUpdate(CogniteModel):
    type: Literal["started"] | Literal["ended"]
    name: str
    timestamp: int
    message: MessageType | None = None


class Error(WithExternalId):
    level: ErrorLevel
    description: str
    details: str | None
    start_time: int
    end_time: int | None
    task: str | None
    type: Literal["config"] | None = None
    config_revision: int | None = None

    @classmethod
    def from_internal(cls, error: InternalError) -> "Error":
        """Convert the error into a DTO (Data Transfer Object) for reporting."""
        return Error(
            external_id=error.external_id,
            level=error.level,
            description=error.description,
            details=error.details,
            start_time=error.start_time,
            end_time=error.end_time,
            task=error._task_name,
        )


TaskUpdateList = Annotated[list[TaskUpdate], Len(min_length=1, max_length=1000)]
ErrorList = Annotated[list[Error], Len(min_length=0, max_length=1000)]
VersionType = Annotated[str, StringConstraints(min_length=1, max_length=32)]
DescriptionType = Annotated[str, StringConstraints(min_length=0, max_length=500)]
IdentifierType = Annotated[str, StringConstraints(min_length=1, max_length=255)]
TaskList = Annotated[list["Task"], Len(min_length=1, max_length=1000)]
JSONType = TypeAliasType(  # type: ignore[misc]
    "JSONType",
    bool | int | float | str | None | list[Optional["JSONType"]] | dict[str, Optional["JSONType"]],  # type: ignore[misc]
)


class WithVersion(CogniteModel):
    version: VersionType | None = None


class ExtractorInfo(WithExternalId, WithVersion):
    pass


class TaskType(Enum):
    continuous = "continuous"
    batch = "batch"


class ActionType(Enum):
    start_task = "start_task"
    stop_task = "stop_task"
    custom = "custom"


class ActionStatus(Enum):
    pending = "pending"
    running = "running"
    failed = "failed"
    succeeded = "succeeded"
    cancel_pending = "cancel_pending"
    canceled = "canceled"


class Task(CogniteModel):
    type: TaskType
    name: str
    action: bool = False
    description: DescriptionType | None = None


class AvailableActionWrite(CogniteModel):
    name: IdentifierType
    type: ActionType
    description: MessageType | None = None
    task: IdentifierType | None = None


class Action(CogniteModel):
    """Server may add fields before SDK is updated, so ignore extras on deserialize."""

    model_config = ConfigDict(extra="ignore")

    external_id: IdentifierType
    action_name: IdentifierType
    status: ActionStatus
    call_metadata: dict[str, str] | None = None
    created_time: int | None = None
    last_updated_time: int | None = None
    result_message: MessageType | None = None
    result_metadata: dict[str, str] | None = None


class ActionUpdate(CogniteModel):
    external_id: IdentifierType
    status: ActionStatus
    result_message: MessageType | None = None
    result_metadata: dict[str, str] | None = None

    @field_validator("status")
    @classmethod
    def validate_status(cls, v: ActionStatus) -> ActionStatus:
        if v in (ActionStatus.pending, ActionStatus.cancel_pending):
            raise ValueError(f"Extractors cannot set action status to '{v.value}'")
        return v


AvailableActionList = Annotated[list[AvailableActionWrite], Len(min_length=0, max_length=100)]
ActionUpdateList = Annotated[list[ActionUpdate], Len(min_length=0, max_length=100)]


class StartupRequest(WithExternalId):
    extractor: ExtractorInfo
    tasks: TaskList | None = None
    active_config_revision: int | Literal["local"] | None = None
    timestamp: int | None = None
    available_actions: AvailableActionList | None = None


class CheckinRequest(WithExternalId):
    task_events: TaskUpdateList | None = None
    errors: ErrorList | None = None
    action_updates: ActionUpdateList | None = None


class CheckinResponse(WithExternalId):
    """Server may add fields before SDK is updated, so ignore extras on deserialize."""

    model_config = ConfigDict(extra="ignore")

    last_config_revision: int | None = None
    pending_actions: list[Action] | None = None
