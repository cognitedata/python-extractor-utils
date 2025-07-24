"""
Temporary holding place for DTOs against Extraction Pipelines 2.0 until it's in the SDK.
"""

from enum import Enum
from typing import Annotated, Any, Literal, Optional

from annotated_types import Len
from humps import camelize
from pydantic import BaseModel, ConfigDict, StringConstraints
from typing_extensions import TypeAliasType

from cognite.extractorutils.unstable.core.errors import Error as InternalError
from cognite.extractorutils.unstable.core.errors import ErrorLevel


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


class Task(CogniteModel):
    type: TaskType
    name: str
    action: bool = False
    description: DescriptionType | None = None


class StartupRequest(WithExternalId):
    extractor: ExtractorInfo
    tasks: TaskList | None = None
    active_config_revision: int | Literal["local"] | None = None
    timestamp: int | None = None


class CheckinRequest(WithExternalId):
    task_events: TaskUpdateList | None = None
    errors: ErrorList | None = None


class CheckinResponse(WithExternalId):
    last_config_revision: int | None = None
