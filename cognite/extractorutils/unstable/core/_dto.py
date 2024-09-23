"""
Temporary holding place for DTOs against Extraction Pipelines 2.0 until it's in the SDK
"""

from typing import Any, Literal

from humps import camelize
from pydantic import BaseModel, ConfigDict


class CogniteModel(BaseModel):
    """
    Base class for DTO classes based on pydantic, but with a few tweaks to make it inline with the CDF API guidelines:
      * camelCase instead of snake_case when serializing/deserializing into/from JSON
      * exclude Nones from serialized JSON instead of having nulls in the response text
    """

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        if kwargs:
            kwargs["exclude_none"] = True
        else:
            kwargs = {"exclude_none": True}
        return BaseModel.model_dump(self, *args, **kwargs)

    def dict(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.model_dump(*args, **kwargs)

    model_config = ConfigDict(alias_generator=camelize, populate_by_name=True, extra="forbid")


class TaskUpdate(CogniteModel):
    type: Literal["started"] | Literal["ended"]
    name: str
    timestamp: int
