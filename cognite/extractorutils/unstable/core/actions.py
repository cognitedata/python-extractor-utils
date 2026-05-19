"""
This module defines the base classes for custom actions in the extractor framework.
"""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from cognite.extractorutils.unstable.core._dto import JSONType
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.unstable.core.logger import CogniteLogger

if TYPE_CHECKING:
    from cognite.extractorutils.unstable.core.base import Extractor

__all__ = ["ActionContext", "ActionTarget", "CustomAction"]


class ActionContext(CogniteLogger):
    """
    Context for a custom action invocation.

    This class is used to log errors and messages related to the action invocation.
    """

    def __init__(
        self,
        action: "CustomAction",
        extractor: "Extractor",
        external_id: str,
        call_metadata: JSONType | None = None,
    ) -> None:
        super().__init__()
        self._action = action
        self._extractor = extractor
        self.external_id = external_id
        self.call_metadata = call_metadata

        self._logger = logging.getLogger(
            f"{self._extractor.EXTERNAL_ID}.action.{self._action.name.replace(' ', '')}"
        )

    def _new_error(
        self,
        level: ErrorLevel,
        description: str,
        *,
        details: str | None = None,
        task_name: str | None = None,
    ) -> Error:
        return self._extractor._new_error(
            level=level,
            description=description,
            details=details,
        )


ActionTarget = Callable[["ActionContext"], None]


class CustomAction:
    """
    A user-invocable operation exposed by an extractor beyond start/stop of a task.

    Args:
        name: The name of the action.
        target: A callable that takes an ``ActionContext`` and performs the action.
        description: An optional description of the action.
    """

    def __init__(
        self,
        *,
        name: str,
        target: ActionTarget,
        description: str | None = None,
    ) -> None:
        self.name = name
        self.target = target
        self.description = description
