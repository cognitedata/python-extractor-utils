"""
This module defines the base classes for custom actions in the extractor framework.
"""

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Generic

from cognite.extractorutils.unstable.configuration.models import ConfigType
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.unstable.core.logger import CogniteLogger

if TYPE_CHECKING:
    from cognite.client import CogniteClient

    from cognite.extractorutils.unstable.core.base import Extractor

__all__ = ["ActionContext", "ActionError", "ActionTarget", "CustomAction"]


class ActionContext(Generic[ConfigType], CogniteLogger):
    """
    Context for a custom action invocation.

    This class is used to log errors and messages related to the action invocation.

    ``external_id`` and ``call_metadata`` come from the pending action payload sent by Odin and are
    available for use when reporting results back to the server.
    """

    def __init__(
        self,
        action: "CustomAction",
        extractor: "Extractor[ConfigType]",
        external_id: str,
        call_metadata: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        self._action = action
        self._extractor = extractor
        self.external_id = external_id
        self.call_metadata = call_metadata
        self._result_message: str | None = None
        self._result_metadata: dict[str, str] | None = None

        self._logger = logging.getLogger(f"{self._extractor.EXTERNAL_ID}.action.{self._action.name.replace(' ', '')}")

    @property
    def application_config(self) -> ConfigType:
        """The extractor's application configuration."""
        return self._extractor.application_config

    @property
    def cdf_client(self) -> "CogniteClient":
        """The Cognite client for interacting with CDF."""
        return self._extractor.cognite_client

    @property
    def integration_external_id(self) -> str:
        """The external ID of the integration this extractor is registered as."""
        return self._extractor.connection_config.integration.external_id

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
            task_name=task_name if task_name is not None else self._action.name,
        )

    def set_result(self, message: str, *, metadata: dict[str, str] | None = None) -> None:
        """Record the result for a successful action completion."""
        if self._result_message is not None:
            raise RuntimeError(
                f"set_result() has already been called for this action invocation; "
                f"existing message: {self._result_message!r}"
            )
        self._result_message = message
        self._result_metadata = metadata


class ActionError(Exception):
    """Deliberate action failure with structured metadata for Odin result reporting."""

    def __init__(self, message: str, *, error_type: str, details: str | None = None) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.details = details

    @property
    def result_metadata(self) -> dict[str, str]:
        """Structured metadata dict for the action update."""
        meta: dict[str, str] = {"error_type": self.error_type}
        if self.details is not None:
            meta["error_detail"] = self.details
        return meta


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
