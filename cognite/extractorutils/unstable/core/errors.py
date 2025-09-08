"""
This module defines the Error and ErrorLevel classes for reporting errors in extractors.
"""

import logging
from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING
from uuid import uuid4

from typing_extensions import assert_never

from cognite.extractorutils.util import now

if TYPE_CHECKING:
    from .base import Extractor

__all__ = ["Error", "ErrorLevel"]


class ErrorLevel(Enum):
    """
    Enumeration of error levels for reporting errors in extractors.
    """

    warning = "warning"
    error = "error"
    fatal = "fatal"

    @property
    def log_level(self) -> int:
        """
        Returns the corresponding logging level for the error level.
        """
        match self:
            case ErrorLevel.warning:
                return logging.WARNING
            case ErrorLevel.error:
                return logging.ERROR
            case ErrorLevel.fatal:
                return logging.CRITICAL
            case _:
                assert_never(self)


class Error:
    """
    Represents an error that occurred during the run of an extractor.

    This class should not be instantiated directly. Instead, use the ``CogniteLogger`` methods (either in the
    TaskContext or the extractor base class) to create errors.

    Args:
        level: The severity level of the error.
        description: A brief description of the error.
        details: Additional details about the error, if any.
        task_name: The name of the task during which the error occurred, if applicable.
        extractor: The extractor instance that reported the error.
    """

    def __init__(
        self,
        level: ErrorLevel,
        description: str,
        details: str | None,
        task_name: str | None,
        extractor: "Extractor",
    ) -> None:
        self.level = level
        self.description = description
        self.details = details

        self.external_id = str(uuid4())
        self.start_time = now()
        self.end_time: int | None = None

        self._extractor = extractor
        self._task_name = task_name

        self._extractor._report_error(self)

    def instant(self) -> None:
        """
        Make this error an instant error, meaning it does not have a duration.
        """
        # Only end the error once
        if self.end_time is not None:
            return

        self.end_time = self.start_time

        # Re-add in case the error has already been reported and dict cleared
        self._extractor._try_report_error(self)

    def finish(self) -> None:
        """
        Mark the error as finished, setting the end time to the current time.

        This method should be called when the error is resolved or no longer relevant.
        """
        # Only end the error once
        if self.end_time is not None:
            return

        self.end_time = now()

        # Re-add in case the error has already been reported and dict cleared
        self._extractor._try_report_error(self)

    def __enter__(self) -> "Error":
        """
        Start tracking an error as a context manager.

        This allows the error to be automatically finished when exiting the context.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """
        Finish the error context manager, marking the error as finished.
        """
        self.finish()
        return exc_val is None
