import typing
from enum import Enum
from types import TracebackType
from uuid import uuid4

from cognite.extractorutils.util import now

if typing.TYPE_CHECKING:
    from .base import Extractor

__all__ = ["Error", "ErrorLevel"]


class ErrorLevel(Enum):
    warning = "warning"
    error = "error"
    fatal = "fatal"


class Error:
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
        # Only end the error once
        if self.end_time is not None:
            return

        self.end_time = self.start_time

        # Re-add in case the error has already been reported and dict cleared
        self._extractor._report_error(self)

    def finish(self) -> None:
        # Only end the error once
        if self.end_time is not None:
            return

        self.end_time = now()

        # Re-add in case the error has already been reported and dict cleared
        self._extractor._report_error(self)

    def __enter__(self) -> "Error":
        return self

    def __exit__(
        self,
        exc_type: typing.Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        self.finish()
        return exc_val is None
