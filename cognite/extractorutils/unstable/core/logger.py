from abc import ABC, abstractmethod
from logging import Logger, getLogger
from traceback import format_exception
from typing import Literal

from typing_extensions import assert_never

from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel


class CogniteLogger(ABC):
    def __init__(self) -> None:
        self._logger: Logger = getLogger()

    @abstractmethod
    def _new_error(
        self,
        level: ErrorLevel,
        description: str,
        *,
        details: str | None = None,
        task_name: str | None = None,
    ) -> Error:
        pass

    def debug(self, message: str) -> None:
        self._logger.debug(message)

    def info(self, message: str) -> None:
        self._logger.info(message)

    def begin_warning(
        self,
        message: str,
        *,
        details: str | None = None,
        auto_log: bool = True,
    ) -> Error:
        if auto_log:
            self._logger.warning(message)
        return self._new_error(
            level=ErrorLevel.warning,
            description=message,
            details=details,
        )

    def begin_error(
        self,
        message: str,
        *,
        details: str | None = None,
        auto_log: bool = True,
    ) -> Error:
        if auto_log:
            self._logger.error(message)
        return self._new_error(
            level=ErrorLevel.error,
            description=message,
            details=details,
        )

    def begin_fatal(
        self,
        message: str,
        *,
        details: str | None = None,
        auto_log: bool = True,
    ) -> Error:
        if auto_log:
            self._logger.critical(message)
        return self._new_error(
            level=ErrorLevel.fatal,
            description=message,
            details=details,
        )

    def warning(
        self,
        message: str,
        *,
        details: str | None = None,
        auto_log: bool = True,
    ) -> None:
        if auto_log:
            self._logger.warning(message)
        self._new_error(
            level=ErrorLevel.warning,
            description=message,
            details=details,
        ).instant()

    def error(
        self,
        message: str,
        *,
        details: str | None = None,
        auto_log: bool = True,
    ) -> None:
        if auto_log:
            self._logger.error(message)
        self._new_error(
            level=ErrorLevel.error,
            description=message,
            details=details,
        ).instant()

    def fatal(
        self,
        message: str,
        *,
        details: str | None = None,
        auto_log: bool = True,
    ) -> None:
        if auto_log:
            self._logger.critical(message)
        self._new_error(
            level=ErrorLevel.fatal,
            description=message,
            details=details,
        ).instant()

    def exception(
        self,
        message: str,
        exception: Exception,
        *,
        level: ErrorLevel = ErrorLevel.error,
        include_details: Literal["stack_trace"] | Literal["exception_message"] | bool = "exception_message",
        auto_log: bool = True,
    ) -> None:
        if auto_log:
            self._logger.log(level=level.log_level, msg=message, exc_info=exception)

        details: str | None
        match include_details:
            case "stack_trace":
                details = "".join(format_exception(exception))
            case "exception_message" | True:
                details = str(exception)
            case False:
                details = None
            case _:
                assert_never(include_details)

        self._new_error(
            level=level,
            description=message,
            details=details,
        ).instant()
