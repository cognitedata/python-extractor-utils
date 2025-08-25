"""
This module provides the ``CogniteLogger`` base class, which is an abstract base class for logging.

This class is subclassed by both the ``TaskContext`` and the ``Extractor`` base classes, providing a unified interface
for logging and error handling in extractors.
"""

import datetime
import os
from abc import ABC, abstractmethod
from logging import Logger, getLogger
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from traceback import format_exception
from typing import Literal

from typing_extensions import assert_never

from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel


class CogniteLogger(ABC):
    """
    Base class for logging and error handling in extractors.

    This class provides methods to log messages at different levels (debug, info, warning, error, fatal) and to
    create and manage errors that occur during the execution of an extractor.

    If you use this class instead of a standard logger, you will get additional functionality such as reporting errors
    back to CDF.
    """

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
        """
        Log a debug message.
        """
        self._logger.debug(message)

    def info(self, message: str) -> None:
        """
        Log an information message.
        """
        self._logger.info(message)

    def begin_warning(
        self,
        message: str,
        *,
        details: str | None = None,
        auto_log: bool = True,
    ) -> Error:
        """
        Begin a warning error.

        This will both log the message and create an error object that can be used to track and report the error.

        Args:
            message: The message to log and include in the error.
            details: Additional details about the error, if any.
            auto_log: If True, the message will be logged to the standard logging framework as well. Defaults to True.

        Returns:
            An ``Error`` object representing the warning error, tied to the current extractor instance.

        Examples:
            To track and complete an error, you can keep a reference to the error object and call its ``finish``
            method when the error is resolved, or use it in a context manager to automatically finish it:

            ... code-block:: python
                error = logger.begin_warning("This is a warning", details="Some details")
                # Do something
                error.finish()

            ... code-block:: python
                with logger.begin_warning("This is a warning", details="Some details")
                    # Do something
        """
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
        """
        Begin an error.

        This will both log the message and create an error object that can be used to track and report the error.

        Args:
            message: The message to log and include in the error.
            details: Additional details about the error, if any.
            auto_log: If True, the message will be logged to the standard logging framework as well. Defaults to True.

        Returns:
            An ``Error`` object representing the error, tied to the current extractor instance.

        Examples:
            To track and complete an error, you can keep a reference to the error object and call its ``finish``
            method when the error is resolved, or use it in a context manager to automatically finish it:

            ... code-block:: python
                error = logger.begin_error("This is an error", details="Some details")
                # Do something
                error.finish()

            ... code-block:: python
                with logger.begin_error("This is an error", details="Some details")
                    # Do something
        """
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
        """
        Begin a fatal error.

        This will both log the message and create an error object that can be used to track and report the error.

        Args:
            message: The message to log and include in the error.
            details: Additional details about the error, if any.
            auto_log: If True, the message will be logged to the standard logging framework as well. Defaults to True.

        Returns:
            An ``Error`` object representing the fatal error, tied to the current extractor instance.

        Examples:
            To track and complete an error, you can keep a reference to the error object and call its ``finish``
            method when the error is resolved, or use it in a context manager to automatically finish it:

            ... code-block:: python
                error = logger.begin_fatal("This is a fatal error", details="Some details")
                # Do something
                error.finish()

            ... code-block:: python
                with logger.begin_fatal("This is a fatal error", details="Some details")
                    # Do something
        """
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
        """
        Report an instant warning.

        This will log the message and create an error object that is marked as instant, meaning it does not have a
        duration.

        Args:
            message: The message to log and include in the error.
            details: Additional details about the error, if any.
            auto_log: If True, the message will be logged to the standard logging framework as well. Defaults to True.
        """
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
        """
        Report an instant error.

        This will log the message and create an error object that is marked as instant, meaning it does not have a
        duration.

        Args:
            message: The message to log and include in the error.
            details: Additional details about the error, if any.
            auto_log: If True, the message will be logged to the standard logging framework as well. Defaults to True.
        """
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
        """
        Report an instant fatal.

        This will log the message and create an error object that is marked as instant, meaning it does not have a
        duration.

        Args:
            message: The message to log and include in the error.
            details: Additional details about the error, if any.
            auto_log: If True, the message will be logged to the standard logging framework as well. Defaults to True.
        """
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
        """
        Report an exception as an error.

        This will log the message and create an error object that is marked as instant, meaning it does not have a
        duration. The exception details can be included in the error.

        Args:
            message: The message to log and include in the error.
            exception: The exception to report.
            level: The severity level of the error. Defaults to ``ErrorLevel.error``.
            include_details: How to include details about the exception. Can be "stack_trace", "exception_message",
                or True (equivalent to "exception_message"). If False, no details are included. Defaults to
                "exception_message".
            auto_log: If True, the message will be logged to the standard logging framework as well. Defaults to True.
        """
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


class RobustFileHandler(TimedRotatingFileHandler):
    """
    A TimedRotatingFileHandler that gracefully handles directory/permission issues.

    It can automatically create log directories and raise error to fallback to console logging
    if the file cannot be created or accessed.
    """

    def __init__(
        self,
        filename: Path,
        create_dirs: bool = True,
        when: str = "h",
        interval: int = 1,
        backupCount: int = 0,
        encoding: str | None = None,
        delay: bool = False,
        utc: bool = False,
        atTime: datetime.time | None = None,
        errors: str | None = None,
    ) -> None:
        self.create_dirs = create_dirs

        if self.create_dirs:
            directory = filename.parent
            directory.mkdir(parents=True, exist_ok=True)
            if not os.access(directory, os.W_OK):
                raise PermissionError(f"Cannot write to directory: {directory}")

        super().__init__(
            filename,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding=encoding,
            delay=delay,
            utc=utc,
            atTime=atTime,
            errors=errors,
        )

        self.stream.write("")
        self.stream.flush()
