import logging
from collections.abc import Callable
from traceback import format_exception
from typing import TYPE_CHECKING, Literal

from typing_extensions import assert_never

from cognite.extractorutils.unstable.configuration.models import (
    CronConfig,
    IntervalConfig,
    ScheduleConfig,
    TimeIntervalConfig,
)
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel

if TYPE_CHECKING:
    from cognite.extractorutils.unstable.core.base import Extractor

__all__ = ["ScheduledTask", "ContinuousTask", "StartupTask", "Task", "TaskContext"]


class TaskContext:
    def __init__(self, task: "Task", extractor: "Extractor"):
        self._task = task
        self._extractor = extractor

        self._logger = logging.getLogger(f"{self._extractor.EXTERNAL_ID}.{self._task.name.replace(' ', '')}")

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
        return self._extractor._error(
            level=ErrorLevel.warning,
            description=message,
            details=details,
            task_name=self._task.name,
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
        return self._extractor._error(
            level=ErrorLevel.error,
            description=message,
            details=details,
            task_name=self._task.name,
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
        return self._extractor._error(
            level=ErrorLevel.fatal,
            description=message,
            details=details,
            task_name=self._task.name,
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
        self._extractor._error(
            level=ErrorLevel.warning,
            description=message,
            details=details,
            task_name=self._task.name,
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
        self._extractor._error(
            level=ErrorLevel.error,
            description=message,
            details=details,
            task_name=self._task.name,
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
        self._extractor._error(
            level=ErrorLevel.fatal,
            description=message,
            details=details,
            task_name=self._task.name,
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

        self._extractor._error(
            level=level,
            description=message,
            details=details,
            task_name=self._task.name,
        ).instant()


TaskTarget = Callable[[TaskContext], None]


class _Task:
    def __init__(
        self,
        *,
        name: str,
        target: TaskTarget,
        description: str | None = None,
    ) -> None:
        self.name = name
        self.target = target
        self.description = description


class ScheduledTask(_Task):
    def __init__(
        self,
        *,
        name: str,
        target: TaskTarget,
        description: str | None = None,
        schedule: ScheduleConfig,
    ):
        super().__init__(name=name, target=target, description=description)
        self.schedule = schedule

    @classmethod
    def from_interval(
        cls, *, interval: str, name: str, target: TaskTarget, description: str | None = None
    ) -> "ScheduledTask":
        return ScheduledTask(
            name=name,
            target=target,
            description=description,
            schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig(interval)),
        )

    @classmethod
    def from_cron(cls, *, cron: str, name: str, target: TaskTarget, description: str | None = None) -> "ScheduledTask":
        return ScheduledTask(
            name=name,
            target=target,
            description=description,
            schedule=CronConfig(type="cron", expression=cron),
        )


class ContinuousTask(_Task):
    def __init__(
        self,
        *,
        name: str,
        target: TaskTarget,
        description: str | None = None,
    ) -> None:
        super().__init__(name=name, target=target, description=description)


class StartupTask(_Task):
    def __init__(
        self,
        *,
        name: str,
        target: TaskTarget,
        description: str | None = None,
    ) -> None:
        super().__init__(name=name, target=target, description=description)


# Making a type union to help with exhaustion checks in matches
Task = ScheduledTask | ContinuousTask | StartupTask
