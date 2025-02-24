import logging
from collections.abc import Callable
from typing import TYPE_CHECKING

from cognite.extractorutils.unstable.configuration.models import (
    CronConfig,
    IntervalConfig,
    ScheduleConfig,
    TimeIntervalConfig,
)
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.unstable.core.logger import CogniteLogger

if TYPE_CHECKING:
    from cognite.extractorutils.unstable.core.base import Extractor

__all__ = ["ScheduledTask", "ContinuousTask", "StartupTask", "Task", "TaskContext"]


class TaskContext(CogniteLogger):
    def __init__(self, task: "Task", extractor: "Extractor"):
        super().__init__()
        self._task = task
        self._extractor = extractor

        self._logger = logging.getLogger(f"{self._extractor.EXTERNAL_ID}.{self._task.name.replace(' ', '')}")

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
            task_name=self._task.name,
        )


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
