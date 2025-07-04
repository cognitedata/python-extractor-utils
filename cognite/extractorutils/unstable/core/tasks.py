"""
This module defines the base classes for tasks in the extractor framework.
"""

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

__all__ = ["ContinuousTask", "ScheduledTask", "StartupTask", "Task", "TaskContext"]


class TaskContext(CogniteLogger):
    """
    Context for a task execution.

    This class is used to log errors and messages related to the task execution.
    """

    def __init__(self, task: "Task", extractor: "Extractor") -> None:
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
    """
    A task that is scheduled to run at specific intervals or according to a cron expression.

    This class allows you to define tasks that can be scheduled using either an interval or a cron expression.

    Args:
        name: The name of the task.
        target: A callable that takes a ``TaskContext`` and performs the task.
        description: An optional description of the task.
        schedule: A ``ScheduleConfig`` object that defines the scheduling configuration for the task.
    """

    def __init__(
        self,
        *,
        name: str,
        target: TaskTarget,
        description: str | None = None,
        schedule: ScheduleConfig,
    ) -> None:
        super().__init__(name=name, target=target, description=description)
        self.schedule = schedule

    @classmethod
    def from_interval(
        cls, *, interval: str, name: str, target: TaskTarget, description: str | None = None
    ) -> "ScheduledTask":
        """
        Create a scheduled task that runs at regular intervals.

        Args:
            interval: A string representing the time interval (e.g., "5m" for 5 minutes).
            name: The name of the task.
            target: A callable that takes a ``TaskContext`` and performs the task.
            description: An optional description of the task.
        """
        return ScheduledTask(
            name=name,
            target=target,
            description=description,
            schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig(interval)),
        )

    @classmethod
    def from_cron(cls, *, cron: str, name: str, target: TaskTarget, description: str | None = None) -> "ScheduledTask":
        """
        Create a scheduled task that runs according to a cron expression.

        Args:
            cron: A string representing the cron expression (e.g., "0 0 * * *" for daily at midnight).
            name: The name of the task.
            target: A callable that takes a ``TaskContext`` and performs the task.
            description: An optional description of the task.
        """
        return ScheduledTask(
            name=name,
            target=target,
            description=description,
            schedule=CronConfig(type="cron", expression=cron),
        )


class ContinuousTask(_Task):
    """
    A task that runs continuously.

    Continuous tasks are started when the extractor starts and are expected to run until the extractor stops.
    """

    def __init__(
        self,
        *,
        name: str,
        target: TaskTarget,
        description: str | None = None,
    ) -> None:
        super().__init__(name=name, target=target, description=description)


class StartupTask(_Task):
    """
    A task that runs once at the startup of the extractor.

    Startup tasks are executed before any continuous or scheduled tasks and are typically used for initialization.
    """

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
