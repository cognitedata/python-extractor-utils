from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass

from cognite.extractorutils.unstable.configuration.models import ScheduleConfig

__all__ = ["ScheduledTask", "ContinuousTask", "StartupTask", "Task"]


@dataclass
class _Task(ABC):
    name: str
    target: Callable[[], None]


@dataclass
class ScheduledTask(_Task):
    schedule: ScheduleConfig


@dataclass
class ContinuousTask(_Task):
    pass


class StartupTask(_Task):
    pass


# Making a type union to help with exhaustion checks in matches
Task = ScheduledTask | ContinuousTask | StartupTask
