from abc import ABC
from dataclasses import dataclass
from typing import Callable

from cognite.extractorutils.unstable.configuration.models import ScheduleConfig


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
