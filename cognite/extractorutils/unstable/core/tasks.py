from collections.abc import Callable

from cognite.extractorutils.unstable.configuration.models import (
    CronConfig,
    IntervalConfig,
    ScheduleConfig,
    TimeIntervalConfig,
)

__all__ = ["ScheduledTask", "ContinuousTask", "StartupTask", "Task"]


class _Task:
    def __init__(
        self,
        *,
        name: str,
        target: Callable[[], None],
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
        target: Callable[[], None],
        description: str | None = None,
        schedule: ScheduleConfig,
    ):
        super().__init__(name=name, target=target, description=description)
        self.schedule = schedule

    @classmethod
    def from_interval(
        cls, *, interval: str, name: str, target: Callable[[], None], description: str | None = None
    ) -> "ScheduledTask":
        return ScheduledTask(
            name=name,
            target=target,
            description=description,
            schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig(interval)),
        )

    @classmethod
    def from_cron(
        cls, *, cron: str, name: str, target: Callable[[], None], description: str | None = None
    ) -> "ScheduledTask":
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
        target: Callable[[], None],
        description: str | None = None,
    ) -> None:
        super().__init__(name=name, target=target, description=description)


class StartupTask(_Task):
    def __init__(
        self,
        *,
        name: str,
        target: Callable[[], None],
        description: str | None = None,
    ) -> None:
        super().__init__(name=name, target=target, description=description)


# Making a type union to help with exhaustion checks in matches
Task = ScheduledTask | ContinuousTask | StartupTask
