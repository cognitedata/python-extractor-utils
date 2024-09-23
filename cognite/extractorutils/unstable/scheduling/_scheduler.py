from dataclasses import dataclass
from logging import getLogger
from threading import RLock, Thread
from time import time
from typing import Callable

import arrow
from humps import pascalize

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import CronConfig, IntervalConfig, ScheduleConfig
from cognite.extractorutils.unstable.scheduling._schedules import CronSchedule, IntervalSchedule, Schedule


@dataclass
class Job:
    name: str
    call: Callable[[], None]
    schedule: Schedule

    def __hash__(self) -> int:
        return hash(self.name)


class TaskScheduler:
    def __init__(self, cancellation_token: CancellationToken) -> None:
        self._cancellation_token = cancellation_token
        self._jobs: dict[str, Job] = {}
        self._jobs_lock = RLock()
        self._running: set[Job] = set()
        self._running_lock = RLock()

        self._logger = getLogger()

    def schedule_task(self, name: str, schedule: ScheduleConfig, task: Callable[[], None]) -> None:
        parsed_schedule: Schedule
        match schedule:
            case CronConfig() as cron_config:
                parsed_schedule = CronSchedule(expression=cron_config.expression)

            case IntervalConfig() as interval_config:
                parsed_schedule = IntervalSchedule(interval=interval_config.expression.seconds)

        with self._jobs_lock:
            if name in self._jobs:
                raise KeyError(f"Job '{name}' is already added to the scheduler")
            self._jobs[name] = Job(name=name, call=task, schedule=parsed_schedule)

    def _get_next(self) -> list[Job]:
        if not self._jobs:
            return []
        with self._jobs_lock:
            next_runs = sorted([(j.schedule.next(), j) for j in self._jobs.values()], key=lambda tup: tup[0])
        return [job for (next, job) in next_runs if next == next_runs[0][0]] if next_runs else []

    def _run_job(self, job: Job) -> bool:
        with self._running_lock:
            if job in self._running:
                self._logger.warning(f"Job {job.name} already running")
                return False

        def wrap() -> None:
            with self._running_lock:
                self._running.add(job)
            try:
                job.call()

                self._logger.info(f"Job {job.name} done. Next run at {arrow.get(job.schedule.next()).isoformat()}")

            finally:
                with self._running_lock:
                    self._running.remove(job)

        Thread(target=wrap, name=f"Run{pascalize(job.name)}").start()
        return True

    def trigger(self, name: str) -> bool:
        return self._run_job(self._jobs[name])

    def run(self) -> None:
        if not self._jobs:
            raise ValueError("Can't run scheduler without any scheduled tasks")

        # Run all interval jobs on startup since the first next() is one interval from now
        for job in [j for j in self._jobs.values() if isinstance(j.schedule, IntervalSchedule)]:
            self.trigger(job.name)

        while not self._cancellation_token.is_cancelled:
            next_runs = self._get_next()

            next_time = next_runs[0].schedule.next()
            wait_time = max(next_time - time(), 0)

            if wait_time:
                self._logger.info(f"Waiting until {arrow.get(next_time).isoformat()}")
                if self._cancellation_token.wait(wait_time):
                    break

            for job in next_runs:
                self._logger.info(f"Starting job {job.name}")
                self._run_job(job)

    def stop(self) -> None:
        self._cancellation_token.cancel()
