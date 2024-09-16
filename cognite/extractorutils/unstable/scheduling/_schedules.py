from abc import ABC, abstractmethod
from random import randint
from time import time

from croniter import croniter


class Schedule(ABC):
    @abstractmethod
    def next(self) -> int:
        pass


class CronSchedule(Schedule):
    def __init__(self, expression: str) -> None:
        self._cron = croniter(expression)

    def next(self) -> int:
        return int(self._cron.get_next(start_time=time()))


class IntervalSchedule(Schedule):
    def __init__(self, interval: int) -> None:
        self._interval = interval
        self._jitter = randint(0, interval)

        self._next = int(time() + self._jitter)

    def next(self) -> int:
        t = time()
        while t > self._next:
            self._next += self._interval

        return self._next
