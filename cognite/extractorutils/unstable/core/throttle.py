"""
Module containing the helper class for Throttling.
"""

from collections.abc import Callable, Generator
from contextlib import contextmanager
from functools import wraps
from threading import Semaphore
from typing import ParamSpec, TypeVar

P = ParamSpec("P")
T = TypeVar("T")


class TaskThrottle:
    """
    A throttle to limit the number of concurrent tasks using semaphores.

    Usage:
        As a decorator:
            >>> throttle = TaskThrottle(max_concurrent=5)
            >>> @throttle.limit
            ... def my_task(data):
            ...     # Process data
            ...     pass

        As a context manager:
            >>> throttle = TaskThrottle(max_concurrent=5)
            >>> with throttle.lease():
            ...     # Protected code block
            ...     pass
    """

    def __init__(self, max_concurrent: int) -> None:
        """
        Create a throttle with specified concurrency limit.

        Args:
            max_concurrent: Maximum number of tasks that can run concurrently
        """
        if max_concurrent < 1:
            raise ValueError("max_concurrent must be at least 1")
        self._semaphore: Semaphore = Semaphore(max_concurrent)
        self._max_concurrent: int = max_concurrent

    def limit(self, func: Callable[P, T]) -> Callable[P, T]:
        """
        Decorator to throttle a task function.
        """

        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            with self.lease():
                return func(*args, **kwargs)

        return wrapper

    @contextmanager
    def lease(self) -> Generator[None, None, None]:
        """
        Context manager that acquires/releases a throttle slot.
        """
        self._semaphore.acquire()
        try:
            yield
        finally:
            self._semaphore.release()

    @property
    def max_concurrent(self) -> int:
        """Get the configured concurrency limit."""
        return self._max_concurrent
