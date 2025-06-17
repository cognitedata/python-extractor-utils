"""
This module defines the restart policies for extractors.

Is is used by the ``Runtime`` to determine whether an extractor should be restarted after a task failure.

It provides three predefined restart policies:
- ``NEVER``: The extractor will never be restarted.
- ``WHEN_ANY_TASK_CRASHES``: The extractor will be restarted if any task crashes.
- ``WHEN_CONTINUOUS_TASKS_CRASHES``: The extractor will be restarted only if a continuous task crashes.

Users can also define their own restart policies by providing a callable that takes a `Task` and an `Exception`
and returns a boolean indicating whether the extractor should be restarted.
"""

from collections.abc import Callable

from cognite.extractorutils.unstable.core.tasks import ContinuousTask, Task

RestartPolicy = Callable[[Task, Exception], bool]


def _false(_task: Task, _exception: Exception) -> bool:
    return False


def _true(_task: Task, _exception: Exception) -> bool:
    return True


def _is_continuous(task: Task, _exception: Exception) -> bool:
    return isinstance(task, ContinuousTask)


NEVER = _false
WHEN_CONTINUOUS_TASKS_CRASHES = _is_continuous
WHEN_ANY_TASK_CRASHES = _true

__all__ = [
    "NEVER",
    "WHEN_ANY_TASK_CRASHES",
    "WHEN_CONTINUOUS_TASKS_CRASHES",
    "RestartPolicy",
]
