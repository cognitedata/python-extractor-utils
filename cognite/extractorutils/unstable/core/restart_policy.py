from typing import Callable

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
    "RestartPolicy",
    "NEVER",
    "WHEN_CONTINUOUS_TASKS_CRASHES",
    "WHEN_ANY_TASK_CRASHES",
]
