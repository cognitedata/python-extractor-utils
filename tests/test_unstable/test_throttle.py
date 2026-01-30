import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

import pytest

from cognite.extractorutils.unstable.core.throttle import TaskThrottle


def test_throttle_initialization() -> None:
    """Test throttle initialization with valid and invalid parameters."""

    throttle = TaskThrottle(max_concurrent=5)
    assert throttle.max_concurrent == 5

    with pytest.raises(ValueError, match="max_concurrent must be at least 1"):
        TaskThrottle(max_concurrent=0)

    with pytest.raises(ValueError, match="max_concurrent must be at least 1"):
        TaskThrottle(max_concurrent=-1)


def test_throttle_concurrency_limits() -> None:
    max_concurrent = 3
    throttle = TaskThrottle(max_concurrent=max_concurrent)

    concurrent_count = 0
    max_observed = 0
    lock = Lock()

    def task(task_id: int) -> int:
        nonlocal concurrent_count, max_observed

        with throttle.lease():
            with lock:
                concurrent_count += 1
                max_observed = max(max_observed, concurrent_count)

            time.sleep(0.1)

            with lock:
                concurrent_count -= 1

        return task_id

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(task, i) for i in range(10)]
        results = [f.result() for f in as_completed(futures)]

    assert len(results) == 10
    assert max_observed <= max_concurrent


def test_throttle_serial_execution() -> None:
    lock = Lock()
    throttle_serial = TaskThrottle(max_concurrent=1)
    execution_order = []

    def serial_task(task_id: int) -> None:
        with throttle_serial.lease():
            with lock:
                execution_order.append(task_id)
            time.sleep(0.05)
            with lock:
                execution_order.append(task_id)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(serial_task, i) for i in range(3)]
        for f in as_completed(futures):
            f.result()

    for i in range(0, len(execution_order) - 1, 2):
        task_id = execution_order[i]
        assert execution_order[i + 1] == task_id


def test_throttle_high_concurrency() -> None:
    lock = Lock()
    throttle_high = TaskThrottle(max_concurrent=50)
    completed = []

    def fast_task(task_id: int) -> int:
        with throttle_high.lease():
            time.sleep(0.01)
            with lock:
                completed.append(task_id)
            return task_id

    num_tasks = 100
    with ThreadPoolExecutor(max_workers=num_tasks) as executor:
        futures = [executor.submit(fast_task, i) for i in range(num_tasks)]
        for f in as_completed(futures):
            f.result()

    assert len(completed) == num_tasks
