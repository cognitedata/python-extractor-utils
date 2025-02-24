from threading import Thread
from time import sleep, time

from pytest import approx

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import IntervalConfig, TimeIntervalConfig
from cognite.extractorutils.unstable.scheduling._scheduler import TaskScheduler

from .conftest import MockFunction


def test_interval_schedules() -> None:
    ct = CancellationToken()

    mock = MockFunction(sleep_time=1)

    scheduler = TaskScheduler(cancellation_token=ct.create_child_token())
    scheduler.schedule_task(
        name="test",
        schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig("3s")),
        task=mock,
    )
    start = time()
    Thread(target=scheduler.run).start()
    sleep(7)
    scheduler.stop()

    assert len(mock.called_times) == 3
    assert mock.called_times[0] == approx(start)
    assert mock.called_times[1] == approx(mock.called_times[0] + 3)
    assert mock.called_times[2] == approx(mock.called_times[1] + 3)


def test_overlapping_schedules() -> None:
    """
    Test with a trigger that fires when the job is still running

    Timeline:

    time    | 0 1 2 3 4 5 6 7 8 9 STOP
    --------|-------------------------
    trigger | x   x   x   x   x
    job     | |-----| |-----| |-----|
    """

    ct = CancellationToken()

    mock = MockFunction(sleep_time=3)

    scheduler = TaskScheduler(cancellation_token=ct.create_child_token())
    scheduler.schedule_task(
        name="test",
        schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig("2s")),
        task=mock,
    )
    start = time()
    Thread(target=scheduler.run).start()
    sleep(9)
    scheduler.stop()

    assert len(mock.called_times) == 3
    assert mock.called_times[0] == approx(start)
    assert mock.called_times[1] == approx(mock.called_times[0] + 4)
    assert mock.called_times[1] == approx(mock.called_times[1] + 4)


def test_manual() -> None:
    ct = CancellationToken()
    mock = MockFunction(sleep_time=0)

    scheduler = TaskScheduler(cancellation_token=ct.create_child_token())
    scheduler.schedule_task(
        name="test",
        schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig("1h")),
        task=mock,
    )

    Thread(target=scheduler.run).start()

    scheduler.trigger("test")
    sleep(0.1)
    scheduler.trigger("test")
    sleep(0.1)
    scheduler.trigger("test")

    sleep(1)

    scheduler.stop()

    assert len(mock.called_times) == 4


def test_manual_interval_mix() -> None:
    """
    Test with a scheduled trigger mixed with manual trigger, make sure there's no overlap

    Timeline:

    time     | 0 1 2 3 4 5 6 7 8 9 STOP
    ---------|-------------------------
    schedule | x       x       x
    manual   |   x   x
    job      | |---| |---|     |---|
    """

    ct = CancellationToken()
    mock = MockFunction(sleep_time=2)

    scheduler = TaskScheduler(cancellation_token=ct.create_child_token())
    scheduler.schedule_task(
        name="test",
        schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig("4s")),
        task=mock,
    )

    start = time()
    Thread(target=scheduler.run).start()
    sleep(1)
    first_trigger = scheduler.trigger("test")
    sleep(2)
    second_trigger = scheduler.trigger("test")

    sleep(6)
    scheduler.stop()

    assert not first_trigger
    assert second_trigger

    assert len(mock.called_times) == 3
    assert mock.called_times[0] == approx(start)
    assert mock.called_times[1] == approx(start + 3)
    assert mock.called_times[2] == approx(start + 8)
