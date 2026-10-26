"""Tests that queuing an action update wakes the periodic check-in loop early (debounced)."""

import logging
from collections.abc import Callable
from threading import Event, Thread
from time import monotonic, sleep
from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.core import checkin_worker as cw
from cognite.extractorutils.unstable.core._dto import ActionStatus, ActionUpdate, StartupRequest
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker

DEBOUNCE = 0.2
LONG_INTERVAL = 60.0


class _Harness:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(cw, "ACTION_UPDATE_DEBOUNCE_SECONDS", DEBOUNCE)
        self.worker = CheckinWorker(MagicMock(), "integration", logging.getLogger("test.early"))
        self.flush_times: list[float] = []
        self.sent: list[ActionUpdate] = []
        self.token = CancellationToken()
        self.first_flush = Event()

        def fake_flush(token: CancellationToken) -> None:
            self.flush_times.append(monotonic())
            with self.worker._lock:
                self.sent.extend(self.worker._action_updates)
                self.worker._action_updates = []
            self.first_flush.set()

        monkeypatch.setattr(self.worker, "flush", fake_flush)
        monkeypatch.setattr(self.worker, "_run_startup_report", lambda *a, **k: None)
        self.thread = Thread(
            target=self.worker.run_periodic_checkin,
            args=(self.token, MagicMock(spec=StartupRequest), LONG_INTERVAL),
            daemon=True,
        )

    def __enter__(self) -> "_Harness":
        self.thread.start()
        assert self.first_flush.wait(5)
        return self

    def __exit__(self, *_: object) -> None:
        self.token.cancel()
        self.thread.join(5)
        assert not self.thread.is_alive(), "loop must honor cancellation"


def _update(i: int = 0) -> ActionUpdate:
    return ActionUpdate(external_id=f"a{i}", status=ActionStatus.running)


def _wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    end = monotonic() + timeout
    while monotonic() < end:
        if predicate():
            return True
        sleep(0.01)
    return False


def test_update_triggers_early_checkin_within_debounce_window(monkeypatch: pytest.MonkeyPatch) -> None:
    with _Harness(monkeypatch) as h:
        queued_at = monotonic()
        h.worker.queue_action_update(_update())
        assert _wait_for(lambda: len(h.flush_times) == 2), "update waited for the periodic interval"
        assert h.flush_times[1] - queued_at < DEBOUNCE + 1.0
        assert h.flush_times[1] - queued_at >= DEBOUNCE * 0.9
        assert len(h.sent) == 1


def test_burst_of_updates_produces_one_extra_checkin(monkeypatch: pytest.MonkeyPatch) -> None:
    with _Harness(monkeypatch) as h:
        for i in range(50):
            h.worker.queue_action_update(_update(i))
        assert _wait_for(lambda: len(h.flush_times) == 2)
        sleep(DEBOUNCE * 3)
        assert len(h.flush_times) == 2
        assert len(h.sent) == 50


def test_no_update_keeps_periodic_cadence(monkeypatch: pytest.MonkeyPatch) -> None:
    with _Harness(monkeypatch) as h:
        sleep(DEBOUNCE * 3)
        assert len(h.flush_times) == 1


def test_cancellation_during_wait_stops_loop_without_extra_flush(monkeypatch: pytest.MonkeyPatch) -> None:
    h = _Harness(monkeypatch)
    with h:
        pass
    assert len(h.flush_times) == 1


def test_queue_update_before_loop_starts_does_not_fail() -> None:
    worker = CheckinWorker(MagicMock(), "integration", logging.getLogger("test.early"))
    worker.queue_action_update(_update())
    assert len(worker._action_updates) == 1
