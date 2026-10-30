"""Tests for clean shutdown: final action statuses are sent on exit, and the parent-process watchdog."""

import logging
import os
import sys
import threading
from datetime import datetime, timezone
from threading import Event
from time import monotonic, sleep
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.statestore import NoStateStore
from cognite.extractorutils.unstable.core import base
from cognite.extractorutils.unstable.core._dto import Action, ActionStatus
from cognite.extractorutils.unstable.core.actions import ActionContext, CustomAction
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker

from .conftest import TestConfig, TestExtractor


def _make_extractor() -> tuple[TestExtractor, list[dict[str, Any]]]:
    """Extractor with a real CheckinWorker whose HTTP posts are captured in the returned list."""
    sent: list[dict[str, Any]] = []
    response = MagicMock()
    response.json.return_value = {"externalId": "test-integration"}

    def post(url: str, json: dict[str, Any], **_: Any) -> MagicMock:
        sent.append(json)
        return response

    client = MagicMock()
    client.config.project = "test-project"
    client.post.side_effect = post
    worker = CheckinWorker(client, "test-integration", logging.getLogger("test.shutdown"))
    worker._has_reported_startup = True

    conn = MagicMock()
    conn.integration.external_id = "test-integration"
    extractor = TestExtractor(
        FullConfig(
            connection_config=conn,
            application_config=TestConfig(parameter_one=1, parameter_two="a"),
            current_config_revision=1,
        ),
        worker,
    )
    extractor._start_time = datetime.now(tz=timezone.utc)
    extractor.state_store = NoStateStore()
    return extractor, sent


def _action_updates(sent: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {u["externalId"]: u for req in sent for u in req.get("actionUpdates", [])}


def _start_action(extractor: TestExtractor, name: str, external_id: str) -> None:
    extractor._handle_actions([Action(external_id=external_id, action_name=name, status=ActionStatus.pending)])


def _wait_for(event: Event) -> None:
    assert event.wait(5)


def test_exit_sends_canceled_for_cooperative_running_action() -> None:
    extractor, sent = _make_extractor()
    started = Event()

    def target(ctx: ActionContext) -> None:
        started.set()
        ctx.cancellation_token.wait()  # returns once shutdown cancels the token

    extractor.add_action(CustomAction(name="long", target=target))
    _start_action(extractor, "long", "act-1")
    _wait_for(started)

    extractor.__exit__(None, None, None)

    updates = _action_updates(sent)
    assert updates["act-1"]["status"] == "canceled"
    assert extractor._checkin_worker._action_updates == [], "update must actually be sent, not just requeued"


def test_exit_reports_canceled_for_stuck_action_after_bounded_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(base, "SHUTDOWN_ACTION_JOIN_TIMEOUT_SECONDS", 0.3)
    extractor, sent = _make_extractor()
    started = Event()
    release = Event()

    def target(ctx: ActionContext) -> None:
        started.set()
        release.wait(30)  # ignores cancellation, like a blocking call

    extractor.add_action(CustomAction(name="stuck", target=target))
    _start_action(extractor, "stuck", "act-2")
    _wait_for(started)

    begin = monotonic()
    try:
        extractor.__exit__(None, None, None)
        elapsed = monotonic() - begin
    finally:
        release.set()

    assert elapsed < 5, "shutdown must stay bounded"
    update = _action_updates(sent)["act-2"]
    assert update["status"] == "canceled"
    assert update["resultMessage"] == base.SHUTDOWN_ACTION_MESSAGE


def test_exit_without_actions_still_flushes_and_does_not_hang() -> None:
    extractor, sent = _make_extractor()
    extractor.__exit__(None, None, None)
    assert _action_updates(sent) == {}


def test_exit_flush_is_bounded_when_cdf_hangs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(base, "SHUTDOWN_FLUSH_TIMEOUT_SECONDS", 0.3)
    extractor, _ = _make_extractor()
    release = Event()
    extractor._checkin_worker._cognite_client.post.side_effect = lambda *a, **k: release.wait(30)  # type: ignore[attr-defined]
    begin = monotonic()
    try:
        extractor.__exit__(None, None, None)
    finally:
        release.set()
    assert monotonic() - begin < 5


# ---------------------------------------------------------------------------
# Parent process watchdog
# ---------------------------------------------------------------------------


def test_watchdog_cancels_token_when_parent_disappears(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(base, "PARENT_WATCHDOG_INTERVAL_SECONDS", 0.05)
    extractor, _ = _make_extractor()
    alive = Event()
    alive.set()
    monkeypatch.setattr(base, "_parent_is_alive", lambda pid: alive.is_set())

    extractor._setup_parent_watchdog(4242)
    sleep(0.3)
    assert not extractor.cancellation_token.is_cancelled

    alive.clear()  # simulate the parent dying (the real check sees a changed os.getppid())
    assert extractor.cancellation_token.wait(5)
    assert extractor.cancellation_token.is_cancelled


def test_attach_runtime_controls_without_parent_pid_starts_no_watchdog() -> None:
    extractor, _ = _make_extractor()
    extractor._attach_runtime_controls(cancel_event=MagicMock(), message_queue=MagicMock())
    assert not [t for t in threading.enumerate() if t.name == "ParentProcessWatchdog"]


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX re-parenting semantics")
def test_parent_is_alive_posix() -> None:
    assert base._parent_is_alive(os.getppid())
    assert not base._parent_is_alive(os.getppid() + 1)
