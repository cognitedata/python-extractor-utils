"""Tests for CheckinWorker action dispatch (Task 5).

Follows the same patterns as test_checkin_worker.py:
  - requests_mock + gzip-decoded body inspection
  - connection_config / checkin_bag / mock_startup_request fixtures
  - Thread + join + cancellation_token pattern
"""

import gzip
import json
import logging
import threading
from collections.abc import Callable, Generator
from datetime import datetime, timezone
from multiprocessing import Event, Queue
from threading import Thread
from typing import Any

import pytest
import requests_mock as requests_mock_lib

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core._dto import Action, ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.util import now
from tests.test_unstable.conftest import TestConfig, TestExtractor

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def action_updates_bag() -> Generator[list, None]:
    bag: list = []
    yield bag
    bag.clear()


@pytest.fixture
def mock_checkin_with_actions(
    connection_config: ConnectionConfig,
    checkin_bag: list,
    action_updates_bag: list,
) -> Callable[[requests_mock_lib.Mocker, list[dict] | None, int], None]:
    """
    Like mock_checkin_request, but also captures actionUpdates and can embed
    pendingActions in the response JSON.
    """

    def mocker(
        mock: requests_mock_lib.Mocker,
        pending_actions: list[dict] | None = None,
        status_code: int = 200,
    ) -> None:
        def json_callback(request: Any, context: Any) -> dict:
            if status_code != 200:
                return {"error": {"message": "Request failed", "code": status_code}}
            req = json.loads(gzip.decompress(request.body).decode("utf-8"))
            checkin_bag.append(req)
            if "actionUpdates" in req:
                action_updates_bag.extend(req["actionUpdates"])
            resp: dict = {"externalId": connection_config.integration.external_id}
            if pending_actions:
                resp["pendingActions"] = pending_actions
            return resp

        mock.register_uri(
            method="POST",
            url=f"{connection_config.base_url}/api/v1/projects/{connection_config.project}/integrations/checkin",
            json=json_callback,
            status_code=status_code,
        )

    return mocker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_worker(connection_config: ConnectionConfig) -> CheckinWorker:
    return CheckinWorker(
        connection_config.get_cognite_client("test_checkin_actions"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )


def _make_extractor(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    worker: CheckinWorker,
) -> TestExtractor:
    extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config,
            application_config=application_config,
            current_config_revision=1,
        ),
        worker,
    )
    extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    extractor._attach_runtime_controls(cancel_event=Event(), message_queue=Queue())
    return extractor


# ===========================================================================
# set_action_dispatcher
# ===========================================================================


def test_set_action_dispatcher_replaces_previous(
    connection_config: ConnectionConfig,
) -> None:
    """Calling set_action_dispatcher a second time replaces the first dispatcher."""
    worker = _make_worker(connection_config)
    first = lambda actions: None  # noqa: E731
    second = lambda actions: None  # noqa: E731

    worker.set_action_dispatcher(first)
    worker.set_action_dispatcher(second)

    assert worker._action_dispatcher is second


# ===========================================================================
# queue_action_update
# ===========================================================================


def test_queue_action_update_is_thread_safe(
    connection_config: ConnectionConfig,
) -> None:
    """Concurrent queue_action_update calls from multiple threads produce no data loss."""
    worker = _make_worker(connection_config)
    updates = [ActionUpdate(external_id=f"act-{i}", status=ActionStatus.succeeded) for i in range(50)]

    threads = [Thread(target=worker.queue_action_update, args=(u,)) for u in updates]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(worker._action_updates) == 50


# ===========================================================================
# Dispatcher mechanics
# ===========================================================================


def test_dispatcher_called_on_separate_daemon_thread(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock_lib.Mocker,
    mock_startup_request: Callable[[requests_mock_lib.Mocker], None],
    mock_checkin_with_actions: Callable,
) -> None:
    """Dispatcher is invoked on a daemon thread named 'ActionDispatcher', not the checkin thread."""
    requests_mock.real_http = True
    mock_startup_request(requests_mock)
    mock_checkin_with_actions(
        requests_mock,
        pending_actions=[{"externalId": "act-1", "actionName": "do-thing", "status": "pending"}],
    )

    worker = _make_worker(connection_config)
    extractor = _make_extractor(connection_config, application_config, worker)
    cancellation_token = CancellationToken()

    received: list[Action] = []
    thread_info: dict = {}
    dispatcher_fired = threading.Event()

    def dispatcher(actions: list[Action]) -> None:
        t = threading.current_thread()
        thread_info["name"] = t.name
        thread_info["daemon"] = t.daemon
        received.extend(actions)
        dispatcher_fired.set()
        cancellation_token.cancel()

    worker.set_action_dispatcher(dispatcher)

    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, extractor._get_startup_request(), 5),
    )
    process.start()
    process.join(timeout=10)
    cancellation_token.cancel()

    assert dispatcher_fired.wait(timeout=2), "Dispatcher was never called"
    assert thread_info["name"] == "ActionDispatcher"
    assert thread_info["daemon"] is True
    assert len(received) == 1
    assert received[0].external_id == "act-1"
    assert received[0].action_name == "do-thing"


def test_dispatcher_not_called_when_no_pending_actions(
    connection_config: ConnectionConfig,
    requests_mock: requests_mock_lib.Mocker,
    mock_checkin_with_actions: Callable,
    checkin_bag: list,
) -> None:
    """Dispatcher is not invoked when the checkin response carries no pending_actions."""
    requests_mock.real_http = True
    mock_checkin_with_actions(requests_mock)  # no pending_actions

    worker = _make_worker(connection_config)
    worker._has_reported_startup = True
    cancellation_token = CancellationToken()

    dispatcher_fired = threading.Event()
    worker.set_action_dispatcher(lambda actions: dispatcher_fired.set())

    worker.flush(cancellation_token)

    assert not dispatcher_fired.wait(timeout=0.1), "Dispatcher was called but should not have been"
    assert len(checkin_bag) == 1


def test_no_error_when_dispatcher_not_set_and_actions_present(
    connection_config: ConnectionConfig,
) -> None:
    """No exception is raised when pending_actions arrive but no dispatcher is registered.

    Extractors that don't use action dispatch must not crash when the server sends actions.
    """
    worker = _make_worker(connection_config)
    assert worker._action_dispatcher is None

    worker._handle_checkin_response(
        {
            "externalId": connection_config.integration.external_id,
            "pendingActions": [{"externalId": "act-1", "actionName": "do-thing", "status": "pending"}],
        }
    )


def test_dispatcher_exception_is_logged_not_silently_swallowed(
    connection_config: ConnectionConfig,
) -> None:
    """An unhandled exception inside the dispatcher is logged, not silently swallowed.

    Regression guard for the Gemini review comment: daemon threads must not hide crashes.
    """
    worker = _make_worker(connection_config)

    class _CapturingHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.records: list[logging.LogRecord] = []
            self.received = threading.Event()

        def emit(self, record: logging.LogRecord) -> None:
            self.records.append(record)
            self.received.set()

    handler = _CapturingHandler()
    worker._logger.addHandler(handler)
    worker._logger.setLevel(logging.ERROR)

    def bad_dispatcher(actions: list) -> None:
        raise RuntimeError("dispatcher crashed")

    worker.set_action_dispatcher(bad_dispatcher)
    worker._handle_checkin_response(
        {
            "externalId": connection_config.integration.external_id,
            "pendingActions": [{"externalId": "act-1", "actionName": "do-thing", "status": "pending"}],
        }
    )

    assert handler.received.wait(timeout=2), "No log record was emitted after dispatcher exception"
    worker._logger.removeHandler(handler)

    assert any("action dispatcher" in r.getMessage().lower() for r in handler.records)


# ===========================================================================
# action_updates in the outgoing checkin body
# ===========================================================================


def test_action_updates_included_in_checkin_request(
    connection_config: ConnectionConfig,
    requests_mock: requests_mock_lib.Mocker,
    mock_checkin_with_actions: Callable,
    checkin_bag: list,
    action_updates_bag: list,
) -> None:
    """Queued action updates appear in the next checkin body and are cleared after success."""
    requests_mock.real_http = True
    mock_checkin_with_actions(requests_mock)

    worker = _make_worker(connection_config)
    worker._has_reported_startup = True
    cancellation_token = CancellationToken()

    worker.queue_action_update(ActionUpdate(external_id="act-1", status=ActionStatus.succeeded))
    worker.flush(cancellation_token)

    assert len(checkin_bag) == 1
    assert "actionUpdates" in checkin_bag[0]
    assert len(action_updates_bag) == 1
    assert action_updates_bag[0]["externalId"] == "act-1"
    assert action_updates_bag[0]["status"] == "succeeded"
    assert worker._action_updates == []


def test_action_updates_absent_when_none_queued(
    connection_config: ConnectionConfig,
    requests_mock: requests_mock_lib.Mocker,
    mock_checkin_with_actions: Callable,
    checkin_bag: list,
) -> None:
    """The actionUpdates field is omitted entirely from the body when nothing is queued."""
    requests_mock.real_http = True
    mock_checkin_with_actions(requests_mock)

    worker = _make_worker(connection_config)
    worker._has_reported_startup = True
    cancellation_token = CancellationToken()

    worker.flush(cancellation_token)

    assert len(checkin_bag) == 1
    assert "actionUpdates" not in checkin_bag[0]


# ===========================================================================
# Failure / requeue
# ===========================================================================


def test_action_updates_requeue_order_preserved_across_retry(
    connection_config: ConnectionConfig,
    requests_mock: requests_mock_lib.Mocker,
    checkin_bag: list,
    action_updates_bag: list,
) -> None:
    """Failed-batch updates are prepended so they precede updates queued after the failure."""
    requests_mock.real_http = True

    call_count = 0

    def json_callback(request: Any, context: Any) -> dict:
        nonlocal call_count
        call_count += 1
        req = json.loads(gzip.decompress(request.body).decode("utf-8"))
        if call_count == 1:
            # First attempt — fail
            context.status_code = 400
            return {"error": {"message": "Request failed", "code": 400}}
        # Second attempt — succeed
        checkin_bag.append(req)
        if "actionUpdates" in req:
            action_updates_bag.extend(req["actionUpdates"])
        return {"externalId": connection_config.integration.external_id}

    requests_mock.register_uri(
        method="POST",
        url=f"{connection_config.base_url}/api/v1/projects/{connection_config.project}/integrations/checkin",
        json=json_callback,
    )

    worker = _make_worker(connection_config)
    worker._has_reported_startup = True
    cancellation_token = CancellationToken()

    worker.queue_action_update(ActionUpdate(external_id="act-a", status=ActionStatus.succeeded))
    worker.queue_action_update(ActionUpdate(external_id="act-b", status=ActionStatus.succeeded))
    worker.flush(cancellation_token)  # first flush — fails, act-a and act-b are requeued

    # new update arrives while retry is pending
    worker.queue_action_update(ActionUpdate(external_id="act-c", status=ActionStatus.succeeded))

    worker.flush(cancellation_token)  # second flush — succeeds

    assert len(action_updates_bag) == 3
    assert [u["externalId"] for u in action_updates_bag] == ["act-a", "act-b", "act-c"]
