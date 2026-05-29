"""Tests for CheckinWorker action dispatch.

Two test levels: direct _handle_checkin_response calls (unit) and full
flush → POST → response via requests_mock (integration).
"""

import gzip
import json
import logging
import threading
from collections.abc import Callable, Generator
from datetime import datetime, timezone
from multiprocessing import Event, Queue
from typing import Any

import pytest
import requests_mock

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core._dto import Action, ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import MAX_ACTION_UPDATES_PER_CHECKIN, CheckinWorker
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.util import now
from tests.test_unstable.conftest import TestConfig, TestExtractor


@pytest.fixture
def action_updates_bag() -> Generator[list, None, None]:
    bag: list = []
    yield bag
    bag.clear()


@pytest.fixture
def mock_checkin_with_actions(
    connection_config: ConnectionConfig,
    checkin_bag: list,
    action_updates_bag: list,
) -> Callable[[requests_mock.Mocker, list[dict] | None, int], None]:
    def mocker(
        mock: requests_mock.Mocker,
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


def test_set_action_dispatcher_replaces_previous(
    connection_config: ConnectionConfig,
) -> None:
    """Calling set_action_dispatcher a second time replaces the first: only the second fires."""
    worker = _make_worker(connection_config)
    first_called = threading.Event()
    second_called = threading.Event()

    worker.set_action_dispatcher(lambda actions: first_called.set())
    worker.set_action_dispatcher(lambda actions: second_called.set())

    worker._handle_checkin_response(
        {
            "externalId": connection_config.integration.external_id,
            "pendingActions": [{"externalId": "act-1", "actionName": "do-thing", "status": "pending"}],
        }
    )

    assert second_called.wait(timeout=2), "Second dispatcher was never called"
    assert not first_called.is_set(), "First dispatcher was called but should not have been"


def test_queue_action_update_is_thread_safe(
    connection_config: ConnectionConfig,
) -> None:
    """Concurrent queue_action_update calls from multiple threads produce no data loss."""
    worker = _make_worker(connection_config)
    updates = [ActionUpdate(external_id=f"act-{i}", status=ActionStatus.succeeded) for i in range(50)]

    threads = [threading.Thread(target=worker.queue_action_update, args=(u,)) for u in updates]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(worker._action_updates) == 50
    assert {u.external_id for u in worker._action_updates} == {f"act-{i}" for i in range(50)}


def test_dispatcher_called_on_separate_daemon_thread(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_startup_request: Callable[[requests_mock.Mocker], None],
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

    process = threading.Thread(
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


def test_dispatcher_called_from_startup_response(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
) -> None:
    """Pending actions in a startup response are dispatched, not only checkin responses."""
    requests_mock.real_http = True
    requests_mock.register_uri(
        method="POST",
        url=f"{connection_config.base_url}/api/v1/projects/{connection_config.project}/integrations/startup",
        json={
            "externalId": connection_config.integration.external_id,
            "lastConfigRevision": 1,
            "pendingActions": [{"externalId": "startup-act-1", "actionName": "boot-action", "status": "pending"}],
        },
    )

    worker = _make_worker(connection_config)
    extractor = _make_extractor(connection_config, application_config, worker)

    received: list[Action] = []
    fired = threading.Event()

    def dispatcher(actions: list[Action]) -> None:
        received.extend(actions)
        fired.set()

    worker.set_action_dispatcher(dispatcher)
    worker._report_startup(extractor._get_startup_request())

    assert fired.wait(timeout=2), "Dispatcher was never called from startup response"
    assert len(received) == 1
    assert received[0].external_id == "startup-act-1"
    assert received[0].action_name == "boot-action"


@pytest.mark.parametrize(
    "register_dispatcher,pending_actions",
    [
        # Dispatcher registered but response carries no pending_actions → not called.
        (True, None),
        # Actions present in response but no dispatcher registered → no crash, no side-effects.
        (False, [{"externalId": "act-1", "actionName": "do-thing", "status": "pending"}]),
    ],
    ids=["no_pending_actions", "no_dispatcher_registered"],
)
def test_dispatcher_not_invoked(
    register_dispatcher: bool,
    pending_actions: list[dict] | None,
    connection_config: ConnectionConfig,
) -> None:
    """Dispatcher is not invoked when pending_actions is absent or no dispatcher is registered."""
    worker = _make_worker(connection_config)
    fired = threading.Event()

    if register_dispatcher:
        worker.set_action_dispatcher(lambda actions: fired.set())

    response: dict = {"externalId": connection_config.integration.external_id}
    if pending_actions:
        response["pendingActions"] = pending_actions
    worker._handle_checkin_response(response)

    assert not fired.wait(timeout=0.1), "Dispatcher was called but should not have been"
    assert worker._action_updates == []


def test_dispatcher_exception_is_logged_not_silently_swallowed(
    connection_config: ConnectionConfig,
) -> None:
    """An unhandled exception inside the dispatcher is logged, not silently swallowed."""
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

    def bad_dispatcher(actions: list[Action]) -> None:
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


@pytest.mark.parametrize(
    "queued_updates,expect_updates_key",
    [
        ([], False),
        ([ActionUpdate(external_id="act-1", status=ActionStatus.succeeded)], True),
    ],
    ids=["no_updates_queued", "updates_queued"],
)
def test_action_updates_presence_in_checkin_body(
    queued_updates: list[ActionUpdate],
    expect_updates_key: bool,
    connection_config: ConnectionConfig,
    requests_mock: requests_mock.Mocker,
    mock_checkin_with_actions: Callable,
    checkin_bag: list,
    action_updates_bag: list,
) -> None:
    """actionUpdates present when queued, absent otherwise; queue drained after flush."""
    requests_mock.real_http = True
    mock_checkin_with_actions(requests_mock)

    worker = _make_worker(connection_config)
    worker._has_reported_startup = True
    cancellation_token = CancellationToken()

    for update in queued_updates:
        worker.queue_action_update(update)
    worker.flush(cancellation_token)

    assert len(checkin_bag) == 1
    assert ("actionUpdates" in checkin_bag[0]) is expect_updates_key
    assert worker._action_updates == []

    if expect_updates_key:
        assert len(action_updates_bag) == 1
        assert action_updates_bag[0]["externalId"] == "act-1"
        assert action_updates_bag[0]["status"] == "succeeded"


def test_action_updates_sent_in_pre_startup_path(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_checkin_with_actions: Callable,
    checkin_bag: list,
    action_updates_bag: list,
) -> None:
    """action_updates flow through the pre-startup path; task_updates are suppressed."""
    requests_mock.real_http = True
    mock_checkin_with_actions(requests_mock)

    worker = _make_worker(connection_config)
    extractor = _make_extractor(connection_config, application_config, worker)
    cancellation_token = CancellationToken()

    # pre-startup path only fires when at least one non-task error exists
    Error(level=ErrorLevel.warning, description="pre-startup error", details=None, task_name=None, extractor=extractor)

    worker.report_task_start("some-task")
    worker.queue_action_update(ActionUpdate(external_id="act-pre", status=ActionStatus.succeeded))
    worker.flush(cancellation_token)

    assert len(checkin_bag) == 1
    assert "actionUpdates" in checkin_bag[0]
    assert action_updates_bag[0]["externalId"] == "act-pre"
    assert "taskEvents" not in checkin_bag[0]


def test_action_updates_capped_at_100_per_checkin(
    connection_config: ConnectionConfig,
    requests_mock: requests_mock.Mocker,
    mock_checkin_with_actions: Callable,
    checkin_bag: list,
    action_updates_bag: list,
) -> None:
    """At most MAX_ACTION_UPDATES_PER_CHECKIN updates go out per flush; excess carries to the next."""
    requests_mock.real_http = True
    mock_checkin_with_actions(requests_mock)

    worker = _make_worker(connection_config)
    worker._has_reported_startup = True
    cancellation_token = CancellationToken()

    overflow = 10
    total = MAX_ACTION_UPDATES_PER_CHECKIN + overflow
    for i in range(total):
        worker.queue_action_update(ActionUpdate(external_id=f"act-{i}", status=ActionStatus.succeeded))

    worker.flush(cancellation_token)  # first flush: sends exactly 100

    assert len(action_updates_bag) == MAX_ACTION_UPDATES_PER_CHECKIN
    assert len(worker._action_updates) == overflow

    worker.flush(cancellation_token)  # second flush: sends remaining 10

    assert len(action_updates_bag) == total


def test_action_updates_requeue_order_preserved_across_retry(
    connection_config: ConnectionConfig,
    requests_mock: requests_mock.Mocker,
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
