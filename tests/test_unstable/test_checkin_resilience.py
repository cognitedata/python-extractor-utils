"""
Tests for check-in resilience: startup failures are fatal and logged, transient network errors are not fatal.

These tests do not need a CDF project: the Cognite client is a mock whose ``post`` is controlled by each test.
"""

import logging
from collections.abc import Callable
from datetime import datetime, timezone
from threading import Event, Thread
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest
import requests
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError, CogniteConnectionError
from oauthlib.oauth2 import OAuth2Error

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.core._dto import ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.tasks import ScheduledTask

from .conftest import TestConfig, TestExtractor

MAX_AVAILABLE_ACTIONS = 100


def _make_worker(post: Callable[..., Any] | None = None) -> tuple[CheckinWorker, MagicMock]:
    client = MagicMock()
    client.config.project = "test-project"
    if post is not None:
        client.post.side_effect = post
    worker = CheckinWorker(client, "test-integration", logging.getLogger("test.checkin_resilience"))
    worker.set_retry_startup(True)
    return worker, client


def _make_extractor(worker: CheckinWorker, n_tasks: int = 0) -> TestExtractor:
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
    for i in range(n_tasks):
        extractor.add_task(ScheduledTask.from_interval(interval="1h", name=f"task-{i}", target=lambda _: None))
    return extractor


def _n_builtin_actions() -> int:
    worker, _ = _make_worker()
    request = _make_extractor(worker)._get_startup_request()
    return len(request.available_actions or [])


def _ok_response() -> MagicMock:
    response = MagicMock()
    response.json.return_value = {"externalId": "test-integration"}
    return response


def _raiser(error: BaseException) -> Callable[..., Any]:
    def post(*args: Any, **kwargs: Any) -> Any:
        raise error

    return post


def _oauth_auth_error() -> CogniteAuthError:
    error = CogniteAuthError("Error generating access token: invalid_client")
    error.__cause__ = OAuth2Error("invalid_client")
    return error


def _timeout_auth_error() -> CogniteAuthError:
    error = CogniteAuthError("Error generating access token")
    error.__cause__ = requests.exceptions.ReadTimeout("token endpoint timed out")
    return error


# ---------------------------------------------------------------------------
# Startup request failures
# ---------------------------------------------------------------------------


def test_too_many_available_actions_is_logged_and_fatal(caplog: pytest.LogCaptureFixture) -> None:
    """More than 100 available actions fails StartupRequest validation; this must not kill the thread silently."""
    worker, client = _make_worker(lambda *a, **k: _ok_response())
    fatal_errors: list[Exception] = []
    worker.set_on_fatal_error_handler(fatal_errors.append)

    n_tasks = (MAX_AVAILABLE_ACTIONS - _n_builtin_actions()) // 2 + 1  # one task over the limit
    extractor = _make_extractor(worker, n_tasks=n_tasks)

    with caplog.at_level(logging.WARNING):
        extractor._run_checkin()  # runs synchronously; would raise (thread dies) without the fix

    assert len(fatal_errors) == 1
    assert extractor.cancellation_token.is_cancelled
    client.post.assert_not_called()
    critical = [r for r in caplog.records if r.levelno >= logging.CRITICAL]
    assert critical, "failure must be logged at CRITICAL so it reaches the log file"
    assert critical[0].exc_info is not None


def test_exactly_100_available_actions_is_fine() -> None:
    worker, _ = _make_worker()
    n_tasks = (MAX_AVAILABLE_ACTIONS - _n_builtin_actions()) // 2
    extractor = _make_extractor(worker, n_tasks=n_tasks)
    request = extractor._get_startup_request()
    assert len(request.available_actions or []) <= MAX_AVAILABLE_ACTIONS


def test_startup_not_retried_failure_is_fatal(caplog: pytest.LogCaptureFixture) -> None:
    """A startup failure when retries are disabled used to kill the check-in thread with no log."""
    worker, _ = _make_worker(_raiser(CogniteConnectionError()))
    worker.set_retry_startup(False)
    fatal_errors: list[Exception] = []
    worker.set_on_fatal_error_handler(fatal_errors.append)
    extractor = _make_extractor(worker)

    with caplog.at_level(logging.WARNING):
        extractor._run_checkin()

    assert len(fatal_errors) == 1
    assert any(r.levelno >= logging.CRITICAL for r in caplog.records)


# ---------------------------------------------------------------------------
# Transient errors are not fatal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(requests.exceptions.ReadTimeout("read timed out"), id="requests-read-timeout"),
        pytest.param(requests.exceptions.ConnectTimeout("connect timed out"), id="requests-connect-timeout"),
        pytest.param(requests.exceptions.ConnectionError("reset"), id="requests-connection-error"),
        pytest.param(httpx.ReadTimeout("read timed out"), id="httpx-read-timeout"),
        pytest.param(httpx.ConnectTimeout("connect timed out"), id="httpx-connect-timeout"),
        pytest.param(httpx.ConnectError("refused"), id="httpx-connect-error"),
        pytest.param(TimeoutError("timed out"), id="builtin-timeout"),
        pytest.param(CogniteConnectionError(), id="sdk-connection-error"),
        pytest.param(CogniteAPIError("Bad gateway", code=502), id="http-502"),
        pytest.param(CogniteAPIError("Unavailable", code=503), id="http-503"),
        pytest.param(_timeout_auth_error(), id="token-refresh-timeout-wrapped"),
    ],
)
def test_transient_error_is_requeued_and_not_fatal(error: BaseException) -> None:
    worker, _ = _make_worker(_raiser(error))
    on_fatal = MagicMock()
    worker.set_on_fatal_error_handler(on_fatal)
    update = ActionUpdate(external_id="act-1", status=ActionStatus.running)
    worker.queue_action_update(update)

    worker.try_write_checkin([], [])

    on_fatal.assert_not_called()
    assert worker._action_updates == [update], "the update must be requeued for the next check-in"


def test_transient_error_during_startup_is_not_fatal_and_is_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = 0
    token = CancellationToken()

    def post(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise requests.exceptions.ReadTimeout("token refresh timed out")
        return _ok_response()

    worker, _ = _make_worker(post)
    on_fatal = MagicMock()
    worker.set_on_fatal_error_handler(on_fatal)
    monkeypatch.setattr("cognite.extractorutils.unstable.core.checkin_worker.sleep", lambda _: None)
    extractor = _make_extractor(worker)

    worker._run_startup_report(token, extractor._get_startup_request(), interval=0.01)

    assert calls == 2
    assert worker._has_reported_startup
    on_fatal.assert_not_called()


# ---------------------------------------------------------------------------
# Auth/config errors stay fatal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(CogniteAPIError("Unauthorized", code=401), id="401"),
        pytest.param(CogniteAPIError("Forbidden", code=403), id="403"),
        pytest.param(_oauth_auth_error(), id="bad-credentials"),
        pytest.param(CogniteAuthError("Unable to determine device authorization endpoint"), id="auth-error-no-cause"),
        pytest.param(ValueError("unexpected"), id="unexpected-error"),
        pytest.param(requests.exceptions.InvalidURL("bad url"), id="invalid-url-is-a-config-error"),
        pytest.param(requests.exceptions.MissingSchema("no scheme"), id="missing-schema-is-a-config-error"),
        pytest.param(httpx.UnsupportedProtocol("bad protocol"), id="unsupported-protocol-is-a-config-error"),
    ],
)
def test_auth_and_config_errors_remain_fatal(error: BaseException) -> None:
    worker, _ = _make_worker(_raiser(error))
    on_fatal = MagicMock()
    worker.set_on_fatal_error_handler(on_fatal)

    worker.try_write_checkin([], [])

    on_fatal.assert_called_once()
    assert on_fatal.call_args[0][0] is error


def test_periodic_checkin_keeps_running_after_read_timeout() -> None:
    """End to end: a timeout in a periodic check-in does not stop the loop, and it recovers on the next cycle."""
    calls = 0
    token = CancellationToken()
    recovered = Event()

    def post(*args: Any, **kwargs: Any) -> Any:
        nonlocal calls
        calls += 1
        if calls == 2:  # first periodic check-in after startup
            raise httpx.ReadTimeout("timed out")
        if calls >= 3:
            recovered.set()
        return _ok_response()

    worker, _ = _make_worker(post)
    on_fatal = MagicMock()
    worker.set_on_fatal_error_handler(on_fatal)
    extractor = _make_extractor(worker)

    thread = Thread(
        target=worker.run_periodic_checkin, args=(token, extractor._get_startup_request(), 0.05), daemon=True
    )
    thread.start()
    assert recovered.wait(timeout=10)
    token.cancel()
    thread.join(timeout=10)

    on_fatal.assert_not_called()
    assert calls >= 3


# ---------------------------------------------------------------------------
# Exit code when the extractor stops because of a fatal error
# ---------------------------------------------------------------------------


class _FakeExtractor:
    """Stands in for an Extractor in the child process entrypoint; ``run`` optionally hits a fatal check-in error."""

    EXTERNAL_ID = "fake"
    RETRY_STARTUP = True
    raise_fatal = False

    def __init__(self, worker: CheckinWorker) -> None:
        self._worker = worker

    @classmethod
    def _init_from_runtime(cls, config: Any, checkin_worker: CheckinWorker) -> "_FakeExtractor":
        return cls(checkin_worker)

    def _attach_runtime_controls(self, **_: Any) -> None:
        pass

    def __enter__(self) -> "_FakeExtractor":
        return self

    def __exit__(self, *_: Any) -> bool:
        return False

    def run(self) -> None:
        if self.raise_fatal:
            self._worker.notify_fatal_error(RuntimeError("bad credentials"))


@pytest.mark.parametrize("fatal", [True, False])
def test_extractor_process_exit_code_reflects_fatal_error(fatal: bool) -> None:
    from cognite.extractorutils.unstable.core.runtime import (
        FATAL_ERROR_EXIT_CODE,
        _extractor_process_entrypoint,
        _RuntimeControls,
    )

    class Extractor(_FakeExtractor):
        raise_fatal = fatal

    controls = _RuntimeControls(cancel_event=MagicMock(), message_queue=MagicMock())
    config = MagicMock()
    config.current_config_revision = 1
    worker, _ = _make_worker()

    if fatal:
        with pytest.raises(SystemExit) as exc_info:
            _extractor_process_entrypoint(Extractor, controls, config, worker)  # type: ignore[arg-type]
        assert exc_info.value.code == FATAL_ERROR_EXIT_CODE
        controls.cancel_event.set.assert_called_once()
    else:
        _extractor_process_entrypoint(Extractor, controls, config, worker)  # type: ignore[arg-type]
