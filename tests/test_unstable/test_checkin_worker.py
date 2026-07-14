import logging
import pickle
from collections.abc import Callable
from datetime import datetime, timezone
from multiprocessing import Event, Queue, get_context
from threading import Thread
from time import sleep

import faker
import pytest
import requests_mock
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.util import now
from tests.test_unstable.conftest import TestConfig, TestExtractor


def _make_local_cognite_client() -> CogniteClient:
    """
    Build a CogniteClient that never talks to the network, for tests that only exercise pickling.

    Avoids the live-integration `connection_config` fixture, which provisions a real integration
    in a CDF project and therefore requires dev credentials to be set in the environment.
    """
    return CogniteClient(
        ClientConfig(
            client_name="test-checkin-worker-pickling",
            project="test-project",
            base_url="https://api.cognitedata.com",
            credentials=OAuthClientCredentials(
                token_url="https://example.com/token",
                client_id="client-id",
                client_secret="client-secret",
                scopes=["scope"],
            ),
        )
    )


def _put_active_revision_in_queue(worker: CheckinWorker, result_queue: "Queue[int]") -> None:
    """
    Module-level target for the spawn-based multiprocessing test below.

    A spawned child process re-imports this function by qualified name, so it must be a
    top-level, module-scoped callable rather than a closure or a lambda.
    """
    with worker._lock, worker._flush_lock:
        result_queue.put(worker.active_revision)


def test_report_startup_request(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_startup_request: Callable[[requests_mock.Mocker], str],
    checkin_bag: list,
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)

    worker._report_startup(test_extractor._get_startup_request())

    assert len(checkin_bag) == 1
    assert checkin_bag[0]["externalId"] == connection_config.integration.external_id
    assert "extractor" in checkin_bag[0]

    # This is 2 because requests seems to include the authentication request as well
    assert requests_mock.call_count == 2


def test_flush_and_checkin(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    faker: faker.Faker,
    mock_checkin_request: Callable[[requests_mock.Mocker], None],
    checkin_bag: list,
) -> None:
    requests_mock.real_http = True
    mock_checkin_request(requests_mock)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    cancellation_token = CancellationToken()
    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker._has_reported_startup = True
    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    worker.report_task_end("task1", faker.sentence())
    worker.report_task_start("task1", faker.sentence())
    worker.report_error(
        Error(
            level=ErrorLevel.error,
            description=faker.sentence(),
            task_name="task1",
            extractor=test_extractor,
            details=None,
        )
    )
    worker.flush(cancellation_token)
    assert len(checkin_bag) == 1
    assert checkin_bag[0]["externalId"] == connection_config.integration.external_id
    assert len(checkin_bag[0]["errors"]) == 1
    assert len(checkin_bag[0]["taskEvents"]) == 2

    assert requests_mock.call_count == 2


def test_run_report_periodic(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_checkin_request: Callable[[requests_mock.Mocker], None],
    mock_startup_request: Callable[[requests_mock.Mocker], None],
    faker: faker.Faker,
    checkin_bag: list,
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock)
    mock_checkin_request(requests_mock)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    cancellation_token = CancellationToken()
    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)

    worker.report_task_end("task1", faker.sentence())
    worker.report_task_start("task1", faker.sentence())
    worker.report_error(
        Error(
            level=ErrorLevel.error,
            description=faker.sentence(),
            task_name="task1",
            extractor=test_extractor,
            details=None,
        )
    )

    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, test_extractor._get_startup_request(), 5),
    )
    process.start()
    process.join(timeout=10)
    cancellation_token.cancel()

    assert len(checkin_bag) >= 2
    assert "extractor" in checkin_bag[0]
    assert "errors" in checkin_bag[1]
    assert "taskEvents" in checkin_bag[1]


def test_run_report_periodic_reset_startup(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_checkin_request: Callable[[requests_mock.Mocker], None],
    mock_startup_request: Callable[[requests_mock.Mocker], None],
    faker: faker.Faker,
    checkin_bag: list,
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock)
    mock_checkin_request(requests_mock)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    cancellation_token = CancellationToken()
    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )

    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)

    worker.report_task_end("task1", faker.sentence())
    worker.report_task_start("task1", faker.sentence())
    worker.report_error(
        Error(
            level=ErrorLevel.error,
            description=faker.sentence(),
            task_name="task1",
            extractor=test_extractor,
            details=None,
        )
    )

    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, test_extractor._get_startup_request(), 2.0),
    )
    process.start()
    worker.reset_startup()
    process.join(timeout=3)
    cancellation_token.cancel()

    cancellation_token = CancellationToken()
    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, test_extractor._get_startup_request(), 2.0),
    )
    process.start()
    worker.reset_startup()
    process.join(timeout=3)
    cancellation_token.cancel()

    assert len(checkin_bag) >= 3


def test_run_report_periodic_ensure_reorder(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_checkin_request: Callable[[requests_mock.Mocker], None],
    mock_startup_request: Callable[[requests_mock.Mocker], None],
    faker: faker.Faker,
    checkin_bag: list,
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock)
    mock_checkin_request(requests_mock)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    cancellation_token = CancellationToken()
    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)

    behind = now() - 10
    ahead = now()
    first_error = Error(
        level=ErrorLevel.error, description=faker.sentence(), task_name="task1", extractor=test_extractor, details=None
    )
    second_error = Error(
        level=ErrorLevel.warning,
        description=faker.sentence(),
        task_name="task1",
        extractor=test_extractor,
        details=None,
    )

    worker.report_task_end("task1", faker.sentence(), ahead)
    worker.report_task_start("task1", faker.sentence(), behind)
    worker.report_error(second_error)
    worker.report_error(first_error)

    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, test_extractor._get_startup_request(), 5),
    )
    process.start()
    process.join(timeout=10)

    cancellation_token.cancel()

    assert len(checkin_bag) >= 2
    assert "extractor" in checkin_bag[0]
    assert "errors" in checkin_bag[1]
    assert len(checkin_bag[1]["errors"]) == 2
    assert "taskEvents" in checkin_bag[1]
    assert len(checkin_bag[1]["taskEvents"]) == 2

    assert checkin_bag[1]["taskEvents"][0]["timestamp"] < checkin_bag[1]["taskEvents"][1]["timestamp"]

    err0 = checkin_bag[1]["errors"][0]
    err1 = checkin_bag[1]["errors"][1]
    err0_time = err0["endTime"] if "endTime" in err0 else err0["startTime"]
    err1_time = err1["endTime"] if "endTime" in err1 else err1["startTime"]
    assert err0_time <= err1_time, "Errors should be ordered by time, but they are not."


def test_run_report_periodic_checkin(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_checkin_request: Callable[[requests_mock.Mocker], None],
    mock_startup_request: Callable[[requests_mock.Mocker], None],
    checkin_bag: list,
    faker: faker.Faker,
    error_list: list,
    task_events: list,
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock)
    mock_checkin_request(requests_mock)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    cancellation_token = CancellationToken()
    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)

    start_time = now()
    for i in range(1000):
        worker.report_task_start(name="task1", message=faker.sentence(), timestamp=start_time + (i * 1000))
        worker.report_task_end(name="task1", message=faker.sentence(), timestamp=start_time + (i * 1000) + 1000)

    first_error = Error(
        level=ErrorLevel.warning,
        description=faker.sentence(),
        task_name="task1",
        extractor=test_extractor,
        details=None,
    )
    first_error.end_time = start_time + (900 * 1000)
    second_error = Error(
        level=ErrorLevel.error, description=faker.sentence(), task_name="task1", extractor=test_extractor, details=None
    )
    second_error.end_time = start_time + 1000

    worker.report_error(first_error)
    worker.report_error(second_error)

    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, test_extractor._get_startup_request(), 5),
    )
    process.start()

    attempts = 0
    while len(checkin_bag) < 2 and attempts < 10:
        sleep(1)
        attempts += 1

    cancellation_token.cancel()
    process.join(timeout=5)

    assert len(task_events) == 2000
    assert len(error_list) == 2
    print([h.url for h in requests_mock.request_history])
    # initial 2 requests for auth and startup, then 2 for expected number of check-ins
    assert requests_mock.call_count == 2 + 2

    assert error_list[0]["endTime"] == second_error.end_time
    assert error_list[1]["endTime"] == first_error.end_time

    assert error_list[0]["level"] == second_error.level.value
    assert error_list[1]["level"] == first_error.level.value
    assert len(worker._errors) == 0
    assert len(worker._task_updates) == 0


@pytest.mark.parametrize(
    "status_code, status_message",
    [(401, "Unauthorized request"), (403, "Forbidden request")],
    ids=["401-Unauthorized", "403-Forbidden"],
)
def test_on_fatal_hook_is_called(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    status_code: int,
    status_message: str,
    mock_startup_request: Callable[[requests_mock.Mocker, int, str], None],
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock, status_code, status_message)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    on_fatal_count = 0

    def on_fatal_hook(_: Exception) -> None:
        nonlocal on_fatal_count
        on_fatal_count = on_fatal_count + 1
        raise RuntimeError("This is a test fatal hook")

    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.set_on_fatal_error_handler(on_fatal_hook)
    worker.set_retry_startup(True)
    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)
    cancellation_token = CancellationToken()

    process = Thread(
        target=worker._run_startup_report,
        args=(cancellation_token, test_extractor._get_startup_request(), 5),
    )
    process.start()
    process.join(timeout=10)
    cancellation_token.cancel()

    assert on_fatal_count == 1


def test_on_revision_change_hook_is_called(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_startup_request: Callable[[requests_mock.Mocker], None],
    mock_checkin_request: Callable[[requests_mock.Mocker, int], None],
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock)
    mock_checkin_request(requests_mock, 2)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    on_revision_change_value = 0

    def on_revision_change(revision: int) -> None:
        nonlocal on_revision_change_value
        on_revision_change_value = revision
        cancellation_token.cancel()

    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.set_on_revision_change_handler(on_revision_change)
    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)
    cancellation_token = CancellationToken()
    on_revision_change_value = 0

    worker.set_retry_startup(True)
    worker.active_revision = 1

    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, test_extractor._get_startup_request(), 5),
    )
    process.start()
    process.join(timeout=5)
    cancellation_token.cancel()

    assert on_revision_change_value == 2


def test_run_report_periodic_checkin_requeue(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_checkin_request: Callable[..., None],
    mock_startup_request: Callable[[requests_mock.Mocker], None],
    faker: faker.Faker,
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock)
    mock_checkin_request(requests_mock, status_code=400)
    cognite_client = connection_config.get_cognite_client("test_checkin")
    cancellation_token = CancellationToken()
    worker = CheckinWorker(
        cognite_client,
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    test_extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config, application_config=application_config, current_config_revision=1
        ),
        worker,
    )
    test_extractor._start_time = datetime.fromtimestamp(int(now() / 1000), tz=timezone.utc)
    message_queue: Queue = Queue()
    mp_cancel_event = Event()
    test_extractor._attach_runtime_controls(cancel_event=mp_cancel_event, message_queue=message_queue)

    first_error = Error(
        level=ErrorLevel.warning,
        description=faker.sentence(),
        task_name="task1",
        extractor=test_extractor,
        details=None,
    )
    second_error = Error(
        level=ErrorLevel.error, description=faker.sentence(), task_name="task1", extractor=test_extractor, details=None
    )

    worker.report_error(first_error)
    worker.report_error(second_error)

    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, test_extractor._get_startup_request(), 2),
    )
    process.start()
    process.join(timeout=2)
    cancellation_token.cancel()

    # initial 2 requests for auth and startup, then 1 for expected number of check-ins
    assert requests_mock.call_count == 2 + 1
    assert len(worker._errors) == 2


def test_checkin_worker_pickle_round_trip() -> None:
    """
    __getstate__/__setstate__ must drop the un-picklable RLocks and recreate fresh, usable ones,
    while preserving the rest of the worker's state.
    """
    worker = CheckinWorker(_make_local_cognite_client(), "test-integration", logging.getLogger(__name__))
    worker.active_revision = 5
    worker.report_task_start("some-task")

    restored: CheckinWorker = pickle.loads(pickle.dumps(worker))  # noqa: S301 (data is produced in-process, not untrusted)

    assert restored.active_revision == 5
    assert len(restored._task_updates) == 1
    assert restored._lock is not worker._lock
    assert restored._flush_lock is not worker._flush_lock

    # The recreated locks must actually be usable, not left in some broken or shared state.
    with restored._lock, restored._flush_lock:
        pass


def test_checkin_worker_is_picklable_with_spawn_start_method() -> None:
    """
    Runtime._spawn_extractor passes a live CheckinWorker to multiprocessing.Process as a target argument.

    On macOS and Windows, the default start method is "spawn", which pickles every argument passed to
    Process(...). This reproduces that path directly: threading.RLock instances used to make the whole
    worker unpicklable, crashing the extractor with a TypeError before any provider/destination code ran.
    """
    worker = CheckinWorker(_make_local_cognite_client(), "test-integration", logging.getLogger(__name__))
    worker.active_revision = 3

    ctx = get_context("spawn")
    result_queue: Queue[int] = ctx.Queue()
    process = ctx.Process(target=_put_active_revision_in_queue, args=(worker, result_queue))
    process.start()
    try:
        assert result_queue.get(timeout=10) == 3
    finally:
        process.join(timeout=10)

    assert process.exitcode == 0
