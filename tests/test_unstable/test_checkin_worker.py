import logging
from collections.abc import Callable
from datetime import datetime, timezone
from multiprocessing import Event, Queue
from threading import Thread
from time import sleep

import faker
import requests_mock

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.util import now
from tests.test_unstable.conftest import TestConfig, TestExtractor


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
        lambda _: None,
        lambda _: None,
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
        lambda _: None,
        lambda _: None,
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
        lambda _: None,
        lambda _: None,
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
        lambda _: None,
        lambda _: None,
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
        lambda _: None,
        lambda _: None,
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
        continue

    cancellation_token.cancel()

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


def test_on_fatal_hook_is_called(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    requests_mock: requests_mock.Mocker,
    mock_startup_request: Callable[[requests_mock.Mocker, int, str], None],
) -> None:
    requests_mock.real_http = True
    mock_startup_request(requests_mock, 401, "Unauthorized request")
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
        lambda _: None,
        on_fatal_hook,
    )
    worker._retry_startup = True
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
        on_revision_change,
        lambda _: None,
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
    cancellation_token = CancellationToken()
    on_revision_change_value = 0

    worker.should_retry_startup()
    worker.active_revision = 1

    process = Thread(
        target=worker.run_periodic_checkin,
        args=(cancellation_token, test_extractor._get_startup_request(), 5),
    )
    process.start()
    process.join(timeout=5)

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
        lambda _: None,
        lambda _: None,
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
        args=(cancellation_token, test_extractor._get_startup_request(), 20),
    )
    process.start()
    process.join(timeout=2)
    cancellation_token.cancel()

    # initial 2 requests for auth and startup, then 1 for expected number of check-ins
    assert requests_mock.call_count == 2 + 1
    assert len(worker._errors) == 2
