import logging
from datetime import datetime, timezone
from threading import Thread
from time import sleep

import pytest

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core._dto import Error as DtoError
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.unstable.core.tasks import ScheduledTask, TaskContext
from cognite.extractorutils.util import now
from test_unstable.conftest import TestConfig, TestExtractor


def test_global_error(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
) -> None:
    worker = CheckinWorker(
        connection_config.get_cognite_client("testing"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.active_revision = 1
    extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config,
            application_config=application_config,
            current_config_revision=1,
        ),
        worker,
    )

    err = extractor.begin_error("Oh no!", details="There was an error")

    assert len(worker._errors) == 1
    assert err.external_id in worker._errors

    wait_time = 50
    sleep(wait_time / 1000)

    err.finish()

    slack = 5

    assert len(worker._errors) == 1
    assert err.start_time + wait_time - slack < err.end_time <= err.start_time + wait_time + slack
    assert worker._errors[err.external_id].end_time == err.end_time
    assert err._task_name is None


def test_instant_error(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
) -> None:
    worker = CheckinWorker(
        connection_config.get_cognite_client("testing"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.active_revision = 1
    extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config,
            application_config=application_config,
            current_config_revision=1,
        ),
        worker,
    )

    err = extractor.begin_error("Oh no!", details="There was an error")

    assert len(worker._errors) == 1
    assert err.external_id in worker._errors

    sleep(0.05)

    err.instant()

    assert len(worker._errors) == 1
    assert err.end_time == err.start_time
    assert worker._errors[err.external_id].end_time == err.end_time
    assert err._task_name is None


def test_task_error(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
) -> None:
    worker = CheckinWorker(
        connection_config.get_cognite_client("testing"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.active_revision = 1
    extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config,
            application_config=application_config,
            current_config_revision=1,
        ),
        worker,
    )
    extractor._start_time = datetime.fromtimestamp(now() / 1000, timezone.utc)

    def task(tc: TaskContext) -> None:
        sleep(0.05)
        tc.warning("Hey now")
        sleep(0.05)

    extractor.add_task(
        ScheduledTask.from_interval(
            interval="15m",
            name="TestTask",
            target=task,
        )
    )

    worker._report_startup(extractor._get_startup_request())
    extractor._scheduler.trigger("TestTask")

    sleep(0.3)

    assert len(worker._task_updates) == 2
    assert len(worker._errors) == 1

    error = next(iter(worker._errors.values()))
    assert error.description == "Hey now"
    assert error.level == ErrorLevel.warning

    # Make sure error was recorded as a task error
    assert error._task_name == "TestTask"


def test_crashing_task(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
) -> None:
    worker = CheckinWorker(
        connection_config.get_cognite_client("testing"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.active_revision = 1
    extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config,
            application_config=application_config,
            current_config_revision=1,
        ),
        worker,
    )

    def task(_tc: TaskContext) -> None:
        sleep(0.05)
        raise ValueError("Try catching this!")

    extractor.add_task(
        ScheduledTask.from_interval(
            interval="15m",
            name="TestTask",
            target=task,
        )
    )
    extractor._start_time = datetime.fromtimestamp(now() / 1000, timezone.utc)

    worker._report_startup(extractor._get_startup_request())
    extractor._scheduler.trigger("TestTask")

    sleep(0.3)

    assert len(worker._task_updates) == 2
    assert len(worker._errors) == 1

    error = next(iter(worker._errors.values()))
    assert error.description == "Task TestTask failed unexpectedly"
    assert error.level == ErrorLevel.fatal

    # Make sure error was recorded as a task error
    assert error._task_name == "TestTask"


@pytest.mark.parametrize("checkin_between", [True, False])
def test_reporting_errors(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    checkin_between: bool,
) -> None:
    worker = CheckinWorker(
        connection_config.get_cognite_client("testing"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.active_revision = 1
    extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config,
            application_config=application_config,
            current_config_revision=1,
        ),
        worker,
    )
    extractor._start_time = datetime.fromtimestamp(now() / 1000, timezone.utc)

    err = extractor.begin_error("Oh no!", details="There was an error")

    assert len(worker._errors) == 1
    assert err.external_id in worker._errors

    if checkin_between:
        cancellation_token = CancellationToken()
        process = Thread(
            target=worker.flush,
            args=(cancellation_token,),
        )
        process.start()
        process.join(timeout=5)

        res = extractor.cognite_client.get(
            f"/api/v1/projects/{extractor.cognite_client.config.project}/integrations/errors?integration={connection_config.integration.external_id}",
            headers={"cdf-version": "alpha"},
        ).json()["items"]
        assert len(res) == 1
        assert res[0]["externalId"] == err.external_id
        assert res[0]["startTime"] == err.start_time
        assert res[0]["description"] == err.description
        assert "endTime" not in res[0]

    sleep(0.05)

    err.finish()

    worker._is_running = False
    worker._has_reported_startup = False
    cancellation_token = CancellationToken()
    process = Thread(
        target=worker.flush,
        args=(cancellation_token,),
    )
    process.start()
    process.join(timeout=5)
    cancellation_token.cancel()

    res = extractor.cognite_client.get(
        f"/api/v1/projects/{extractor.cognite_client.config.project}/integrations/errors?integration={connection_config.integration.external_id}",
        headers={"cdf-version": "alpha"},
    ).json()["items"]
    assert len(res) == 1
    assert res[0]["externalId"] == err.external_id
    assert res[0]["startTime"] == err.start_time
    assert res[0]["endTime"] == err.end_time
    assert res[0]["description"] == err.description


def test_conversion_to_external(connection_config: ConnectionConfig, application_config: TestConfig) -> None:
    worker = CheckinWorker(
        connection_config.get_cognite_client("testing"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.active_revision = 1
    extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config,
            application_config=application_config,
            current_config_revision=1,
        ),
        worker,
    )
    error = Error(
        ErrorLevel.error, "Test error", details="This is a test error", task_name="TestTask", extractor=extractor
    )
    dto_error = DtoError.from_internal(error)

    assert dto_error.external_id == error.external_id
    assert dto_error.level == error.level
    assert dto_error.description == error.description
    assert dto_error.details == error.details
