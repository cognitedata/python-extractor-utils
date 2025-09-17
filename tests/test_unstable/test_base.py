import contextlib
import json
import logging
import random
from collections.abc import Generator
from pathlib import Path

import pytest

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteNotFoundError
from cognite.extractorutils.statestore.watermark import LocalStateStore, RawStateStore
from cognite.extractorutils.unstable.configuration.models import (
    ConnectionConfig,
    LocalStateStoreConfig,
    LogConsoleHandlerConfig,
    LogFileHandlerConfig,
    LogHandlerConfig,
    LogLevel,
    RawStateStoreConfig,
    StateStoreConfig,
)
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.tasks import TaskContext

from .conftest import TestConfig, TestExtractor


def get_checkin_worker(connection_config: ConnectionConfig) -> CheckinWorker:
    worker = CheckinWorker(
        connection_config.get_cognite_client("testing"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
    )
    worker.active_revision = 1
    return worker


@pytest.mark.parametrize(
    "log_handlers, override_level, expected_logs, unexpected_logs",
    [
        (
            [LogConsoleHandlerConfig(type="console", level=LogLevel("INFO"))],
            None,
            ["This is an info message.", "This is a warning message."],
            ["This is a debug message."],
        ),
        (
            [LogConsoleHandlerConfig(type="console", level=LogLevel("INFO"))],
            "DEBUG",
            ["This is a debug message.", "This is an info message.", "This is a warning message."],
            [],
        ),
        (
            [LogConsoleHandlerConfig(type="console", level=LogLevel("INFO"))],
            "WARNING",
            ["This is a warning message."],
            ["This is a debug message.", "This is an info message."],
        ),
        (
            [LogFileHandlerConfig(type="file", level=LogLevel("INFO"), path=Path("non-existing/test.log"))],
            "WARNING",
            ["This is a warning message."],
            ["Falling back to console logging.", "This is a debug message.", "This is an info message."],
        ),
    ],
)
def test_log_level_override(
    capsys: pytest.CaptureFixture[str],
    connection_config: ConnectionConfig,
    log_handlers: list[LogHandlerConfig],
    override_level: str | None,
    expected_logs: list[str],
    unexpected_logs: list[str],
) -> None:
    """
    Tests that the log level override parameter correctly overrides the log level
    set in the application configuration.
    """
    app_config = TestConfig(
        parameter_one=1,
        parameter_two="a",
        log_handlers=log_handlers,
    )

    full_config = FullConfig(
        connection_config=connection_config,
        application_config=app_config,
        current_config_revision=1,
        log_level_override=override_level,
    )
    worker = get_checkin_worker(connection_config)
    extractor = TestExtractor(full_config, worker)

    with extractor:
        startup_task = next(t for t in extractor._tasks if t.name == "log_task")
        task_context = TaskContext(task=startup_task, extractor=extractor)
        startup_task.target(task_context)

    captured = capsys.readouterr()
    console_output = captured.err

    for log in expected_logs:
        assert log in console_output
    for log in unexpected_logs:
        assert log not in console_output


def test_get_current_statestore_raises_before_start() -> None:
    """
    Tests that calling get_current_statestore before the extractor's
    __enter__ method is called raises a ValueError.
    """
    with pytest.raises(ValueError, match="No state store singleton created. Have a state store been loaded?"):
        TestExtractor.get_current_statestore()


@pytest.fixture
def local_state_file(tmp_path: Path) -> Path:
    """
    Provides a path to a temporary file for a single test function run.
    The file and its parent directory are automatically cleaned up by pytest.
    """
    return tmp_path / "test_states.json"


def test_local_state_store_integration(local_state_file: Path, connection_config: ConnectionConfig) -> None:
    """
    Tests the integration of LocalStateStore with the extractor configuration.
    """
    app_config = TestConfig(
        parameter_one=1,
        parameter_two="a",
        log_handlers=[LogConsoleHandlerConfig(type="console", level=LogLevel("INFO"))],
        state_store=StateStoreConfig(local=LocalStateStoreConfig(path=local_state_file)),
    )

    full_config = FullConfig(
        connection_config=connection_config,
        application_config=app_config,
        current_config_revision=1,
    )

    worker = get_checkin_worker(connection_config)
    extractor = TestExtractor(full_config, worker)

    with pytest.raises(ValueError):
        TestExtractor.get_current_statestore()

    with extractor:
        state_store = TestExtractor.get_current_statestore()

        assert isinstance(state_store, LocalStateStore)
        assert state_store is extractor.state_store

        assert not local_state_file.exists()
        assert state_store.get_state("my-test-id") == (None, None)

        state_store.set_state(external_id="my-test-id", low=1, high=5)
        state_store.synchronize()

        assert local_state_file.exists()
        with open(local_state_file) as f:
            data = json.load(f)
            assert data["my-test-id"] == {"low": 1, "high": 5}

    new_extractor = TestExtractor(full_config, worker)
    with new_extractor:
        assert new_extractor.state_store.get_state("my-test-id") == (1, 5)


@pytest.fixture(scope="function")
def raw_db_table_name() -> tuple[str, str]:
    """Provides a unique database name for a single test function run."""
    test_id = random.randint(0, int(1e9))
    return f"test_db_{test_id}", f"test_table_{test_id}"


@pytest.fixture
def setup_and_teardown_raw_db(
    set_client: CogniteClient, raw_db_table_name: str
) -> Generator[tuple[str, str], None, None]:
    """
    This fixture ensures the RAW database/table is cleaned up after the test.
    """
    db_name, table_name = raw_db_table_name

    yield db_name, table_name

    with contextlib.suppress(CogniteNotFoundError):
        set_client.raw.databases.delete(name=db_name, recursive=True)


def test_raw_state_store_integration(
    connection_config: ConnectionConfig,
    setup_and_teardown_raw_db: tuple[str, str],
) -> None:
    """
    Tests the integration of LocalStateStore with the extractor configuration.
    """
    db_name, table_name = setup_and_teardown_raw_db

    app_config = TestConfig(
        parameter_one=1,
        parameter_two="a",
        log_handlers=[LogConsoleHandlerConfig(type="console", level=LogLevel("INFO"))],
        state_store=StateStoreConfig(raw=RawStateStoreConfig(database=db_name, table=table_name)),
    )

    full_config = FullConfig(
        connection_config=connection_config,
        application_config=app_config,
        current_config_revision=1,
    )

    worker = get_checkin_worker(connection_config)
    extractor = TestExtractor(full_config, worker)

    with pytest.raises(ValueError):
        TestExtractor.get_current_statestore()

    with extractor:
        state_store = TestExtractor.get_current_statestore()

        assert isinstance(state_store, RawStateStore)
        assert state_store is extractor.state_store

        assert state_store.get_state("my-test-id") == (None, None)

        state_store.set_state(external_id="my-test-id", low=1, high=5)
        state_store.synchronize()

        assert state_store.get_state("my-test-id") == (1, 5)

    new_extractor = TestExtractor(full_config, worker)
    with new_extractor:
        assert new_extractor.state_store.get_state("my-test-id") == (1, 5)
