import contextlib
import json
import logging
import random
from collections.abc import Generator
from io import StringIO
from pathlib import Path

import pytest

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteNotFoundError
from cognite.extractorutils.metrics import CognitePusher, PrometheusPusher
from cognite.extractorutils.statestore.watermark import LocalStateStore, RawStateStore
from cognite.extractorutils.unstable.configuration.loaders import ConfigFormat, load_io
from cognite.extractorutils.unstable.configuration.models import (
    ConnectionConfig,
    ExtractorConfig,
    LocalStateStoreConfig,
    LogConsoleHandlerConfig,
    LogFileHandlerConfig,
    LogHandlerConfig,
    LogLevel,
    MetricsConfig,
    RawStateStoreConfig,
    StateStoreConfig,
    TimeIntervalConfig,
    _CogniteMetricsConfig,
    _PromServerConfig,
    _PushGatewayConfig,
)
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.tasks import TaskContext

from .conftest import TestConfig, TestExtractor, TestMetrics


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


@pytest.mark.parametrize("metrics_type", ["prometheus", "cognite"])
def test_extractor_with_metrics_pushers(connection_config: ConnectionConfig, metrics_type: str) -> None:
    override_level = "INFO"
    call_count = {"count": 0}

    if metrics_type == "prometheus":
        metrics_config = MetricsConfig(
            server=_PromServerConfig(host="localhost", port=9090),
            cognite=None,
            push_gateways=[
                _PushGatewayConfig(
                    host="localhost",
                    job_name="test-job",
                    username=None,
                    password=None,
                    clear_after=None,
                    push_interval=TimeIntervalConfig("30s"),
                )
            ],
        )
        pusher_cls = PrometheusPusher

        def counting_push(self: PrometheusPusher) -> None:
            call_count["count"] += 1
            return original_push(self)

        original_push = pusher_cls._push_to_server
    else:
        metrics_config = MetricsConfig(
            server=None,
            cognite=_CogniteMetricsConfig(
                external_id_prefix="extractor_test",
                asset_name="Extractor Test Metrics",
                asset_external_id="extractor_testcognite_assets",
                data_set=None,
            ),
            push_gateways=None,
        )
        pusher_cls = CognitePusher

        def counting_push(self: CognitePusher) -> None:
            call_count["count"] += 1
            return None

        original_push = pusher_cls._push_to_server

    app_config = TestConfig(
        parameter_one=1,
        parameter_two="a",
        metrics=metrics_config,
    )

    full_config = FullConfig(
        connection_config=connection_config,
        application_config=app_config,
        current_config_revision=1,
        log_level_override=override_level,
    )
    worker = get_checkin_worker(connection_config)
    extractor = TestExtractor(full_config, worker, metrics=TestMetrics)
    assert isinstance(extractor._metrics, TestMetrics) or extractor._metrics == TestMetrics

    with contextlib.ExitStack() as stack:
        stack.enter_context(contextlib.suppress(Exception))
        pusher_cls._push_to_server = counting_push
        try:
            with extractor:
                for pusher in extractor.metrics_push_manager.pushers:
                    assert pusher.thread is not None
                    assert pusher.thread.is_alive()
        finally:
            pusher_cls._push_to_server = original_push
    assert call_count["count"] > 0


def test_pushgatewayconfig_none_credentials_from_yaml() -> None:
    config_str = """
push-gateways:
  - host: "http://localhost:9091"
    job_name: "test-job"
"""

    stream = StringIO(config_str)
    config = load_io(stream, ConfigFormat.YAML, MetricsConfig)
    metrics_config = config.push_gateways[0]
    pusher = PrometheusPusher(
        job_name=metrics_config.job_name,
        username=metrics_config.username,
        password=metrics_config.password,
        url=metrics_config.host,
        push_interval=30,
        thread_name="TestPusher",
        cancellation_token=None,
    )
    assert pusher.username is None
    assert pusher.password is None
    assert pusher.url == "http://localhost:9091"
    assert pusher.job_name == "test-job"


def test_extractor_config_upload_queue_size_with_yaml() -> None:
    """Test upload_queue_size parsing from YAML configuration."""
    config_yaml = """
upload-queue-size: 200000
retry-startup: false
"""
    stream = StringIO(config_yaml)
    config = load_io(stream, ConfigFormat.YAML, ExtractorConfig)

    assert config.upload_queue_size == 200_000
    assert config.retry_startup is False
