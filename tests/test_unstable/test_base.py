import logging

import pytest

from cognite.extractorutils.metrics import PrometheusPusher
from cognite.extractorutils.unstable.configuration.models import (
    ConnectionConfig,
    LogConsoleHandlerConfig,
    LogLevel,
    MetricsConfig,
    TimeIntervalConfig,
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
    "config_level, override_level, expected_logs, unexpected_logs",
    [
        (
            "INFO",
            None,
            ["This is an info message.", "This is a warning message."],
            ["This is a debug message."],
        ),
        (
            "INFO",
            "DEBUG",
            ["This is a debug message.", "This is an info message.", "This is a warning message."],
            [],
        ),
        (
            "INFO",
            "WARNING",
            ["This is a warning message."],
            ["This is a debug message.", "This is an info message."],
        ),
    ],
)
def test_log_level_override(
    capsys: pytest.CaptureFixture[str],
    connection_config: ConnectionConfig,
    config_level: str,
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
        log_handlers=[LogConsoleHandlerConfig(type="console", level=LogLevel(config_level))],
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


def test_extractor_with_metrics(
    connection_config: ConnectionConfig,
    override_level: str | None = None,
) -> None:
    app_config = TestConfig(
        parameter_one=1,
        parameter_two="a",
    )
    metrics_config = MetricsConfig(
        server=_PromServerConfig(
            host="localhost",
            port=9090,
        ),
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

    full_config = FullConfig(
        connection_config=connection_config,
        application_config=app_config,
        current_config_revision=1,
        log_level_override=override_level,
        metrics_config=metrics_config,
    )
    worker = get_checkin_worker(connection_config)
    extractor = TestExtractor(full_config, worker, metrics=TestMetrics)
    assert isinstance(extractor._metrics, TestMetrics) or extractor._metrics == TestMetrics

    # The metrics instance should be a singleton
    another_extractor = TestExtractor(full_config, worker, metrics=TestMetrics)
    assert another_extractor._metrics is extractor._metrics
    assert isinstance(another_extractor._metrics, TestMetrics) or another_extractor._metrics == TestMetrics

    # Patch PrometheusPusher._push_to_server to count calls
    from cognite.extractorutils.unstable.core.metrics import PrometheusPusher

    call_count = {"count": 0}
    original_push = PrometheusPusher._push_to_server

    def counting_push(self: "PrometheusPusher") -> None:
        call_count["count"] += 1
        return original_push(self)

    PrometheusPusher._push_to_server = counting_push

    try:
        with extractor:
            pass
        assert call_count["count"] > 0
    finally:
        PrometheusPusher._push_to_server = original_push
