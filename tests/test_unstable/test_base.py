import logging
from unittest.mock import MagicMock, patch

import pytest

from cognite.extractorutils.unstable.configuration.models import (
    ConnectionConfig,
    ExtractorConfig,
    LogConsoleHandlerConfig,
    LogHandlerConfig,
    LogLevel,
    LogWindowsEventHandlerConfig,
)
from cognite.extractorutils.unstable.core.base import Extractor, FullConfig
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.tasks import TaskContext

from .conftest import TestConfig, TestExtractor


def get_checkin_worker(connection_config: ConnectionConfig) -> CheckinWorker:
    return CheckinWorker(
        connection_config.get_cognite_client("testing"),
        connection_config.integration.external_id,
        logging.getLogger(__name__),
        lambda _: None,
        lambda _: None,
        1,
        False,
    )


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


class MockExtractorConfig(ExtractorConfig):
    def __init__(self, handlers: list[LogHandlerConfig]) -> None:
        super().__init__(log_handlers=handlers)


def build_test_extractor(config: ExtractorConfig, connection_config: ConnectionConfig = None) -> Extractor:
    if connection_config is None:
        connection_config = MagicMock()

    class TestExtractor(Extractor):
        NAME = "Test Extractor"
        EXTERNAL_ID = "test-extractor"
        DESCRIPTION = "Test description"
        VERSION = "1.0.0"
        CONFIG_TYPE = type(config)

    full_config = FullConfig(
        connection_config=connection_config,
        application_config=config,
        current_config_revision="123",
    )

    worker = get_checkin_worker(connection_config)

    return TestExtractor(full_config, worker)


@patch("sys.platform", "win32")
@patch("logging.getLogger")
@patch("cognite.extractorutils.unstable.core.base.WindowsEventHandler")
def test_windows_handler_on_windows(mock_nt_handler: MagicMock, mock_get_logger: MagicMock) -> None:
    """Test Windows Event Log handler on Windows platform"""
    mock_root_logger = MagicMock()
    mock_get_logger.return_value = mock_root_logger
    mock_handler_instance = MagicMock()
    mock_nt_handler.return_value = mock_handler_instance

    windows_handler_config = LogWindowsEventHandlerConfig(type="windows-event-log", level=LogLevel.INFO)
    config = MockExtractorConfig([windows_handler_config])

    extractor = build_test_extractor(config)

    extractor._setup_logging()

    mock_nt_handler.assert_called_once_with("Test Extractor")
    mock_root_logger.addHandler.assert_called_with(mock_handler_instance)


@patch("sys.platform", "linux")
@patch("logging.getLogger")
def test_windows_handler_on_non_windows(mock_get_logger: MagicMock) -> None:
    """Test Windows Event Log handler on non-Windows platform"""
    mock_root_logger = MagicMock()
    mock_get_logger.return_value = mock_root_logger

    windows_handler_config = LogWindowsEventHandlerConfig(type="windows-event-log", level=LogLevel.INFO)
    config = MockExtractorConfig([windows_handler_config])

    extractor = build_test_extractor(config)

    extractor._setup_logging()

    mock_root_logger.warning.assert_called_with("Windows Event Log handler is only available on Windows.")
