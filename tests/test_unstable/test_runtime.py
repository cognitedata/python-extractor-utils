import os
import sys
import time
from argparse import Namespace
from collections.abc import Generator
from multiprocessing import Process
from pathlib import Path
from random import randint
from threading import Thread
from unittest.mock import MagicMock, patch

import pytest
import yaml
from _pytest.monkeypatch import MonkeyPatch
from typing_extensions import Self

from cognite.examples.unstable.extractors.simple_extractor.main import SimpleExtractor
from cognite.extractorutils.unstable.configuration.exceptions import InvalidArgumentError
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core.base import ConfigRevision, FullConfig
from cognite.extractorutils.unstable.core.runtime import Runtime
from test_unstable.conftest import TestConfig, TestExtractor


@pytest.fixture
def local_config_file() -> Generator[Path, None, None]:
    file = Path(__file__).parent.parent.parent / f"test-{randint(0, 1000000)}.yaml"
    with open(file, "w") as f:
        f.write("parameter_one: 123\nparameter_two: abc\n")

    yield file

    file.unlink(missing_ok=True)


def test_load_local_config(connection_config: ConnectionConfig, local_config_file: Path) -> None:
    runtime = Runtime(TestExtractor)
    runtime._cognite_client = connection_config.get_cognite_client(
        f"{TestExtractor.EXTERNAL_ID}-{TestExtractor.VERSION}"
    )

    config: TestConfig
    config, revision = runtime._try_get_application_config(
        args=Namespace(force_local_config=[local_config_file]),
        connection_config=connection_config,
    )

    assert revision == "local"
    assert config.parameter_one == 123
    assert config.parameter_two == "abc"


def test_load_cdf_config(connection_config: ConnectionConfig) -> None:
    cognite_client = connection_config.get_cognite_client(f"{TestExtractor.EXTERNAL_ID}-{TestExtractor.VERSION}")
    cognite_client.post(
        url=f"/api/v1/projects/{cognite_client.config.project}/odin/config",
        json={
            "externalId": connection_config.integration.external_id,
            "config": "parameter-one: 123\nparameter-two: abc\n",
        },
        headers={"cdf-version": "alpha"},
    )

    runtime = Runtime(TestExtractor)
    runtime._cognite_client = cognite_client

    config: TestConfig
    config, revision = runtime._try_get_application_config(
        args=Namespace(force_local_config=None),
        connection_config=connection_config,
    )

    assert revision == 1
    assert config.parameter_one == 123
    assert config.parameter_two == "abc"


def test_load_cdf_config_initial_empty(connection_config: ConnectionConfig) -> None:
    """
    Test that the runtime can handle an initial empty config, and that it's picked up when it's set
    """
    cognite_client = connection_config.get_cognite_client(f"{TestExtractor.EXTERNAL_ID}-{TestExtractor.VERSION}")

    runtime = Runtime(TestExtractor)
    runtime._cognite_client = cognite_client
    runtime.RETRY_CONFIG_INTERVAL = 1

    def set_config_after_delay() -> None:
        time.sleep(3)
        cognite_client.post(
            url=f"/api/v1/projects/{cognite_client.config.project}/odin/config",
            json={
                "externalId": connection_config.integration.external_id,
                "config": "parameter-one: 123\nparameter-two: abc\n",
            },
            headers={"cdf-version": "alpha"},
        )

    def cancel_after_delay() -> None:
        time.sleep(10)
        runtime._cancellation_token.cancel()

    Thread(target=set_config_after_delay, daemon=True).start()
    Thread(target=cancel_after_delay, daemon=True).start()

    start_time = time.time()
    result: tuple[TestConfig, ConfigRevision] | None = runtime._safe_get_application_config(
        args=Namespace(force_local_config=None),
        connection_config=connection_config,
    )
    duration = time.time() - start_time

    assert result is not None

    # Duration should not be much higher than sleep before set (3) + retry interval (1)
    assert duration < 5

    config, revision = result
    assert revision == 1
    assert config.parameter_one == 123
    assert config.parameter_two == "abc"

    # There should be one error reported from initially attempting to run without a config
    errors = cognite_client.get(
        url=f"/api/v1/projects/{cognite_client.config.project}/integrations/errors",
        params={"integration": connection_config.integration.external_id},
        headers={"cdf-version": "alpha"},
    ).json()

    assert len(errors["items"]) == 1
    assert "No configuration found for the given integration" in errors["items"][0]["description"]


def test_verify_connection_config(connection_config: ConnectionConfig) -> None:
    runtime = Runtime(TestExtractor)
    assert runtime._verify_connection_config(connection_config)


def test_changing_cwd() -> None:
    runtime = Runtime(TestExtractor)
    original_cwd = os.getcwd()
    runtime._try_set_cwd(args=Namespace(cwd=(Path(__file__).parent.as_posix(),)))

    assert os.getcwd() == str(Path(__file__).parent)
    assert os.getcwd() != original_cwd


def test_change_cwd_to_nonexistent() -> None:
    runtime = Runtime(TestExtractor)

    with pytest.raises(InvalidArgumentError, match="No such file or directory"):
        runtime._try_set_cwd(args=Namespace(cwd=(Path("nonexistent_directory").as_posix(),)))


def _write_conn_from_fixture(base_yaml_path: Path, out_path: Path, cfg: ConnectionConfig) -> None:
    """Start from the repo YAML and overwrite with plain strings from the fixture."""
    data = yaml.safe_load(base_yaml_path.read_text())

    data["project"] = cfg.project
    data["base_url"] = cfg.base_url

    integ = data.setdefault("integration", {})
    integ["external_id"] = cfg.integration.external_id

    auth = cfg.authentication
    scopes_value = getattr(auth.scopes, "value", None) or str(auth.scopes)
    data["authentication"] = {
        "type": "client-credentials",
        "client_id": auth.client_id,
        "client_secret": auth.client_secret,
        "token_url": auth.token_url,
        "scopes": scopes_value,
    }

    out_path.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")


def test_runtime_cancellation_propagates_to_extractor(
    connection_config: ConnectionConfig, tmp_path: Path, monkeypatch: MonkeyPatch, capfd: pytest.CaptureFixture[str]
) -> None:
    """
    Start the runtime, then cancel its token. Verify that:
      1) The child's watcher logs "Cancellation signal received from runtime. Shutting down gracefully."
      2) The child process is not alive after shutdown.
      3) The runtime main loop returns (thread finished).

    This test runs close to how you run the CLI:
      uv run simple-extractor --cwd cognite/examples/unstable/extractors/simple_extractor/config \
         -c connection_config.yaml -f config.yaml --skip-init-checks
    """
    cfg_dir = Path("cognite/examples/unstable/extractors/simple_extractor/config")
    base_conn = cfg_dir / "connection_config.yaml"
    base_app = cfg_dir / "config.yaml"

    conn_file = tmp_path / f"test-{randint(0, 1000000)}-connection_config.yaml"
    _write_conn_from_fixture(base_conn, conn_file, connection_config)

    app_file = tmp_path / f"test-{randint(0, 1000000)}-config.yaml"
    app_file.write_text(base_app.read_text(encoding="utf-8"))

    argv = [
        "simple-extractor",
        "--cwd",
        str(tmp_path),
        "-c",
        conn_file.name,
        "-f",
        app_file.name,
        "--skip-init-checks",
        "-l",
        "info",
    ]

    monkeypatch.setattr(sys, "argv", argv)

    runtime = Runtime(SimpleExtractor)

    child_holder = {}
    original_spawn = Runtime._spawn_extractor

    def spy_spawn(self: Self, config: FullConfig) -> Process:
        p = original_spawn(self, config)
        child_holder["proc"] = p
        return p

    monkeypatch.setattr(Runtime, "_spawn_extractor", spy_spawn, raising=True)

    t = Thread(target=runtime.run, name="RuntimeMain")
    t.start()

    start = time.time()
    while "proc" not in child_holder and time.time() - start < 10:
        time.sleep(0.05)

    assert "proc" in child_holder, "Extractor process was not spawned in time."
    proc = child_holder["proc"]

    time.sleep(0.5)

    runtime._cancellation_token.cancel()

    t.join(timeout=30)
    assert not t.is_alive(), "Runtime did not shut down within timeout after cancellation."

    proc.join(timeout=0)
    assert not proc.is_alive(), "Extractor process is still alive"

    out, err = capfd.readouterr()
    combined = (out or "") + (err or "")
    assert "Cancellation signal received from runtime. Shutting down gracefully." in combined, (
        f"Expected cancellation log line not found in output.\nCaptured output:\n{combined}"
    )


@patch("sys.platform", "win32")
@patch("cognite.extractorutils.unstable.core.runtime.Queue")
@patch("cognite.extractorutils.unstable.core.runtime.WindowsEventHandler")
@patch("logging.getLogger")
def test_logging_on_windows(mock_get_logger: MagicMock, mock_windows_handler: MagicMock, mock_queue: MagicMock) -> None:
    """
    Tests that the logger correctly initializes a console handler
    and a WindowsEventHandler when running on Windows.
    """
    mock_root_logger = MagicMock()
    mock_get_logger.return_value = mock_root_logger
    mock_handler_instance = MagicMock()
    mock_windows_handler.return_value = mock_handler_instance

    runtime = Runtime(TestExtractor)

    mock_windows_handler.assert_called_once_with(TestExtractor.NAME)

    assert mock_root_logger.addHandler.call_count == 2
    mock_root_logger.addHandler.assert_any_call(mock_handler_instance)


@patch("sys.platform", "linux")
@patch("cognite.extractorutils.unstable.core.runtime.WindowsEventHandler")
@patch("logging.getLogger")
def test_logging_on_non_windows(mock_get_logger: MagicMock, mock_windows_handler: MagicMock) -> None:
    """
    Tests that the logger only initializes a console handler
    and skips the WindowsEventHandler when not on Windows.
    """
    mock_root_logger = MagicMock()
    mock_get_logger.return_value = mock_root_logger
    runtime = Runtime(TestExtractor)

    mock_windows_handler.assert_not_called()

    assert mock_root_logger.addHandler.call_count == 1


@patch("sys.platform", "win32")
@patch("cognite.extractorutils.unstable.core.runtime.Queue")
@patch("cognite.extractorutils.unstable.core.runtime.WindowsEventHandler", side_effect=ImportError)
@patch("logging.getLogger")
def test_logging_on_windows_with_import_error(
    mock_get_logger: MagicMock, mock_windows_handler: MagicMock, mock_queue: MagicMock
) -> None:
    """
    Tests that the bootstrap logger handles an ImportError gracefully if pywin32
    is not installed on a Windows system.
    """
    mock_root_logger = MagicMock()
    mock_get_logger.return_value = mock_root_logger
    runtime = Runtime(TestExtractor)

    runtime.logger.warning.assert_called_with(
        "Failed to import the 'pywin32' package. This should install automatically on windows. "
        "Please try reinstalling to resolve this issue."
    )

    assert mock_root_logger.addHandler.call_count == 1
