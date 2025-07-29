import os
import sys
import time
from argparse import Namespace
from collections.abc import Generator
from multiprocessing import Process
from pathlib import Path
from random import randint
from threading import Thread

import pytest
from _pytest.monkeypatch import MonkeyPatch
from typing_extensions import Self

from cognite.examples.unstable.extractors.simple_extractor.main import SimpleExtractor
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
        url=f"/api/v1/projects/{cognite_client.config.project}/odin/errors",
        params={"integration": connection_config.integration.external_id},
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


def test_runtime_cancellation_propagates_to_extractor(
    monkeypatch: MonkeyPatch, capfd: pytest.CaptureFixture[str]
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

    cfg_dir = Path("cognite/examples/unstable/extractors/simple_extractor/config").resolve()
    assert cfg_dir.exists(), f"Config directory not found: {cfg_dir}"

    conn_cfg = cfg_dir / "connection_config.yaml"
    app_cfg = cfg_dir / "config.yaml"
    assert conn_cfg.exists(), f"Missing connection_config.yaml at {conn_cfg}"
    assert app_cfg.exists(), f"Missing config.yaml at {app_cfg}"

    argv = [
        "simple-extractor",
        "--cwd",
        str(cfg_dir),
        "-c",
        str(conn_cfg.name),
        "-f",
        str(app_cfg.name),
        "--skip-init-checks",
        "-l",
        "info",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    monkeypatch.setenv("PYTHONUNBUFFERED", "1")

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
