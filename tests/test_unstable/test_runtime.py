import os
import time
from argparse import Namespace
from collections.abc import Generator
from pathlib import Path
from random import randint
from threading import Thread

import pytest

from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core.base import ConfigRevision
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


def test_changing_cwd() -> None:
    runtime = Runtime(TestExtractor)
    original_cwd = os.getcwd()
    runtime._try_set_cwd(args=Namespace(cwd=(Path(__file__).parent.as_posix(),)))

    assert os.getcwd() == str(Path(__file__).parent)
    assert os.getcwd() != original_cwd
