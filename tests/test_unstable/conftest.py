import os
from collections.abc import Generator
from threading import RLock
from time import sleep, time
from uuid import uuid4

import pytest

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.extractorutils.unstable.configuration.models import (
    ConnectionConfig,
    ExtractorConfig,
    IntegrationConfig,
    Scopes,
    _ClientCredentialsConfig,
)
from cognite.extractorutils.unstable.core.base import Extractor, StartupTask, TaskContext

working_dir = os.getcwd()


@pytest.fixture(autouse=True)
def reset_environment() -> Generator[None, None, None]:
    yield
    os.chdir(working_dir)


@pytest.fixture
def set_client() -> CogniteClient:
    cognite_project = os.environ["COGNITE_DEV_PROJECT"]
    cognite_base_url = os.environ["COGNITE_DEV_BASE_URL"]
    cognite_token_url = os.environ.get("COGNITE_DEV_TOKEN_URL", os.environ["COGNITE_TOKEN_URL"])
    cognite_client_id = os.environ.get("COGNITE_DEV_CLIENT_ID", os.environ["COGNITE_CLIENT_ID"])
    cognite_client_secret = os.environ.get("COGNITE_DEV_CLIENT_SECRET", os.environ["COGNITE_CLIENT_SECRET"])
    cognite_project_scopes = os.environ["COGNITE_DEV_TOKEN_SCOPES"].split(",")
    client_config = ClientConfig(
        project=cognite_project,
        base_url=cognite_base_url,
        credentials=OAuthClientCredentials(
            cognite_token_url, cognite_client_id, cognite_client_secret, cognite_project_scopes
        ),
        client_name="extractor-utils-integration-tests",
    )
    return CogniteClient(client_config)


class MockFunction:
    def __init__(self, sleep_time: int) -> None:
        self.called_times: list[float] = []
        self.sleep_time = sleep_time
        self.lock = RLock()

    def __call__(self) -> None:
        with self.lock:
            self.called_times.append(time())
        sleep(self.sleep_time)


@pytest.fixture
def extraction_pipeline(set_client: CogniteClient) -> Generator[str, None, None]:
    external_id = f"utils-test-{uuid4().hex}"
    response = set_client.post(
        url=f"/api/v1/projects/{set_client.config.project}/odin",
        json={
            "items": [
                {"externalId": external_id, "extractor": {"externalId": "test-extractor"}},
            ]
        },
        headers={"cdf-version": "alpha"},
    )

    print(f"Created extraction pipeline with external ID: {external_id}")
    print(f"Response from odin: {response.json()}")
    assert response.status_code == 201, f"Failed to create extraction pipeline: {response.text}"

    yield external_id

    set_client.post(
        url=f"/api/v1/projects/{set_client.config.project}/odin/delete",
        json={"items": [{"externalId": external_id}]},
        headers={"cdf-version": "alpha"},
    )


@pytest.fixture
def connection_config(extraction_pipeline: str) -> ConnectionConfig:
    return ConnectionConfig(
        project=os.environ["COGNITE_DEV_PROJECT"],
        base_url=os.environ["COGNITE_DEV_BASE_URL"],
        integration=IntegrationConfig(external_id=extraction_pipeline),
        authentication=_ClientCredentialsConfig(
            type="client-credentials",
            client_id=os.environ.get("COGNITE_DEV_CLIENT_ID", os.environ["COGNITE_CLIENT_ID"]),
            client_secret=os.environ.get("COGNITE_DEV_CLIENT_SECRET", os.environ["COGNITE_CLIENT_SECRET"]),
            scopes=Scopes(
                os.environ["COGNITE_DEV_TOKEN_SCOPES"],
            ),
            token_url=os.environ.get("COGNITE_DEV_TOKEN_URL", os.environ["COGNITE_TOKEN_URL"]),
        ),
    )


class TestConfig(ExtractorConfig):
    __test__ = False
    parameter_one: int
    parameter_two: str


@pytest.fixture
def application_config() -> TestConfig:
    return TestConfig(parameter_one=123, parameter_two="abc")


class TestExtractor(Extractor[TestConfig]):
    __test__ = False
    NAME = "Test extractor"
    EXTERNAL_ID = "test-extractor"
    DESCRIPTION = "Test of the new runtime"
    VERSION = "1.0.0"
    CONFIG_TYPE = TestConfig

    def __init_tasks__(self) -> None:
        """
        A simple task that runs on startup and logs messages at different levels.
        """

        def log_messages_task(ctx: TaskContext) -> None:
            ctx.debug("This is a debug message.")
            ctx.info("This is an info message.")
            ctx.warning("This is a warning message.")

        self.add_task(StartupTask(name="log_task", target=log_messages_task))
