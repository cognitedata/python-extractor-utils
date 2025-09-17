import contextlib
import os
from collections.abc import Generator
from dataclasses import dataclass
from enum import Enum

import pytest
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

NUM_NODES = 5000
NUM_EDGES = NUM_NODES // 100


class ETestType(Enum):
    TIME_SERIES = "time_series"
    CDM_TIME_SERIES = "cdm_time_series"
    FILES = "files"
    RAW = "raw"
    ASSETS = "assets"
    EVENTS = "events"
    DATA_MODELING = "data_modeling"


@dataclass
class ParamTest:
    test_type: ETestType
    external_ids: list[str] | None = None
    database_name: str | None = None
    table_name: str | None = None
    space: str | None = None


@pytest.fixture
def set_upload_test(
    set_test_parameters: ParamTest, set_client: CogniteClient
) -> Generator[tuple[CogniteClient, ParamTest], None, None]:
    client = set_client
    test_parameter = set_test_parameters
    clean_test(client, test_parameter)
    yield client, test_parameter
    clean_test(client, test_parameter)


@pytest.fixture
def set_client() -> CogniteClient:
    cognite_project = os.environ["COGNITE_PROJECT"]
    cognite_base_url = os.environ["COGNITE_BASE_URL"]
    cognite_token_url = os.environ["COGNITE_TOKEN_URL"]
    cognite_client_id = os.environ["COGNITE_CLIENT_ID"]
    cognite_client_secret = os.environ["COGNITE_CLIENT_SECRET"]
    cognite_project_scopes = os.environ["COGNITE_TOKEN_SCOPES"].split(",")
    client_config = ClientConfig(
        project=cognite_project,
        base_url=cognite_base_url,
        credentials=OAuthClientCredentials(
            cognite_token_url, cognite_client_id, cognite_client_secret, cognite_project_scopes
        ),
        client_name="extractor-utils-integration-tests",
    )
    return CogniteClient(client_config)


def clean_test(client: CogniteClient, test_parameter: ParamTest) -> None:
    if test_parameter.test_type.value == ETestType.TIME_SERIES.value:
        client.time_series.delete(external_id=test_parameter.external_ids, ignore_unknown_ids=True)
    if test_parameter.test_type.value == ETestType.CDM_TIME_SERIES.value:
        client.data_modeling.instances.delete(
            nodes=[NodeId("ExtractorUtilsTests", i) for i in test_parameter.external_ids]
        )
    elif test_parameter.test_type.value == ETestType.EVENTS.value:
        client.events.delete(external_id=test_parameter.external_ids, ignore_unknown_ids=True)
    elif test_parameter.test_type.value == ETestType.ASSETS.value:
        client.assets.delete(external_id=test_parameter.external_ids, ignore_unknown_ids=True)
    elif test_parameter.test_type.value == ETestType.RAW.value:
        with contextlib.suppress(CogniteAPIError):
            client.raw.tables.delete(test_parameter.database_name, test_parameter.table_name)
    elif test_parameter.test_type.value == ETestType.FILES.value:
        for file in test_parameter.external_ids:
            try:
                if "core_dm" in file:
                    # This according to the core dm team should trigger the file syncer to delete any files associated to this instance
                    client.data_modeling.instances.delete(NodeId(test_parameter.space, file))
                else:
                    client.files.delete(external_id=file)
            except CogniteNotFoundError:
                pass
    elif test_parameter.test_type.value == ETestType.DATA_MODELING.value:
        client.data_modeling.instances.delete(
            nodes=[("ExtractorUtilsTests", i) for i in test_parameter.external_ids[0:NUM_NODES]],
            edges=[("ExtractorUtilsTests", i) for i in test_parameter.external_ids[NUM_NODES : NUM_NODES + NUM_EDGES]],
        )
        client.data_modeling.instances.delete(
            nodes=[("ExtractorUtilsTests", test_parameter.external_ids[-1])],
        )
