import os
from dataclasses import dataclass
from enum import Enum
from typing import List, Optional

import pytest

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError


class ETestType(Enum):
    TIME_SERIES = "time_series"
    FILES = "files"
    RAW = "raw"
    ASSETS = "assets"
    EVENTS = "events"


@dataclass
class ParamTest:
    test_type: ETestType
    external_ids: Optional[List[str]] = None
    database_name: Optional[str] = None
    table_name: Optional[str] = None


@pytest.fixture
def set_upload_test(set_test_parameters: ParamTest, set_client: CogniteClient):
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


def clean_test(client: CogniteClient, test_parameter: ParamTest):
    if test_parameter.test_type == ETestType.TIME_SERIES:
        client.time_series.delete(external_id=test_parameter.external_ids, ignore_unknown_ids=True)
    elif test_parameter.test_type == ETestType.EVENTS:
        client.events.delete(external_id=test_parameter.external_ids, ignore_unknown_ids=True)
    elif test_parameter.test_type == ETestType.ASSETS:
        client.assets.delete(external_id=test_parameter.external_ids, ignore_unknown_ids=True)
    elif test_parameter.test_type == ETestType.RAW:
        try:
            client.raw.tables.delete(test_parameter.database_name, test_parameter.table_name)
        except CogniteAPIError:
            pass
    elif test_parameter.test_type == ETestType.FILES:
        for file in test_parameter.external_ids:
            try:
                client.files.delete(external_id=file)
            except CogniteNotFoundError:
                pass
