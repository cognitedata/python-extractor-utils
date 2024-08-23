import os

import pytest

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials


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
