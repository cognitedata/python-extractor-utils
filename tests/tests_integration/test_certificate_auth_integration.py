#  Copyright 2026 Cognite AS
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import base64
import binascii
import os
from pathlib import Path

import pytest
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCertificate

from cognite.extractorutils.configtools._util import _load_certificate_data

_AUTHORITY_URL = os.environ.get("COGNITE_PROJECT_AUTHORITY_URL")
_CLIENT_ID = os.environ.get("COGNITE_CLIENT_ID")
_PROJECT = os.environ.get("COGNITE_PROJECT")
_BASE_URL = os.environ.get("COGNITE_BASE_URL")
_PEM = os.environ.get("CERTIFICATE_AUTH_PEM")
# Split by comma or whitespace; fall back to the standard CDF scope derived from base URL.
_SCOPES = [f"{_BASE_URL}/.default"]


@pytest.fixture(scope="module")
def cert_pem_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Decode the base64 PEM from env and write it to a temp file."""
    pem_bytes = base64.b64decode(_PEM)
    path = tmp_path_factory.mktemp("certs") / "cert.pem"
    path.write_bytes(pem_bytes)
    return path


@pytest.fixture(scope="module")
def cognite_client(cert_pem_path: Path) -> CogniteClient:
    thumbprint, key = _load_certificate_data(cert_pem_path, password=None)
    credentials = OAuthClientCertificate(
        authority_url=_AUTHORITY_URL,
        client_id=_CLIENT_ID,
        cert_thumbprint=str(thumbprint, "utf-8"),
        certificate=str(key, "utf-8"),
        scopes=_SCOPES,
    )
    config = ClientConfig(
        client_name="extractor-utils-cert-integration-test",
        project=_PROJECT,
        base_url=_BASE_URL,
        credentials=credentials,
    )
    return CogniteClient(config)


def test_load_certificate_data_parses_pem(cert_pem_path: Path) -> None:
    """_load_certificate_data must parse the real PEM and return valid hex bytes."""
    thumbprint, key = _load_certificate_data(cert_pem_path, password=None)
    assert isinstance(thumbprint, bytes)
    assert isinstance(key, bytes)
    binascii.a2b_hex(thumbprint)  # raises if thumbprint is not valid hex


def test_token_acquisition_succeeds(cognite_client: CogniteClient) -> None:
    """End-to-end: certificate auth must successfully acquire a token and reach CDF."""
    token_info = cognite_client.iam.token.inspect()
    assert token_info is not None
    assert token_info.subject, "Token subject must be non-empty (confirms a real identity was issued)"


def test_api_call_with_certificate_auth(cognite_client: CogniteClient) -> None:
    """Verify the acquired token grants access to the expected CDF project."""
    token_info = cognite_client.iam.token.inspect()
    projects = token_info.projects
    assert any(p.url_name == _PROJECT for p in projects), (
        f"Expected project '{_PROJECT}' in token's project list, got: {[p.url_name for p in projects]}"
    )
