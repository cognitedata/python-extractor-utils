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

from cognite.extractorutils.configtools._util import _load_certificate_data
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig

_AUTHORITY_URL = os.environ.get("COGNITE_AUTHORITY_URL")
_CLIENT_ID = os.environ.get("COGNITE_DEV_CLIENT_ID") or os.environ.get("COGNITE_CLIENT_ID")
_PROJECT = os.environ.get("COGNITE_DEV_PROJECT")
_BASE_URL = os.environ.get("COGNITE_DEV_BASE_URL")
_SCOPES = os.environ.get("COGNITE_DEV_TOKEN_SCOPES")
_PEM = os.environ.get("CERTIFICATE_AUTH_PEM")


@pytest.fixture(scope="module")
def cert_pem_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    pem_bytes = base64.b64decode(_PEM)
    path = tmp_path_factory.mktemp("certs") / "cert.pem"
    path.write_bytes(pem_bytes)
    return path


@pytest.fixture(scope="module")
def cognite_client(cert_pem_path: Path) -> CogniteClient:
    config = ConnectionConfig.model_validate(
        {
            "project": _PROJECT,
            "base_url": _BASE_URL,
            "integration": {"external_id": "extractor-utils-cert-integration-test"},
            "authentication": {
                "type": "client-certificate",
                "client_id": _CLIENT_ID,
                "path": str(cert_pem_path),
                "authority_url": _AUTHORITY_URL,
                "scopes": _SCOPES,
            },
        }
    )
    return config.get_cognite_client("extractor-utils-cert-integration-test")


@pytest.mark.unstable
def test_load_certificate_data_parses_pem(cert_pem_path: Path) -> None:
    """_load_certificate_data must parse the real PEM and return valid hex bytes."""
    thumbprint, key = _load_certificate_data(cert_pem_path, password=None)
    assert isinstance(thumbprint, bytes)
    assert isinstance(key, bytes)
    binascii.a2b_hex(thumbprint)  # raises if thumbprint is not valid hex


@pytest.mark.unstable
def test_connection_config_certificate_auth(cognite_client: CogniteClient) -> None:
    """ConnectionConfig with client-certificate must acquire a token and grant access to the expected CDF project."""
    token_info = cognite_client.iam.token.inspect()
    assert token_info is not None
    assert token_info.subject, "Token subject must be non-empty (confirms a real identity was issued)"
    assert any(p.url_name == _PROJECT for p in token_info.projects), (
        f"Expected project '{_PROJECT}' in token's project list, got: {[p.url_name for p in token_info.projects]}"
    )
