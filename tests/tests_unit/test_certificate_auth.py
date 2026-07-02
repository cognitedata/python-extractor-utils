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

import binascii
import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cognite.client.credentials import OAuthClientCertificate
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

# MSAL internal that calls binascii.a2b_hex on the thumbprint during JWT assertion building.
# If this import breaks after a MSAL upgrade, verify a2b_hex is still called on cert_thumbprint.
from msal.oauth2cli.assertion import _encode_thumbprint

from cognite.extractorutils.configtools._util import _load_certificate_data
from cognite.extractorutils.configtools.elements import (
    AuthenticatorConfig,
    CertificateConfig,
    CogniteConfig,
)
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig

_FAKE_THUMB = b"A5BFE559AA1234567890ABCDEF123456DEADBEEF"
_FAKE_KEY = b"-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEA...\n-----END RSA PRIVATE KEY-----\n"
_AUTHORITY_URL = "https://login.microsoftonline.com/test-tenant"
_CLIENT_ID = "test-client-id"
_SCOPES = ["https://api.cognitedata.com/.default"]


@pytest.fixture(scope="module")
def self_signed_cert_pem(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Generate a self-signed RSA cert + private key in a single .pem file."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc))
        .not_valid_after(datetime.datetime(2099, 1, 1, tzinfo=datetime.timezone.utc))
        .sign(key, hashes.SHA256())
    )
    pem = cert.public_bytes(serialization.Encoding.PEM)
    pem += key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption(),
    )
    path = tmp_path_factory.mktemp("certs") / "test_cert.pem"
    path.write_bytes(pem)
    return path


def test_load_certificate_data_returns_bytes(self_signed_cert_pem: Path) -> None:
    """_load_certificate_data must return (bytes, bytes) — establishes the type contract."""
    thumbprint, key = _load_certificate_data(self_signed_cert_pem, password=None)
    assert isinstance(thumbprint, bytes), "thumbprint must be bytes"
    assert isinstance(key, bytes), "private key must be bytes"


def test_bare_str_produces_repr_with_b_prefix() -> None:
    """Document the bug: bare str() wraps bytes in b'...' repr, making a2b_hex fail."""
    broken = str(_FAKE_THUMB)
    assert broken.startswith("b'"), f"Expected b'...' repr, got: {broken!r}"
    with pytest.raises(binascii.Error, match=r"(?i)odd"):
        binascii.a2b_hex(broken)


def test_utf8_decode_produces_clean_hex() -> None:
    """str(bytes, 'utf-8') yields a clean even-length hex string that a2b_hex accepts."""
    clean = str(_FAKE_THUMB, "utf-8")
    assert not clean.startswith("b'"), f"Unexpected b'...' prefix: {clean!r}"
    assert len(clean) % 2 == 0, f"Hex string must have even length, got {len(clean)}"
    binascii.a2b_hex(clean)  # must not raise


def test_oauth_certificate_constructs_with_correct_strings() -> None:
    """OAuthClientCertificate must accept decoded thumbprint and key without raising."""
    with patch("msal.ConfidentialClientApplication.__init__", return_value=None):
        OAuthClientCertificate(
            authority_url=_AUTHORITY_URL,
            client_id=_CLIENT_ID,
            cert_thumbprint=str(_FAKE_THUMB, "utf-8"),
            certificate=str(_FAKE_KEY, "utf-8"),
            scopes=_SCOPES,
        )


def test_oauth_certificate_broken_str_raises_on_auth() -> None:
    """Regression anchor: bare str() thumbprint causes binascii.Error during JWT assertion building.

    OAuthClientCertificate delegates JWT creation to msal.oauth2cli.assertion._encode_thumbprint,
    which calls binascii.a2b_hex. Verify this call path fails with the broken repr string.
    """
    broken = str(_FAKE_THUMB)  # intentionally broken — b'...' repr
    with pytest.raises(binascii.Error, match=r"(?i)odd"):
        _encode_thumbprint(broken)


def test_elements_passes_decoded_strings_to_oauth(tmp_path: Path) -> None:
    """CogniteConfig.get_cognite_client() (elements.py) must pass decoded strings to OAuthClientCertificate."""
    cert_path = tmp_path / "cert.pem"
    cert_path.write_bytes(b"placeholder")  # contents irrelevant; _load_certificate_data is mocked

    mock_oauth = MagicMock()

    config = CogniteConfig(
        project="my-project",
        host="https://api.cognitedata.com",
        idp_authentication=AuthenticatorConfig(
            client_id=_CLIENT_ID,
            scopes=_SCOPES,
            certificate=CertificateConfig(
                path=str(cert_path),
                password=None,
                authority_url=_AUTHORITY_URL,
            ),
        ),
    )

    # _load_certificate_data is imported directly into elements, so patch there.
    with (
        patch(
            "cognite.extractorutils.configtools.elements._load_certificate_data", return_value=(_FAKE_THUMB, _FAKE_KEY)
        ),
        patch("cognite.extractorutils.configtools.elements.OAuthClientCertificate", mock_oauth),
        patch("cognite.extractorutils.configtools.elements.ClientConfig"),
        patch("cognite.extractorutils.configtools.elements.CogniteClient"),
    ):
        config.get_cognite_client("test-extractor")

    mock_oauth.assert_called_once()
    kwargs = mock_oauth.call_args.kwargs
    # Variable named to match the source in elements.py: `thumprint`
    thumprint = kwargs["cert_thumbprint"]
    key = kwargs["certificate"]

    assert not thumprint.startswith("b'"), f"cert_thumbprint must not be a bytes repr; got {thumprint!r}"
    assert not key.startswith("b'"), f"certificate must not be a bytes repr; got {key!r}"
    assert thumprint == _FAKE_THUMB.decode("utf-8"), f"cert_thumbprint must equal decoded bytes; got {thumprint!r}"
    assert key == _FAKE_KEY.decode("utf-8"), f"certificate must equal decoded bytes; got {key!r}"


def test_models_passes_decoded_strings_to_oauth(tmp_path: Path) -> None:
    """ConnectionConfig.get_cognite_client() (models.py) must pass decoded strings to OAuthClientCertificate."""
    cert_path = tmp_path / "cert.pem"
    cert_path.write_bytes(b"placeholder")

    mock_oauth = MagicMock()

    config = ConnectionConfig.model_validate(
        {
            "project": "my-project",
            "base_url": "https://api.cognitedata.com",
            "integration": {"external_id": "test-extractor"},
            "authentication": {
                "type": "client-certificate",
                "client_id": _CLIENT_ID,
                "path": str(cert_path),
                "authority_url": _AUTHORITY_URL,
                "scopes": " ".join(_SCOPES),
            },
        }
    )

    with (
        patch(
            "cognite.extractorutils.unstable.configuration.models._load_certificate_data",
            return_value=(_FAKE_THUMB, _FAKE_KEY),
        ),
        patch("cognite.extractorutils.unstable.configuration.models.OAuthClientCertificate", mock_oauth),
        patch("cognite.extractorutils.unstable.configuration.models.ClientConfig"),
        patch("cognite.extractorutils.unstable.configuration.models.CogniteClient"),
    ):
        config.get_cognite_client("test-extractor")

    mock_oauth.assert_called_once()
    kwargs = mock_oauth.call_args.kwargs
    thumbprint = kwargs["cert_thumbprint"]
    key = kwargs["certificate"]

    assert not thumbprint.startswith("b'"), f"cert_thumbprint must not be a bytes repr; got {thumbprint!r}"
    assert not key.startswith("b'"), f"certificate must not be a bytes repr; got {key!r}"
    assert thumbprint == _FAKE_THUMB.decode("utf-8"), f"cert_thumbprint must equal decoded bytes; got {thumbprint!r}"
    assert key == _FAKE_KEY.decode("utf-8"), f"certificate must equal decoded bytes; got {key!r}"
