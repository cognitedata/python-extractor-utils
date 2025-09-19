import os
from dataclasses import field
from io import StringIO

import pytest

from cognite.client.credentials import OAuthClientCredentials
from cognite.extractorutils.unstable.configuration.loaders import ConfigFormat, load_io
from cognite.extractorutils.unstable.configuration.models import (
    ConfigModel,
    ConnectionConfig,
    FileSizeConfig,
    TimeIntervalConfig,
    _ClientCredentialsConfig,
)

CONFIG_EXAMPLE_ONLY_REQUIRED = """
project: test-project
base-url: https://baseurl.com

integration:
  external_id: test-pipeline

authentication:
  type: client-credentials
  client-id: testid
  client-secret: very_secret123
  token-url: https://get-a-token.com/token
  scopes: scopea scopeb
"""

CONFIG_EXAMPLE_ONLY_REQUIRED2 = """
project: test-project
base-url: https://baseurl.com

integration:
  external_id: test-pipeline

authentication:
  type: client-credentials
  client-id: testid
  client-secret: very_secret123
  token-url: https://get-a-token.com/token
  scopes: scopea scopeb
"""

FULL_CONFIG_EXAMPLE = """
project: test-project
base-url: https://baseurl.com

integration:
  external_id: test-pipeline
"""

CLIENT_CREDENTIALS_STRING = """
authentication:
  type: client-credentials
  client-id: testid
  client-secret: very_secret123
  token-url: https://get-a-token.com/token
  scopes: scopea scopeb

connection:
  retries:
    max-retries: 1
    max-backoff: 2d
    timeout: 3h
  ssl-certificates:
    verify: true
    allow-list:
    - thumbprint1
    - thumbprint2
"""
CLIENT_CERTIFICATES_STRING = """
authentication:
  type: client-certificate
  client-id: testid
  path: /path/to/cert.pem
  password: very-strong-password
  authority-url: https://you-are-authorized.com
  scopes: scopea scopeb

connection:
  retries:
    max-retries: 1
    max-backoff: 2d
    timeout: 3h
  ssl-certificates:
    verify: true
    allow-list:
    - thumbprint1
    - thumbprint2
"""

CLIENT_CREDENTIALS_LIST = """
authentication:
  type: client-credentials
  client-id: testid
  client-secret: very_secret123
  token-url: https://get-a-token.com/token
  scopes: scopea scopeb

connection:
  retries:
    max-retries: 1
    max-backoff: 2d
    timeout: 3h
  ssl-certificates:
    verify: true
    allow-list:
    - thumbprint1
    - thumbprint2
"""
CLIENT_CERTIFICATES_LIST = """
authentication:
  type: client-certificate
  client-id: testid
  path: /path/to/cert.pem
  password: very-strong-password
  authority-url: https://you-are-authorized.com
  scopes: scopea scopeb

connection:
  retries:
    max-retries: 1
    max-backoff: 2d
    timeout: 3h
  ssl-certificates:
    verify: true
    allow-list:
    - thumbprint1
    - thumbprint2
"""


@pytest.mark.parametrize("config_str", [CONFIG_EXAMPLE_ONLY_REQUIRED, CONFIG_EXAMPLE_ONLY_REQUIRED2])
def test_load_from_io(config_str: str) -> None:
    stream = StringIO(config_str)
    config = load_io(stream, ConfigFormat.YAML, ConnectionConfig)

    assert config.project == "test-project"
    assert config.base_url == "https://baseurl.com"
    assert config.integration.external_id == "test-pipeline"
    assert config.authentication.type == "client-credentials"
    assert config.authentication.client_secret == "very_secret123"
    assert list(config.authentication.scopes) == ["scopea", "scopeb"]


@pytest.mark.parametrize(
    "config_str", [FULL_CONFIG_EXAMPLE + CLIENT_CREDENTIALS_STRING, FULL_CONFIG_EXAMPLE + CLIENT_CREDENTIALS_LIST]
)
def test_full_config_client_credentials(config_str: str) -> None:
    stream = StringIO(config_str)
    config = load_io(stream, ConfigFormat.YAML, ConnectionConfig)

    assert config.project == "test-project"
    assert config.base_url == "https://baseurl.com"
    assert config.integration.external_id == "test-pipeline"

    assert config.authentication.type == "client-credentials"
    assert config.authentication.client_id == "testid"
    assert config.authentication.client_secret == "very_secret123"
    assert config.authentication.token_url == "https://get-a-token.com/token"
    assert list(config.authentication.scopes) == ["scopea", "scopeb"]

    assert config.connection.retries.max_retries == 1
    assert config.connection.retries.max_backoff.seconds == 2 * 24 * 60 * 60
    assert config.connection.retries.max_backoff == TimeIntervalConfig("2d")
    assert config.connection.retries.timeout.seconds == 3 * 60 * 60
    assert config.connection.retries.timeout == TimeIntervalConfig("3h")

    assert config.connection.ssl_certificates.verify
    assert config.connection.ssl_certificates.allow_list == ["thumbprint1", "thumbprint2"]


@pytest.mark.parametrize(
    "config_str", [FULL_CONFIG_EXAMPLE + CLIENT_CERTIFICATES_STRING, FULL_CONFIG_EXAMPLE + CLIENT_CERTIFICATES_LIST]
)
def test_full_config_client_certificates(config_str: str) -> None:
    stream = StringIO(config_str)
    config = load_io(stream, ConfigFormat.YAML, ConnectionConfig)

    assert config.project == "test-project"
    assert config.base_url == "https://baseurl.com"
    assert config.integration.external_id == "test-pipeline"

    assert config.authentication.type == "client-certificate"
    assert config.authentication.client_id == "testid"
    assert config.authentication.password == "very-strong-password"
    assert config.authentication.path.as_posix() == "/path/to/cert.pem"
    assert config.authentication.authority_url == "https://you-are-authorized.com"
    assert list(config.authentication.scopes) == ["scopea", "scopeb"]

    assert config.connection.retries.max_retries == 1
    assert config.connection.retries.max_backoff.seconds == 2 * 24 * 60 * 60
    assert config.connection.retries.max_backoff == TimeIntervalConfig("2d")
    assert config.connection.retries.timeout.seconds == 3 * 60 * 60
    assert config.connection.retries.timeout == TimeIntervalConfig("3h")

    assert config.connection.ssl_certificates.verify
    assert config.connection.ssl_certificates.allow_list == ["thumbprint1", "thumbprint2"]


def test_from_env() -> None:
    # Init a config from env
    config = ConnectionConfig.from_environment()
    assert config.project == os.environ["COGNITE_PROJECT"]
    assert config.base_url == os.environ["COGNITE_BASE_URL"]
    assert isinstance(config.authentication, _ClientCredentialsConfig)

    client = config.get_cognite_client("utils-tests")
    assert client.config.project == os.environ["COGNITE_PROJECT"]
    assert client.config.base_url == os.environ["COGNITE_BASE_URL"]
    assert isinstance(client.config.credentials, OAuthClientCredentials)

    # Check that the produces cogniteclient object is valid
    assert len(client.assets.list(limit=1)) == 1


class CustomFileConfig(ConfigModel):
    file_size: FileSizeConfig = field(default_factory=lambda: FileSizeConfig("1MB"))
    file_max_size: FileSizeConfig = field(default_factory=lambda: FileSizeConfig("10MiB"))


def test_parse_file_size() -> None:
    config_str = """
file_size: 25MB
file_max_size: 10MiB
"""
    stream = StringIO(config_str)
    config = load_io(stream, ConfigFormat.YAML, CustomFileConfig)
    assert config.file_size == FileSizeConfig("25MB")
    assert config.file_size.bytes == 25_000_000
    assert config.file_size._expression == "25MB"
    assert config.file_max_size == FileSizeConfig("10MiB")
    assert config.file_max_size.bytes == 10_485_760
    assert config.file_max_size._expression == "10MiB"
