import os
from io import StringIO
from unittest.mock import Mock

import pytest
from pydantic import Field

from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import DataSet
from cognite.extractorutils.exceptions import InvalidConfigError
from cognite.extractorutils.unstable.configuration.loaders import ConfigFormat, load_io
from cognite.extractorutils.unstable.configuration.models import (
    ConfigModel,
    ConnectionConfig,
    EitherIdConfig,
    ExtractorConfig,
    FileSizeConfig,
    LogLevel,
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
    file_size: FileSizeConfig = Field(default_factory=lambda: FileSizeConfig("1MB"))
    file_max_size: FileSizeConfig = Field(default_factory=lambda: FileSizeConfig("10MiB"))


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


def test_file_size_config_default_values() -> None:
    config = CustomFileConfig()
    assert config.file_size == FileSizeConfig("1MB")
    assert config.file_max_size == FileSizeConfig("10MiB")
    assert config.file_size.bytes == 1_000_000
    assert config.file_max_size.bytes == 10_485_760


def test_file_size_config_partial_fields() -> None:
    config_str = """
file_size: 5MB
"""
    stream = StringIO(config_str)
    config = load_io(stream, ConfigFormat.YAML, CustomFileConfig)
    assert config.file_size == FileSizeConfig("5MB")
    assert config.file_max_size == FileSizeConfig("10MiB")


def test_file_size_config_equality() -> None:
    file_size_1 = FileSizeConfig("2000MB")
    file_size_2 = FileSizeConfig("2GB")
    file_size_3 = FileSizeConfig("1GB")

    assert file_size_1.bytes == 2_000_000_000
    assert file_size_2.bytes == 2_000_000_000
    assert file_size_3.bytes == 1_000_000_000
    assert file_size_1 == file_size_2
    assert file_size_3 != file_size_1


@pytest.mark.parametrize(
    "expression", ["12.3kbkb", "10XY", "abcMB", "5.5.5GB", "MB", "", " ", "10 M B", "10MB extra", "tenMB"]
)
def test_file_size_config_invalid(expression: str) -> None:
    with pytest.raises(InvalidConfigError):
        FileSizeConfig(expression)


@pytest.mark.parametrize(
    "expression, value",
    [
        ("10MB", 10_000_000),
        ("1GB", 1_000_000_000),
        ("512KiB", 524_288),
        ("2.5TB", 2_500_000_000_000),
        ("100", 100),
        ("0.5MiB", 524_288),
        ("1.2GB", 1_200_000_000),
    ],
)
def test_file_size_config_valid(expression: str, value: int) -> None:
    config = FileSizeConfig(expression)
    assert config._expression == expression
    assert config.bytes == value


def test_setting_log_level_from_any_case() -> None:
    log_level = LogLevel("DEBUG")
    assert log_level == LogLevel.DEBUG

    log_level = LogLevel("debug")
    assert log_level == LogLevel.DEBUG

    with pytest.raises(ValueError):
        LogLevel("not-a-log-level")


@pytest.mark.parametrize(
    "data_set_external_id,data_set_config,expected_call,expected_result_attrs,should_return_none",
    [
        # Test with data_set_external_id provided
        (
            "test-dataset",
            None,
            {"external_id": "test-dataset"},
            {"external_id": "test-dataset", "name": "Test Dataset"},
            False,
        ),
        # Test with data_set config using internal ID
        (
            None,
            EitherIdConfig(id=12345),
            {"id": 12345, "external_id": None},
            {"id": 12345, "name": "Test Dataset"},
            False,
        ),
        # Test with data_set config using external ID
        (
            None,
            EitherIdConfig(external_id="config-dataset"),
            {"id": None, "external_id": "config-dataset"},
            {"external_id": "config-dataset", "name": "Config Dataset"},
            False,
        ),
        # Test that data_set_external_id takes priority over data_set
        (
            "priority-dataset",
            EitherIdConfig(external_id="should-be-ignored"),
            {"external_id": "priority-dataset"},
            {"external_id": "priority-dataset", "name": "Priority Dataset"},
            False,
        ),
        # Test with neither data_set_external_id nor data_set provided
        (
            None,
            None,
            {},
            {},
            True,
        ),
    ],
)
def test_get_data_set_various_configurations(
    data_set_external_id: str | None,
    data_set_config: EitherIdConfig | None,
    expected_call: dict | None,
    expected_result_attrs: dict | None,
    should_return_none: bool,
) -> None:
    """Test get_data_set method with various configuration scenarios."""
    extractor_config = ExtractorConfig(
        retry_startup=False,
        data_set_external_id=data_set_external_id,
        data_set=data_set_config,
    )

    # Create a mock client instead of using a real one
    mock_client = Mock()

    if not should_return_none:
        mock_dataset = DataSet(**expected_result_attrs)
        mock_client.data_sets.retrieve.return_value = mock_dataset

    result = extractor_config.get_data_set(mock_client)

    if should_return_none:
        assert result is None
        mock_client.data_sets.retrieve.assert_not_called()
    else:
        assert result is not None
        for attr, value in expected_result_attrs.items():
            if attr != "name":
                assert getattr(result, attr) == value
        mock_client.data_sets.retrieve.assert_called_once_with(**expected_call)
