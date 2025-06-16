"""
Module containing pre-built models for common extractor configuration.
"""

import os
import re
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Literal

from humps import kebabize
from pydantic import BaseModel, ConfigDict, Field, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema
from typing_extensions import assert_never

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import (
    CredentialProvider,
    OAuthClientCertificate,
    OAuthClientCredentials,
)
from cognite.extractorutils.configtools._util import _load_certificate_data
from cognite.extractorutils.exceptions import InvalidConfigError

__all__ = [
    "AuthenticationConfig",
    "ConfigModel",
    "ConnectionConfig",
    "CronConfig",
    "ExtractorConfig",
    "IntervalConfig",
    "LogConsoleHandlerConfig",
    "LogFileHandlerConfig",
    "LogHandlerConfig",
    "LogLevel",
    "ScheduleConfig",
    "TimeIntervalConfig",
]


class ConfigModel(BaseModel):
    """
    Base model for configuration objects, setting the correct pydantic options for extractor config.
    """

    model_config = ConfigDict(
        alias_generator=kebabize,
        populate_by_name=True,
        extra="forbid",
        # arbitrary_types_allowed=True,
    )


class _ClientCredentialsConfig(ConfigModel):
    type: Literal["client-credentials"]
    client_id: str
    client_secret: str
    token_url: str
    scopes: list[str]
    resource: str | None = None
    audience: str | None = None


class _ClientCertificateConfig(ConfigModel):
    type: Literal["client-certificate"]
    client_id: str
    path: Path
    password: str | None = None
    authority_url: str
    scopes: list[str]


AuthenticationConfig = Annotated[_ClientCredentialsConfig | _ClientCertificateConfig, Field(discriminator="type")]


class TimeIntervalConfig:
    """
    Configuration parameter for setting a time interval.
    """

    def __init__(self, expression: str) -> None:
        self._interval, self._expression = TimeIntervalConfig._parse_expression(expression)

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        """
        Pydantic hook to define how this class should be serialized/deserialized.

        This allows the class to be used as a field in Pydantic models.
        """
        return core_schema.no_info_after_validator_function(cls, handler(str | int))

    def __eq__(self, other: object) -> bool:
        """
        Two TimeIntervalConfig objects are equal if they have the same number of seconds in their interval.
        """
        if not isinstance(other, TimeIntervalConfig):
            return NotImplemented
        return self._interval == other._interval

    def __hash__(self) -> int:
        """
        Hash function for TimeIntervalConfig based on the number of seconds in the interval.
        """
        return hash(self._interval)

    @classmethod
    def _parse_expression(cls, expression: str) -> tuple[int, str]:
        # First, try to parse pure number and assume seconds (for backwards compatibility)
        try:
            return int(expression), f"{expression}s"
        except ValueError:
            pass

        match = re.match(r"(\d+)[ \t]*(s|m|h|d)", expression)
        if not match:
            raise InvalidConfigError("Invalid interval pattern")

        number, unit = match.groups()
        numeric_unit = {"s": 1, "m": 60, "h": 60 * 60, "d": 60 * 60 * 24}[unit]

        return int(number) * numeric_unit, expression

    @property
    def seconds(self) -> int:
        """
        Time interval as number of seconds.
        """
        return self._interval

    @property
    def minutes(self) -> float:
        """
        Time interval as number of minutes.

        This is a float since the underlying interval is in seconds.
        """
        return self._interval / 60

    @property
    def hours(self) -> float:
        """
        Time interval as number of hours.

        This is a float since the underlying interval is in seconds.
        """
        return self._interval / (60 * 60)

    @property
    def days(self) -> float:
        """
        Time interval as number of days.

        This is a float since the underlying interval is in seconds.
        """
        return self._interval / (60 * 60 * 24)

    @property
    def timedelta(self) -> timedelta:
        """
        Time interval as a timedelta object.
        """
        days = self._interval // (60 * 60 * 24)
        seconds = self._interval % (60 * 60 * 24)
        return timedelta(days=days, seconds=seconds)

    def __int__(self) -> int:
        """
        Returns the time interval as a number of seconds.
        """
        return int(self._interval)

    def __float__(self) -> float:
        """
        Returns the time interval as a number of seconds.
        """
        return float(self._interval)

    def __str__(self) -> str:
        """
        Returns the time interval as a human readable string.
        """
        return self._expression

    def __repr__(self) -> str:
        """
        Returns the time interval as a human readable string.
        """
        return self._expression


class _ConnectionParameters(ConfigModel):
    gzip_compression: bool = False
    status_forcelist: list[int] = Field(default_factory=lambda: [429, 502, 503, 504])
    max_retries: int = 10
    max_retries_connect: int = 3
    max_retry_backoff: TimeIntervalConfig = Field(default_factory=lambda: TimeIntervalConfig("30s"))
    max_connection_pool_size: int = 50
    ssl_verify: bool = True
    proxies: dict[str, str] = Field(default_factory=dict)
    timeout: TimeIntervalConfig = Field(default_factory=lambda: TimeIntervalConfig("30s"))


class ConnectionConfig(ConfigModel):
    """
    Configuration for connecting to a Cognite Data Fusion project.

    This configuration includes the project name, base URL, integration name, and authentication details, as well as
    optional connection parameters.

    This configuration is common for all extractors.
    """

    project: str
    base_url: str

    integration: str

    authentication: AuthenticationConfig

    connection: _ConnectionParameters = Field(default_factory=_ConnectionParameters)

    def get_cognite_client(self, client_name: str) -> CogniteClient:
        """
        Create a CogniteClient instance using the configuration parameters.

        Args:
            client_name: Name of the client, set as the x-cdp-app header in the requests

        Returns:
            CogniteClient: An instance of CogniteClient configured with the provided parameters.
        """
        from cognite.client.config import global_config

        global_config.disable_pypi_version_check = True
        global_config.disable_gzip = not self.connection.gzip_compression
        global_config.status_forcelist = set(self.connection.status_forcelist)
        global_config.max_retries = self.connection.max_retries
        global_config.max_retries_connect = self.connection.max_retries_connect
        global_config.max_retry_backoff = self.connection.max_retry_backoff.seconds
        global_config.max_connection_pool_size = self.connection.max_connection_pool_size
        global_config.disable_ssl = not self.connection.ssl_verify
        global_config.proxies = self.connection.proxies

        credential_provider: CredentialProvider
        match self.authentication:
            case _ClientCredentialsConfig() as client_credentials:
                kwargs = {
                    "token_url": client_credentials.token_url,
                    "client_id": client_credentials.client_id,
                    "client_secret": client_credentials.client_secret,
                    "scopes": client_credentials.scopes,
                }
                if client_credentials.audience is not None:
                    kwargs["audience"] = client_credentials.audience
                if client_credentials.resource is not None:
                    kwargs["resource"] = client_credentials.resource

                credential_provider = OAuthClientCredentials(**kwargs)  # type: ignore  # I know what I'm doing

            case _ClientCertificateConfig() as client_certificate:
                thumbprint, key = _load_certificate_data(
                    client_certificate.path,
                    client_certificate.password,
                )
                credential_provider = OAuthClientCertificate(
                    authority_url=client_certificate.authority_url,
                    client_id=client_certificate.client_id,
                    cert_thumbprint=str(thumbprint),
                    certificate=str(key),
                    scopes=client_certificate.scopes,
                )

            case _:
                assert_never(self.authentication)

        client_config = ClientConfig(
            project=self.project,
            base_url=self.base_url,
            client_name=client_name,
            timeout=self.connection.timeout.seconds,
            credentials=credential_provider,
        )

        return CogniteClient(client_config)

    @classmethod
    def from_environment(cls) -> "ConnectionConfig":
        """
        Create a ConnectionConfig instance from environment variables.

        Environment variables should be set as follows:
        - COGNITE_PROJECT: The name of the Cognite Data Fusion project.
        - COGNITE_BASE_URL: The base URL of the Cognite Data Fusion instance.
        - COGNITE_INTEGRATION: The external ID of the corresponding integration in CDF.
        - COGNITE_CLIENT_ID: The client ID for authentication.
        - COGNITE_TOKEN_SCOPES: The scopes for the token.
        - COGNITE_CLIENT_SECRET: The client secret for authentication (if using client credentials).
        - COGNITE_TOKEN_URL: The token URL for authentication (if using client credentials).
        - COGNITE_CLIENT_CERTIFICATE_PATH: The path to the client certificate (if using client certificate).
        - COGNITE_AUTHORITY_URL: The authority URL for authentication (if using client certificate).

        Returns:
            ConnectionConfig: An instance of ConnectionConfig populated with the environment variables.

        Raises:
            KeyError: If any of the required environment variables are missing.
        """
        auth: AuthenticationConfig
        if "COGNITE_CLIENT_SECRET" in os.environ:
            auth = _ClientCredentialsConfig(
                type="client-credentials",
                client_id=os.environ["COGNITE_CLIENT_ID"],
                client_secret=os.environ["COGNITE_CLIENT_SECRET"],
                token_url=os.environ["COGNITE_TOKEN_URL"],
                scopes=os.environ["COGNITE_TOKEN_SCOPES"].split(","),
            )
        elif "COGNITE_CLIENT_CERTIFICATE_PATH" in os.environ:
            auth = _ClientCertificateConfig(
                type="client-certificate",
                client_id=os.environ["COGNITE_CLIENT_ID"],
                path=Path(os.environ["COGNITE_CLIENT_CERTIFICATE_PATH"]),
                password=os.environ.get("COGNITE_CLIENT_CERTIFICATE_PATH"),
                authority_url=os.environ["COGNITE_AUTHORITY_URL"],
                scopes=os.environ["COGNITE_TOKEN_SCOPES"].split(","),
            )
        else:
            raise KeyError("Missing auth, either COGNITE_CLIENT_SECRET or COGNITE_CLIENT_CERTIFICATE_PATH must be set")

        return ConnectionConfig(
            project=os.environ["COGNITE_PROJECT"],
            base_url=os.environ["COGNITE_BASE_URL"],
            integration=os.environ["COGNITE_INTEGRATION"],
            authentication=auth,
        )


class CronConfig(ConfigModel):
    """
    Configuration parameter for setting a cron schedule.
    """

    type: Literal["cron"]
    expression: str


class IntervalConfig(ConfigModel):
    """
    Configuration parameter for setting an interval schedule.
    """

    type: Literal["interval"]
    expression: TimeIntervalConfig


ScheduleConfig = Annotated[CronConfig | IntervalConfig, Field(discriminator="type")]


class LogLevel(Enum):
    """
    Enumeration of log levels for the extractor.
    """

    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"


class LogFileHandlerConfig(ConfigModel):
    """
    Configuration for a log handler that writes to a file, with daily rotation.
    """

    type: Literal["file"]
    path: Path
    level: LogLevel
    retention: int = 7


class LogConsoleHandlerConfig(ConfigModel):
    """
    Configuration for a log handler that writes to standard output.
    """

    type: Literal["console"]
    level: LogLevel


LogHandlerConfig = Annotated[LogFileHandlerConfig | LogConsoleHandlerConfig, Field(discriminator="type")]


# Mypy BS
def _log_handler_default() -> list[LogHandlerConfig]:
    return [LogConsoleHandlerConfig(type="console", level=LogLevel.INFO)]


class ExtractorConfig(ConfigModel):
    """
    Base class for application configuration for extractors.
    """

    log_handlers: list[LogHandlerConfig] = Field(default_factory=_log_handler_default)
