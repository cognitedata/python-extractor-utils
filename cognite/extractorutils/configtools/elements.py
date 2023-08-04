#  Copyright 2023 Cognite AS
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
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from logging.handlers import TimedRotatingFileHandler
from threading import Event
from time import sleep
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin

import yaml
from prometheus_client import REGISTRY, start_http_server

from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import CredentialProvider, OAuthClientCertificate, OAuthClientCredentials
from cognite.client.data_classes import Asset, DataSet, ExtractionPipeline
from cognite.extractorutils.configtools._util import _load_certificate_data
from cognite.extractorutils.exceptions import InvalidConfigError
from cognite.extractorutils.metrics import AbstractMetricsPusher, CognitePusher, PrometheusPusher
from cognite.extractorutils.statestore import AbstractStateStore, LocalStateStore, NoStateStore, RawStateStore
from cognite.extractorutils.util import EitherId

_logger = logging.getLogger(__name__)


@dataclass
class CertificateConfig:
    path: str
    password: Optional[str]
    authority_url: Optional[str] = None


@dataclass
class AuthenticatorConfig:
    """
    Configuration parameters for an OIDC flow
    """

    client_id: str
    scopes: List[str]
    secret: Optional[str]
    tenant: Optional[str] = None
    token_url: Optional[str] = None
    resource: Optional[str] = None
    audience: Optional[str] = None
    authority: str = "https://login.microsoftonline.com/"
    min_ttl: float = 30  # minimum time to live: refresh token ahead of expiration
    certificate: Optional[CertificateConfig] = None


@dataclass
class ConnectionConfig:
    """
    Configuration parameters for the global_config python SDK settings
    """

    disable_gzip: bool = False
    status_forcelist: List[int] = field(default_factory=lambda: [429, 502, 503, 504])
    max_retries: int = 10
    max_retries_connect: int = 3
    max_retry_backoff: int = 30
    max_connection_pool_size: int = 50
    disable_ssl: bool = False
    proxies: Dict[str, str] = field(default_factory=dict)


@dataclass
class EitherIdConfig:
    id: Optional[int]
    external_id: Optional[str]

    @property
    def either_id(self) -> EitherId:
        return EitherId(id=self.id, external_id=self.external_id)


class TimeIntervalConfig(yaml.YAMLObject):
    def __init__(self, expression: str) -> None:
        self._interval, self._expression = TimeIntervalConfig._parse_expression(expression)

    @classmethod
    def _parse_expression(cls, expression: str) -> Tuple[int, str]:
        # First, try to parse pure number and assume seconds (for backwards compatibility)
        try:
            return int(expression), f"{expression}s"
        except ValueError:
            pass

        match = re.match(r"(\d+)(s|m|h|d)", expression)
        if not match:
            raise InvalidConfigError("Invalid interval pattern")

        number, unit = match.groups()
        numeric_unit = {"s": 1, "m": 60, "h": 60 * 60, "d": 60 * 60 * 24}[unit]

        return int(number) * numeric_unit, expression

    @property
    def seconds(self) -> int:
        return self._interval

    @property
    def minutes(self) -> float:
        return self._interval / 60

    @property
    def hours(self) -> float:
        return self._interval / (60 * 60)

    @property
    def days(self) -> float:
        return self._interval / (60 * 60 * 24)

    @property
    def timedelta(self) -> timedelta:
        days = self._interval // (60 * 60 * 24)
        seconds = self._interval % (60 * 60 * 24)
        return timedelta(days=days, seconds=seconds)

    def __int__(self) -> int:
        return int(self._interval)

    def __float__(self) -> float:
        return float(self._interval)

    def __str__(self) -> str:
        return self._expression

    def __repr__(self) -> str:
        return self._expression


class FileSizeConfig(yaml.YAMLObject):
    def __init__(self, expression: str) -> None:
        self._bytes, self._expression = FileSizeConfig._parse_expression(expression)

    @classmethod
    def _parse_expression(cls, expression: str) -> Tuple[int, str]:
        # First, try to parse pure number and assume bytes
        try:
            return int(expression), f"{expression}s"
        except ValueError:
            pass

        sizes = {
            "kb": 1000,
            "mb": 1_000_000,
            "gb": 1_000_000_000,
            "tb": 1_000_000_000_000,
            "kib": 1024,
            "mib": 1_048_576,  # 1024 ^ 2
            "gib": 1_073_741_824,  # 1024 ^ 3
            "tib": 1_099_511_627_776,  # 1024 ^ 4
        }
        expression_lower = expression.lower()
        for size in sizes:
            if expression_lower.endswith(size):
                return int(float(expression_lower.replace(size, "")) * sizes[size]), expression
        else:
            raise InvalidConfigError(f"Invalid unit for file size: {expression}. Valid units: {sizes.keys()}")

    @property
    def bytes(self) -> int:
        return self._bytes

    @property
    def kilobytes(self) -> float:
        return self._bytes / 1000

    @property
    def megabytes(self) -> float:
        return self._bytes / 1_000_000

    @property
    def gigabytes(self) -> float:
        return self._bytes / 1_000_000_000

    @property
    def terabytes(self) -> float:
        return self._bytes / 1_000_000_000_000

    @property
    def kibibytes(self) -> float:
        return self._bytes / 1024

    @property
    def mebibytes(self) -> float:
        return self._bytes / 1_048_576

    @property
    def gibibytes(self) -> float:
        return self._bytes / 1_073_741_824

    @property
    def tebibytes(self) -> float:
        return self._bytes / 1_099_511_627_776

    def __int__(self) -> int:
        return int(self._bytes)

    def __float__(self) -> float:
        return float(self._bytes)

    def __str__(self) -> str:
        return self._expression

    def __repr__(self) -> str:
        return self._expression


@dataclass
class CogniteConfig:
    """
    Configuration parameters for CDF connection, such as project name, host address and authentication
    """

    project: str
    idp_authentication: AuthenticatorConfig
    data_set: Optional[EitherIdConfig]
    data_set_id: Optional[int]
    data_set_external_id: Optional[str]
    extraction_pipeline: Optional[EitherIdConfig]
    timeout: TimeIntervalConfig = TimeIntervalConfig("30s")
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    external_id_prefix: str = ""
    host: str = "https://api.cognitedata.com"

    def get_cognite_client(
        self, client_name: str, token_custom_args: Optional[Dict[str, str]] = None, use_experimental_sdk: bool = False
    ) -> CogniteClient:
        from cognite.client.config import global_config

        global_config.disable_pypi_version_check = True
        global_config.disable_gzip = self.connection.disable_gzip
        global_config.status_forcelist = set(self.connection.status_forcelist)
        global_config.max_retries = self.connection.max_retries
        global_config.max_retries_connect = self.connection.max_retries_connect
        global_config.max_retry_backoff = self.connection.max_retry_backoff
        global_config.max_connection_pool_size = self.connection.max_connection_pool_size
        global_config.disable_ssl = self.connection.disable_ssl
        global_config.proxies = self.connection.proxies

        credential_provider: CredentialProvider
        if self.idp_authentication.certificate:
            if self.idp_authentication.certificate.authority_url:
                authority_url = self.idp_authentication.certificate.authority_url
            elif self.idp_authentication.tenant:
                authority_url = urljoin(self.idp_authentication.authority, self.idp_authentication.tenant)
            else:
                raise InvalidConfigError("Either authority-url or tenant is required for certificate authentication")
            (thumprint, key) = _load_certificate_data(
                self.idp_authentication.certificate.path, self.idp_authentication.certificate.password
            )
            credential_provider = OAuthClientCertificate(
                authority_url=authority_url,
                client_id=self.idp_authentication.client_id,
                cert_thumbprint=str(thumprint),
                certificate=str(key),
                scopes=self.idp_authentication.scopes,
            )

        elif self.idp_authentication.secret:
            kwargs: Dict[str, Any] = {}
            if self.idp_authentication.token_url:
                kwargs["token_url"] = self.idp_authentication.token_url
            elif self.idp_authentication.tenant:
                base_url = urljoin(self.idp_authentication.authority, self.idp_authentication.tenant)
                kwargs["token_url"] = f"{base_url}/oauth2/v2.0/token"
            kwargs["client_id"] = self.idp_authentication.client_id
            kwargs["client_secret"] = self.idp_authentication.secret
            kwargs["scopes"] = self.idp_authentication.scopes
            if token_custom_args is None:
                token_custom_args = {}
            if self.idp_authentication.resource:
                token_custom_args["resource"] = self.idp_authentication.resource
            if self.idp_authentication.audience:
                token_custom_args["audience"] = self.idp_authentication.audience
            credential_provider = OAuthClientCredentials(**kwargs, **token_custom_args)  # type: ignore

        else:
            raise InvalidConfigError("No client certificate or secret provided")

        client_config = ClientConfig(
            project=self.project,
            base_url=self.host,
            client_name=client_name,
            timeout=self.timeout.seconds,
            credentials=credential_provider,
        )

        if use_experimental_sdk:
            from cognite.experimental import CogniteClient as ExperimentalCogniteClient  # type: ignore

            return ExperimentalCogniteClient(client_config)

        return CogniteClient(client_config)

    def get_data_set(self, cdf_client: CogniteClient) -> Optional[DataSet]:
        if self.data_set_external_id:
            logging.getLogger(__name__).warning(
                "Using data-set-external-id is deprecated, please use data-set/external-id instead"
            )
            return cdf_client.data_sets.retrieve(external_id=self.data_set_external_id)

        if self.data_set_id:
            logging.getLogger(__name__).warning("Using data-set-id is deprecated, please use data-set/id instead")
            return cdf_client.data_sets.retrieve(external_id=self.data_set_external_id)

        if not self.data_set:
            return None

        return cdf_client.data_sets.retrieve(
            id=self.data_set.either_id.internal_id, external_id=self.data_set.either_id.external_id  # type: ignore
        )

    def get_extraction_pipeline(self, cdf_client: CogniteClient) -> Optional[ExtractionPipeline]:
        if not self.extraction_pipeline:
            return None

        either_id = self.extraction_pipeline.either_id
        extraction_pipeline = cdf_client.extraction_pipelines.retrieve(
            id=either_id.internal_id,  # type: ignore
            external_id=either_id.external_id,  # type: ignore
        )
        if extraction_pipeline is None:
            raise ValueError(f"Extraction pipeline with {either_id.type()} {either_id.content()} not found")
        return extraction_pipeline


@dataclass
class _ConsoleLoggingConfig:
    level: str = "INFO"


@dataclass
class _FileLoggingConfig:
    path: str
    level: str = "INFO"
    retention: int = 7


@dataclass
class LoggingConfig:
    """
    Logging settings, such as log levels and path to log file
    """

    console: Optional[_ConsoleLoggingConfig]
    file: Optional[_FileLoggingConfig]
    # enables metrics on the number of log messages recorded (per logger and level)
    # In order to collect/see result MetricsConfig should be set as well, so metrics are propagated to
    # Prometheus and/or Cognite
    metrics: Optional[bool] = False

    def setup_logging(self, suppress_console: bool = False) -> None:
        """
        Sets up the default logger in the logging package to be configured as defined in this config object

        Args:
            suppress_console: Don't log to console regardless of config. Useful when running an extractor as a Windows
                service
        """
        fmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d UTC [%(levelname)-8s] %(threadName)s - %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        # Set logging to UTC
        fmt.converter = time.gmtime

        root = logging.getLogger()

        if self.console and not suppress_console and not root.hasHandlers():
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.console.level)
            console_handler.setFormatter(fmt)

            root.addHandler(console_handler)

            if root.getEffectiveLevel() > console_handler.level:
                root.setLevel(console_handler.level)

        if self.file:
            file_handler = TimedRotatingFileHandler(
                filename=self.file.path,
                when="midnight",
                utc=True,
                backupCount=self.file.retention,
            )
            file_handler.setLevel(self.file.level)
            file_handler.setFormatter(fmt)

            for handler in root.handlers:
                if hasattr(handler, "baseFilename") and handler.baseFilename == file_handler.baseFilename:
                    return

            root.addHandler(file_handler)

            if root.getEffectiveLevel() > file_handler.level:
                root.setLevel(file_handler.level)


@dataclass
class _PushGatewayConfig:
    host: str
    job_name: str
    username: Optional[str]
    password: Optional[str]

    clear_after: Optional[TimeIntervalConfig]
    push_interval: TimeIntervalConfig = TimeIntervalConfig("30s")


class _PromServerConfig:
    port: int = 9000
    host: str = "0.0.0.0"


@dataclass
class _CogniteMetricsConfig:
    external_id_prefix: str
    asset_name: Optional[str]
    asset_external_id: Optional[str]

    push_interval: TimeIntervalConfig = TimeIntervalConfig("30s")


@dataclass
class MetricsConfig:
    """
    Destination(s) for metrics, including options for one or several Prometheus push gateways, and pushing as CDF Time
    Series.
    """

    push_gateways: Optional[List[_PushGatewayConfig]]
    cognite: Optional[_CogniteMetricsConfig]
    server: Optional[_PromServerConfig]

    def start_pushers(self, cdf_client: CogniteClient, cancellation_token: Event = Event()) -> None:
        self._pushers: List[AbstractMetricsPusher] = []
        self._clear_on_stop: Dict[PrometheusPusher, int] = {}

        push_gateways = self.push_gateways or []

        pusher: AbstractMetricsPusher
        for counter, push_gateway in enumerate(push_gateways):
            pusher = PrometheusPusher(
                job_name=push_gateway.job_name,
                username=push_gateway.username,
                password=push_gateway.password,
                url=push_gateway.host,
                push_interval=push_gateway.push_interval.seconds,
                thread_name=f"MetricsPusher_{counter}",
                cancellation_token=cancellation_token,
            )

            pusher.start()
            self._pushers.append(pusher)
            if push_gateway.clear_after is not None:
                self._clear_on_stop[pusher] = push_gateway.clear_after.seconds

        if self.cognite:
            asset = None

            if self.cognite.asset_name is not None and self.cognite.asset_external_id:
                asset = Asset(name=self.cognite.asset_name, external_id=self.cognite.asset_external_id)

            pusher = CognitePusher(
                cdf_client=cdf_client,
                external_id_prefix=self.cognite.external_id_prefix,
                push_interval=self.cognite.push_interval.seconds,
                asset=asset,
                thread_name="CogniteMetricsPusher",  # There is only one Cognite project as a target
                cancellation_token=cancellation_token,
            )

            pusher.start()
            self._pushers.append(pusher)

        if self.server:
            start_http_server(self.server.port, self.server.host, registry=REGISTRY)

    def stop_pushers(self) -> None:
        pushers = self.__dict__.get("_pushers") or []

        for pusher in pushers:
            pusher.stop()

        if len(self._clear_on_stop) > 0:
            wait_time = max(self._clear_on_stop.values())
            _logger.debug("Waiting %d seconds before clearing gateways", wait_time)

            sleep(wait_time)
            for pusher in self._clear_on_stop.keys():
                pusher.clear_gateway()


class ConfigType(Enum):
    LOCAL = "local"
    REMOTE = "remote"


@dataclass
class _BaseConfig:
    _file_hash: Optional[str] = field(init=False, repr=False, default=None)

    type: Optional[ConfigType]
    cognite: CogniteConfig


@dataclass
class BaseConfig(_BaseConfig):
    """
    Basis for an extractor config, containing config version, ``CogniteConfig`` and ``LoggingConfig``
    """

    version: Optional[Union[str, int]]
    logger: LoggingConfig


@dataclass
class RawDestinationConfig:
    database: str
    table: str


@dataclass
class RawStateStoreConfig(RawDestinationConfig):
    upload_interval: TimeIntervalConfig = TimeIntervalConfig("30s")


@dataclass
class LocalStateStoreConfig:
    path: str
    save_interval: TimeIntervalConfig = TimeIntervalConfig("30s")


@dataclass
class StateStoreConfig:
    raw: Optional[RawStateStoreConfig] = None
    local: Optional[LocalStateStoreConfig] = None

    def create_state_store(
        self, cdf_client: Optional[CogniteClient] = None, default_to_local: bool = True
    ) -> AbstractStateStore:
        """
        Create a state store object based on the config.

        Args:
            cdf_client: CogniteClient object to use in case of a RAW state store (ignored otherwise)
            default_to_local: If true, return a LocalStateStore if no state store is configured. Otherwise return a
                NoStateStore

        Returns:
            An (uninitialized) state store
        """
        if self.raw and self.local:
            raise ValueError("Only one state store can be used simultaneously")

        if self.raw:
            if cdf_client is None:
                raise TypeError("A cognite client object must be provided when state store is RAW")

            return RawStateStore(
                cdf_client=cdf_client,
                database=self.raw.database,
                table=self.raw.table,
                save_interval=self.raw.upload_interval.seconds,
            )

        if self.local:
            return LocalStateStore(file_path=self.local.path, save_interval=self.local.save_interval.seconds)

        if default_to_local:
            return LocalStateStore(file_path="states.json")
        else:
            return NoStateStore()
