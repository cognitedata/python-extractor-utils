#  Copyright 2020 Cognite AS
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

"""
Module containing tools for loading and verifying config files, and a YAML loader to automatically serialize these
dataclasses from a config file.

Configs are described as ``dataclass``\es, and use the ``BaseConfig`` class as a superclass to get a few things built-in:
config version, Cognite project and logging. Use type hints to specify types, use the ``Optional`` type to specify that
a config parameter is optional, and give the attribute a value to give it a default.

For example, a config class for an extractor may look like the following:

.. code-block:: python

    @dataclass
    class ExtractorConfig:
        parallelism: int = 10

        state_store: Optional[StateStoreConfig]
        ...

    @dataclass
    class SourceConfig:
        host: str
        username: str
        password: str
        ...


    @dataclass
    class MyConfig(BaseConfig):
        extractor: ExtractorConfig
        source: SourceConfig

You can then load a YAML file into this dataclass with the `load_yaml` function:

.. code-block:: python

    with open("config.yaml") as infile:
        config: MyConfig = load_yaml(infile, MyConfig)

The config object can additionally do several things, such as:

Creating a ``CogniteClient`` based on the config:

.. code-block:: python

    client = config.cognite.get_cognite_client("my-client")

Setup the logging according to the config:

.. code-block:: python

    config.logger.setup_logging()

Start and stop threads to automatically push all the prometheus metrics in the default prometheus registry to the
configured push-gateways:

.. code-block:: python

    config.metrics.start_pushers(client)

    # Extractor code

    config.metrics.stop_pushers()

Get a state store object as configured:

.. code-block:: python

    states = config.extractor.state_store.create_state_store()

However, all of these things will be automatically done for you if you are using the base Extractor class.
"""
import argparse
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha256
from logging.handlers import TimedRotatingFileHandler
from threading import Event
from time import sleep
from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, TextIO, Type, TypeVar, Union
from urllib.parse import urljoin

import dacite
import yaml
from cognite.client import ClientConfig, CogniteClient
from cognite.client.credentials import APIKey, OAuthClientCredentials
from cognite.client.data_classes import Asset, DataSet, ExtractionPipeline
from prometheus_client import start_http_server
from prometheus_client.core import REGISTRY
from yaml.scanner import ScannerError

from .authentication import AuthenticatorConfig
from .exceptions import InvalidConfigError
from .logging_prometheus import export_log_stats_on_root_logger
from .metrics import AbstractMetricsPusher, CognitePusher, PrometheusPusher
from .statestore import AbstractStateStore, LocalStateStore, NoStateStore, RawStateStore
from .util import EitherId

_logger = logging.getLogger(__name__)


def _to_snake_case(dictionary: Dict[str, Any], case_style: str) -> Dict[str, Any]:
    """
    Ensure that all keys in the dictionary follows the snake casing convention (recursively, so any sub-dictionaries are
    changed too).

    Args:
        dictionary: Dictionary to update.
        case_style: Existing casing convention. Either 'snake', 'hyphen' or 'camel'.

    Returns:
        An updated dictionary with keys in the given convention.
    """

    def fix_list(list_, key_translator):
        if list_ is None:
            return []

        new_list = [None] * len(list_)
        for i, element in enumerate(list_):
            if isinstance(element, dict):
                new_list[i] = fix_dict(element, key_translator)
            elif isinstance(element, list):
                new_list[i] = fix_list(element, key_translator)
            else:
                new_list[i] = element
        return new_list

    def fix_dict(dict_, key_translator):
        if dict_ is None:
            return {}

        new_dict = {}
        for key in dict_:
            if isinstance(dict_[key], dict):
                new_dict[key_translator(key)] = fix_dict(dict_[key], key_translator)
            elif isinstance(dict_[key], list):
                new_dict[key_translator(key)] = fix_list(dict_[key], key_translator)
            else:
                new_dict[key_translator(key)] = dict_[key]
        return new_dict

    def translate_hyphen(key):
        return key.replace("-", "_")

    def translate_camel(key):
        return re.sub(r"([A-Z]+)", r"_\1", key).strip("_").lower()

    if case_style == "snake" or case_style == "underscore":
        return dictionary
    elif case_style == "hyphen" or case_style == "kebab":
        return fix_dict(dictionary, translate_hyphen)
    elif case_style == "camel" or case_style == "pascal":
        return fix_dict(dictionary, translate_camel)
    else:
        raise ValueError(f"Invalid case style: {case_style}")


@dataclass
class EitherIdConfig:
    id: Optional[int]
    external_id: Optional[str]

    @property
    def either_id(self) -> EitherId:
        return EitherId(id=self.id, external_id=self.external_id)


@dataclass
class CogniteConfig:
    """
    Configuration parameters for CDF connection, such as project name, host address and API key
    """

    project: str
    api_key: Optional[str]
    idp_authentication: Optional[AuthenticatorConfig]
    data_set: Optional[EitherIdConfig]
    data_set_id: Optional[int]
    data_set_external_id: Optional[str]
    extraction_pipeline: Optional[EitherIdConfig]
    timeout: int = 30
    external_id_prefix: str = ""
    host: str = "https://api.cognitedata.com"

    def get_cognite_client(
        self, client_name: str, token_custom_args: Optional[Dict[str, str]] = None, use_experimental_sdk=False
    ) -> CogniteClient:
        from cognite.client.config import global_config

        global_config.disable_pypi_version_check = True

        if self.api_key:
            credential_provider = APIKey(self.api_key)
        elif self.idp_authentication:
            kwargs = {}
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
            credential_provider = OAuthClientCredentials(**kwargs, **token_custom_args)
        else:
            raise InvalidConfigError("No CDF credentials")

        client_config = ClientConfig(
            project=self.project,
            base_url=self.host,
            client_name=client_name,
            timeout=self.timeout,
            credentials=credential_provider,
        )

        if use_experimental_sdk:
            from cognite.experimental import CogniteClient as ExperimentalCogniteClient

            return ExperimentalCogniteClient(client_config)

        return CogniteClient(client_config)

    def get_data_set(self, cdf_client: CogniteClient) -> Optional[DataSet]:
        if self.data_set_external_id:
            logging.getLogger(__name__).warning(
                "Using data-set-external-id is deprecated, please use data-set-id/external-id instead"
            )
            return cdf_client.data_sets.retrieve(external_id=self.data_set_external_id)

        if self.data_set_id:
            logging.getLogger(__name__).warning("Using data-set-id is deprecated, please use data-set/id instead")
            return cdf_client.data_sets.retrieve(external_id=self.data_set_external_id)

        if not self.data_set:
            return None

        return cdf_client.data_sets.retrieve(
            id=self.data_set.either_id.internal_id, external_id=self.data_set.either_id.external_id
        )

    def get_extraction_pipeline(self, cdf_client: CogniteClient) -> Optional[ExtractionPipeline]:
        if not self.extraction_pipeline:
            return None

        either_id = self.extraction_pipeline.either_id
        extraction_pipeline = cdf_client.extraction_pipelines.retrieve(
            id=either_id.internal_id,
            external_id=either_id.external_id,
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

    def setup_logging(self, suppress_console=False) -> None:
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

        if self.metrics:
            export_log_stats_on_root_logger(root)

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

    clear_after: Optional[int]
    push_interval: int = 30


class _PromServerConfig:
    port: int = 9000
    host: str = "0.0.0.0"


@dataclass
class _CogniteMetricsConfig:
    external_id_prefix: str
    asset_name: Optional[str]
    asset_external_id: Optional[str]

    push_interval: int = 30


@dataclass
class MetricsConfig:
    """
    Destination(s) for metrics, including options for one or several Prometheus push gateways, and pushing as CDF Time
    Series.
    """

    push_gateways: Optional[List[_PushGatewayConfig]]
    cognite: Optional[_CogniteMetricsConfig]
    server: Optional[_PromServerConfig]

    def start_pushers(self, cdf_client: CogniteClient, cancelation_token: Event = Event()) -> None:
        self._pushers: List[AbstractMetricsPusher] = []
        self._clear_on_stop: Dict[PrometheusPusher, int] = {}

        push_gateways = self.push_gateways or []

        for counter, push_gateway in enumerate(push_gateways):
            pusher = PrometheusPusher(
                job_name=push_gateway.job_name,
                username=push_gateway.username,
                password=push_gateway.password,
                url=push_gateway.host,
                push_interval=push_gateway.push_interval,
                thread_name=f"MetricsPusher_{counter}",
                cancelation_token=cancelation_token,
            )

            pusher.start()
            self._pushers.append(pusher)
            if push_gateway.clear_after is not None:
                self._clear_on_stop[pusher] = push_gateway.clear_after

        if self.cognite:
            asset = None

            if self.cognite.asset_name is not None:
                asset = Asset(name=self.cognite.asset_name, external_id=self.cognite.asset_external_id)

            pusher = CognitePusher(
                cdf_client=cdf_client,
                external_id_prefix=self.cognite.external_id_prefix,
                push_interval=self.cognite.push_interval,
                asset=asset,
                thread_name="CogniteMetricsPusher",  # There is only one Cognite project as a target
                cancelation_token=cancelation_token,
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
    upload_interval: int = 30


@dataclass
class LocalStateStoreConfig:
    path: str
    save_interval: int = 30


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
                save_interval=self.raw.upload_interval,
            )

        if self.local:
            return LocalStateStore(file_path=self.local.path, save_interval=self.local.save_interval)

        if default_to_local:
            return LocalStateStore(file_path="states.json")
        else:
            return NoStateStore()


CustomConfigClass = TypeVar("CustomConfigClass", bound=BaseConfig)


def _load_yaml(
    source: Union[TextIO, str],
    config_type: Type[CustomConfigClass],
    case_style: str = "hyphen",
    expand_envvars=True,
    dict_manipulator: Callable[[Dict[str, Any]], Dict[str, Any]] = lambda x: x,
) -> CustomConfigClass:
    def env_constructor(_: yaml.SafeLoader, node):
        bool_values = {
            "true": True,
            "false": False,
        }
        expanded_value = os.path.expandvars(node.value)
        return bool_values.get(expanded_value.lower(), expanded_value)

    class EnvLoader(yaml.SafeLoader):
        pass

    EnvLoader.add_implicit_resolver("!env", re.compile(r"\$\{([^}^{]+)\}"), None)
    EnvLoader.add_constructor("!env", env_constructor)

    loader = EnvLoader if expand_envvars else yaml.SafeLoader

    # Safe to use load instead of safe_load since both loader classes are based on SafeLoader
    try:
        config_dict = yaml.load(source, Loader=loader)
    except ScannerError as e:
        location = e.problem_mark or e.context_mark
        formatted_location = f" at line {location.line+1}, column {location.column+1}" if location is not None else ""
        cause = e.problem or e.context
        raise InvalidConfigError(f"Invalid YAML{formatted_location}: {cause or ''}") from e

    config_dict = dict_manipulator(config_dict)
    config_dict = _to_snake_case(config_dict, case_style)

    try:
        config = dacite.from_dict(
            data=config_dict, data_class=config_type, config=dacite.Config(strict=True, cast=[Enum])
        )
    except dacite.UnexpectedDataError as e:
        unknowns = [f'"{k.replace("_", "-") if case_style == "hyphen" else k}"' for k in e.keys]
        raise InvalidConfigError(f"Unknown config parameter{'s' if len(unknowns) > 1 else ''} {', '.join(unknowns)}")

    except (dacite.WrongTypeError, dacite.MissingValueError, dacite.UnionMatchError) as e:
        path = e.field_path.replace("_", "-") if case_style == "hyphen" else e.field_path

        def name(type_: Type) -> str:
            return type_.__name__ if hasattr(type_, "__name__") else str(type_)

        def all_types(type_: Type) -> Iterable[Type]:
            return type_.__args__ if hasattr(type_, "__args__") else [type_]

        if isinstance(e, (dacite.WrongTypeError, dacite.UnionMatchError)) and e.value is not None:
            got_type = name(type(e.value))
            need_type = ", ".join(name(t) for t in all_types(e.field_type))

            raise InvalidConfigError(
                f'Wrong type for field "{path}" - got "{e.value}" of type {got_type} instead of {need_type}'
            )
        raise InvalidConfigError(f'Missing mandatory field "{path}"')

    except dacite.ForwardReferenceError as e:
        raise ValueError(f"Invalid config class: {str(e)}")

    config._file_hash = sha256(json.dumps(config_dict).encode("utf-8")).hexdigest()

    return config


def load_yaml(
    source: Union[TextIO, str], config_type: Type[CustomConfigClass], case_style: str = "hyphen", expand_envvars=True
) -> CustomConfigClass:
    """
    Read a YAML file, and create a config object based on its contents.

    Args:
        source: Input stream (as returned by open(...)) or string containing YAML.
        config_type: Class of config type (i.e. your custom subclass of BaseConfig).
        case_style: Casing convention of config file. Valid options are 'snake', 'hyphen' or 'camel'. Should be
            'hyphen'.
        expand_envvars: Substitute values with the pattern ${VAR} with the content of the environment variable VAR

    Returns:
        An initialized config object.

    Raises:
        InvalidConfigError: If any config field is given as an invalid type, is missing or is unknown
    """
    return _load_yaml(source=source, config_type=config_type, case_style=case_style, expand_envvars=expand_envvars)


T = TypeVar("T", bound=BaseConfig)


class ConfigResolver(Generic[T]):
    def __init__(self, config_path: str, config_type: Type[T]):
        self.config_path = config_path
        self.config_type = config_type

        self._config: Optional[T] = None
        self._next_config: Optional[T] = None

    def _reload_file(self):
        with open(self.config_path, "r") as stream:
            self._config_text = stream.read()

    @property
    def is_remote(self) -> bool:
        raw_config_type = yaml.safe_load(self._config_text).get("type")
        if raw_config_type is None:
            _logger.warning("No config type specified, default to local")
            raw_config_type = "local"
        config_type = ConfigType(raw_config_type)
        return config_type == ConfigType.REMOTE

    @property
    def has_changed(self) -> bool:
        try:
            self._resolve_config()
        except Exception as e:
            _logger.exception("Failed to reload configuration file")
            return False
        return self._config._file_hash != self._next_config._file_hash

    @property
    def config(self) -> T:
        if self._config is None:
            self._resolve_config()
            self.accept_new_config()
        return self._config

    def accept_new_config(self) -> None:
        self._config = self._next_config

    @classmethod
    def from_cli(cls, name: str, description: str, version: str, config_type: Type[T]) -> "ConfigResolver":
        argument_parser = argparse.ArgumentParser(sys.argv[0], description=description)
        argument_parser.add_argument(
            "config", nargs=1, type=str, help="The YAML file containing configuration for the extractor."
        )
        argument_parser.add_argument("-v", "--version", action="version", version=f"{name} v{version}")
        args = argument_parser.parse_args()

        return cls(args.config[0], config_type)

    def _inject_cognite(self, local_part: _BaseConfig, remote_part: Dict[str, Any]) -> Dict[str, Any]:
        if "cognite" not in remote_part:
            remote_part["cognite"] = {}

        if local_part.cognite.idp_authentication is not None:
            remote_part["cognite"]["idp-authentication"] = {
                "client_id": local_part.cognite.idp_authentication.client_id,
                "scopes": local_part.cognite.idp_authentication.scopes,
                "secret": local_part.cognite.idp_authentication.secret,
                "tenant": local_part.cognite.idp_authentication.tenant,
                "token_url": local_part.cognite.idp_authentication.token_url,
                "resource": local_part.cognite.idp_authentication.resource,
                "authority": local_part.cognite.idp_authentication.authority,
            }
        if local_part.cognite.api_key is not None:
            remote_part["cognite"]["api-key"] = local_part.cognite.api_key
        if local_part.cognite.host is not None:
            remote_part["cognite"]["host"] = local_part.cognite.host
        remote_part["cognite"]["project"] = local_part.cognite.project
        remote_part["cognite"]["extraction-pipeline"] = {}
        remote_part["cognite"]["extraction-pipeline"]["id"] = local_part.cognite.extraction_pipeline.id
        remote_part["cognite"]["extraction-pipeline"][
            "external_id"
        ] = local_part.cognite.extraction_pipeline.external_id

        return remote_part

    def _resolve_config(self) -> None:
        self._reload_file()

        if self.is_remote:
            _logger.debug("Loading remote config file")
            tmp_config: _BaseConfig = load_yaml(self._config_text, _BaseConfig)
            client = tmp_config.cognite.get_cognite_client("config_resolver")
            response = client.extraction_pipelines.config.retrieve(
                tmp_config.cognite.get_extraction_pipeline(client).external_id
            )

            self._next_config = _load_yaml(
                source=response.config,
                config_type=self.config_type,
                dict_manipulator=lambda d: self._inject_cognite(tmp_config, d),
            )

        else:
            _logger.debug("Loading local config file")
            self._next_config = load_yaml(self._config_text, self.config_type)
