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
Module containing tools for loading and verifying config files.
"""

import logging
import os
import re
import time
from dataclasses import dataclass
from logging.handlers import TimedRotatingFileHandler
from time import sleep
from typing import Any, Dict, List, Optional, T, TextIO, Type, Union

import dacite
import yaml

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset

from .authentication import Authenticator, AuthenticatorConfig
from .exceptions import InvalidConfigError
from .metrics import AbstractMetricsPusher, CognitePusher, PrometheusPusher
from .statestore import AbstractStateStore, LocalStateStore, NoStateStore, RawStateStore

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


def load_yaml(source: Union[TextIO, str], config_type: Type[T], case_style: str = "hyphen", expand_envvars=True) -> T:
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

    def env_constructor(_, node):
        # Expnadvars uses same syntax as our env var substitution
        return os.path.expandvars(node.value)

    class EnvLoader(yaml.SafeLoader):
        pass

    EnvLoader.add_implicit_resolver("!env", re.compile(r"\$\{([^}^{]+)\}"), None)
    EnvLoader.add_constructor("!env", env_constructor)

    loader = EnvLoader if expand_envvars else yaml.SafeLoader

    # Safe to use load instead of safe_load since both loader classes are based on SafeLoader
    config_dict = yaml.load(source, Loader=loader)

    config_dict = _to_snake_case(config_dict, case_style)

    try:
        config = dacite.from_dict(data=config_dict, data_class=config_type, config=dacite.Config(strict=True))
    except (dacite.WrongTypeError, dacite.MissingValueError, dacite.UnionMatchError, dacite.UnexpectedDataError) as e:
        raise InvalidConfigError(str(e))
    except dacite.ForwardReferenceError as e:
        raise ValueError(f"Invalid config class: {str(e)}")

    return config


@dataclass
class CogniteConfig:
    """
    Configuration parameters for CDF connection, such as project name, host address and API key
    """

    project: str
    api_key: Optional[str]
    idp_authentication: Optional[AuthenticatorConfig]
    data_set_id: Optional[int]
    external_id_prefix: str = ""
    host: str = "https://api.cognitedata.com"

    def get_cognite_client(self, client_name: str) -> CogniteClient:
        kwargs = {}
        if self.api_key:
            kwargs["api_key"] = self.api_key
        elif self.idp_authentication:
            authorizer = Authenticator(self.idp_authentication)
            kwargs["token"] = authorizer.get_token
        else:
            raise InvalidConfigError("No CDF credentials")

        return CogniteClient(
            project=self.project, base_url=self.host, client_name=client_name, disable_pypi_version_check=True, **kwargs
        )


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

    def setup_logging(self, suppress_console=False) -> None:
        """
        Sets up the default logger in the logging package to be configured as defined in this config object

        Args:
            suppress_console: Don't log to console regardless of config. Useful when running an extractor as a Windows
                service
        """
        fmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d UTC [%(levelname)-8s] %(threadName)s - %(message)s", "%Y-%m-%d %H:%M:%S",
        )
        # Set logging to UTC
        fmt.converter = time.gmtime

        root = logging.getLogger()

        if self.console and not suppress_console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self.console.level)
            console_handler.setFormatter(fmt)

            root.addHandler(console_handler)

            if root.getEffectiveLevel() > console_handler.level:
                root.setLevel(console_handler.level)

        if self.file:
            file_handler = TimedRotatingFileHandler(
                filename=self.file.path, when="D", utc=True, backupCount=self.file.retention,
            )
            file_handler.setLevel(self.file.level)
            file_handler.setFormatter(fmt)

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

    def start_pushers(self, cdf_client: CogniteClient) -> None:
        self._pushers: List[AbstractMetricsPusher] = []
        self._clear_on_stop: Dict[PrometheusPusher, int] = {}

        counter = 0

        push_gateways = self.push_gateways or []

        for push_gateway in push_gateways:
            pusher = PrometheusPusher(
                job_name=push_gateway.job_name,
                username=push_gateway.username,
                password=push_gateway.password,
                url=push_gateway.host,
                push_interval=push_gateway.push_interval,
                thread_name=f"MetricsPusher_{counter}",
            )

            pusher.start()
            self._pushers.append(pusher)
            if push_gateway.clear_after is not None:
                self._clear_on_stop[pusher] = push_gateway.clear_after
            counter += 1

        if self.cognite:
            if self.cognite.asset_name is not None:
                asset = Asset(name=self.cognite.asset_name, external_id=self.cognite.asset_external_id)
            else:
                asset = None

            pusher = CognitePusher(
                cdf_client=cdf_client,
                external_id_prefix=self.cognite.external_id_prefix,
                push_interval=self.cognite.push_interval,
                asset=asset,
                thread_name=f"MetricsPusher_{counter}",
            )

            pusher.start()
            self._pushers.append(pusher)

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


@dataclass
class BaseConfig:
    """
    Basis for an extractor config, containing config version, ``CogniteConfig`` and ``LoggingConfig``
    """

    version: Optional[Union[str, int]]

    cognite: CogniteConfig
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

            return RawStateStore(cdf_client=cdf_client, database=self.raw.database, table=self.raw.table)

        if self.local:
            return LocalStateStore(file_path=self.local.path)

        if default_to_local:
            return LocalStateStore(file_path="states.json")
        else:
            return NoStateStore()
