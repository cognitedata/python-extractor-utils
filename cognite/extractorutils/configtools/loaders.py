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

import argparse
import dataclasses
import json
import logging
import os
import re
import sys
from enum import Enum
from hashlib import sha256
from pathlib import Path
from typing import Any, Callable, Dict, Generic, Iterable, List, Optional, TextIO, Type, TypeVar, Union, cast

import dacite
import yaml
from azure.core.credentials import TokenCredential
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError, ServiceRequestError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from yaml.scanner import ScannerError

from cognite.client import CogniteClient
from cognite.extractorutils.configtools._util import _to_snake_case
from cognite.extractorutils.configtools.elements import (
    BaseConfig,
    CastableInt,
    ConfigType,
    IgnorePattern,
    PortNumber,
    TimeIntervalConfig,
    _BaseConfig,
)
from cognite.extractorutils.exceptions import InvalidConfigError

_logger = logging.getLogger(__name__)


CustomConfigClass = TypeVar("CustomConfigClass", bound=BaseConfig)


class KeyVaultAuthenticationMethod(Enum):
    DEFAULT = "default"
    CLIENTSECRET = "client-secret"


class KeyVaultLoader:
    """
    Class responsible for configuring keyvault for clients using Azure
    """

    def __init__(self, config: Optional[dict]):
        self.config = config

        self.credentials: Optional[TokenCredential] = None
        self.client: Optional[SecretClient] = None

    def _init_client(self) -> None:
        from dotenv import find_dotenv, load_dotenv

        if not self.config:
            raise InvalidConfigError(
                "Attempted to load values from Azure key vault with no key vault configured. "
                "Include an `azure-keyvault` section in your config to use the !keyvault tag."
            )

        keyvault_name = self.config.get("keyvault-name")
        if not keyvault_name:
            raise InvalidConfigError("Please add the keyvault-name")

        if "authentication-method" not in self.config:
            raise InvalidConfigError(
                "Please enter the authentication method to access Azure KeyVault"
                "Possible values are: default or client-secret"
            )

        vault_url = f"https://{keyvault_name}.vault.azure.net"

        if self.config["authentication-method"] == KeyVaultAuthenticationMethod.DEFAULT.value:
            _logger.info("Using Azure DefaultCredentials to access KeyVault")
            self.credentials = DefaultAzureCredential()

        elif self.config["authentication-method"] == KeyVaultAuthenticationMethod.CLIENTSECRET.value:
            auth_parameters = ("client-id", "tenant-id", "secret")

            _logger.info("Using Azure ClientSecret credentials to access KeyVault")

            dotenv_path = find_dotenv(usecwd=True)
            load_dotenv(dotenv_path=dotenv_path, override=True)

            if all(param in self.config for param in auth_parameters):
                tenant_id = os.path.expandvars(self.config.get("tenant-id", None))
                client_id = os.path.expandvars(self.config.get("client-id", None))
                secret = os.path.expandvars(self.config.get("secret", None))

                self.credentials = ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=secret,
                )
            else:
                raise InvalidConfigError(
                    "Missing client secret parameters. client-id, tenant-id and client-secret are mandatory"
                )
        else:
            raise InvalidConfigError(
                "Invalid KeyVault authentication method. Possible values : default or client-secret"
            )

        self.client = SecretClient(vault_url=vault_url, credential=self.credentials)  # type: ignore

    def __call__(self, _: yaml.SafeLoader, node: yaml.Node) -> str:
        self._init_client()
        try:
            return self.client.get_secret(node.value).value  # type: ignore  # _init_client guarantees not None
        except (ResourceNotFoundError, ServiceRequestError, HttpResponseError) as e:
            raise InvalidConfigError(str(e)) from e


class _EnvLoader(yaml.SafeLoader):
    pass


class SafeLoaderIgnoreUnknown(yaml.SafeLoader):
    def ignore_unknown(self, node: yaml.Node) -> None:
        return None


def _env_constructor(_: yaml.SafeLoader, node: yaml.Node) -> bool:
    bool_values = {
        "true": True,
        "false": False,
    }
    expanded_value = os.path.expandvars(node.value)
    return bool_values.get(expanded_value.lower(), expanded_value)


def _load_yaml_dict_raw(
    source: Union[TextIO, str],
    expand_envvars: bool = True,
    keyvault_loader: Optional[KeyVaultLoader] = None,
) -> Dict[str, Any]:
    loader = _EnvLoader if expand_envvars else yaml.SafeLoader

    class SafeLoaderIgnoreUnknown(yaml.SafeLoader):
        def ignore_unknown(self, node: yaml.Node) -> None:
            return None

        # Ignoring types since the key can be None.

    SafeLoaderIgnoreUnknown.add_constructor(None, SafeLoaderIgnoreUnknown.ignore_unknown)  # type: ignore
    initial_load = yaml.load(source, Loader=SafeLoaderIgnoreUnknown)  # noqa: S506

    if not isinstance(source, str):
        source.seek(0)

    if keyvault_loader:
        _EnvLoader.add_constructor("!keyvault", keyvault_loader)
    else:
        keyvault_config = initial_load.get("azure-keyvault", initial_load.get("key-vault"))
        _EnvLoader.add_constructor("!keyvault", KeyVaultLoader(keyvault_config))

    _EnvLoader.add_implicit_resolver("!env", re.compile(r"\$\{([^}^{]+)\}"), None)
    _EnvLoader.add_constructor("!env", _env_constructor)

    try:
        config_dict = yaml.load(source, Loader=loader)  # noqa: S506
    except ScannerError as e:
        location = e.problem_mark or e.context_mark
        formatted_location = f" at line {location.line+1}, column {location.column+1}" if location is not None else ""
        cause = e.problem or e.context
        raise InvalidConfigError(f"Invalid YAML{formatted_location}: {cause or ''}") from e

    return config_dict


def _load_yaml_dict(
    source: Union[TextIO, str],
    case_style: str = "hyphen",
    expand_envvars: bool = True,
    dict_manipulator: Callable[[Dict[str, Any]], Dict[str, Any]] = lambda x: x,
    keyvault_loader: Optional[KeyVaultLoader] = None,
) -> Dict[str, Any]:
    config_dict = _load_yaml_dict_raw(source, expand_envvars, keyvault_loader)

    config_dict = dict_manipulator(config_dict)
    config_dict = _to_snake_case(config_dict, case_style)

    if "azure_keyvault" in config_dict:
        config_dict.pop("azure_keyvault")
    if "key_vault" in config_dict:
        config_dict.pop("key_vault")

    return config_dict


def _load_yaml(
    source: Union[TextIO, str],
    config_type: Type[CustomConfigClass],
    case_style: str = "hyphen",
    expand_envvars: bool = True,
    dict_manipulator: Callable[[Dict[str, Any]], Dict[str, Any]] = lambda x: x,
    keyvault_loader: Optional[KeyVaultLoader] = None,
) -> CustomConfigClass:
    config_dict = _load_yaml_dict(
        source,
        case_style=case_style,
        expand_envvars=expand_envvars,
        dict_manipulator=dict_manipulator,
        keyvault_loader=keyvault_loader,
    )

    try:
        config = dacite.from_dict(
            data=config_dict,
            data_class=config_type,
            config=dacite.Config(strict=True, cast=[Enum, TimeIntervalConfig, Path, CastableInt, PortNumber]),
        )
    except dacite.UnexpectedDataError as e:
        unknowns = [f'"{k.replace("_", "-") if case_style == "hyphen" else k}"' for k in e.keys]
        raise InvalidConfigError(
            f"Unknown config parameter{'s' if len(unknowns) > 1 else ''} {', '.join(unknowns)}"
        ) from e

    except (dacite.WrongTypeError, dacite.MissingValueError, dacite.UnionMatchError) as e:
        if e.field_path:
            path = e.field_path.replace("_", "-") if case_style == "hyphen" else e.field_path
        else:
            path = None

        def name(type_: Type) -> str:
            return type_.__name__ if hasattr(type_, "__name__") else str(type_)

        def all_types(type_: Type) -> Iterable[Type]:
            return type_.__args__ if hasattr(type_, "__args__") else [type_]

        if isinstance(e, (dacite.WrongTypeError, dacite.UnionMatchError)) and e.value is not None:
            got_type = name(type(e.value))
            need_type = ", ".join(name(t) for t in all_types(e.field_type))

            raise InvalidConfigError(
                f'Wrong type for field "{path}" - got "{e.value}" of type {got_type} instead of {need_type}'
            ) from e
        raise InvalidConfigError(f'Missing mandatory field "{path}"') from e

    except dacite.ForwardReferenceError as e:
        raise ValueError(f"Invalid config class: {str(e)}") from e

    config._file_hash = sha256(json.dumps(config_dict).encode("utf-8")).hexdigest()

    return config


def load_yaml(
    source: Union[TextIO, str],
    config_type: Type[CustomConfigClass],
    case_style: str = "hyphen",
    expand_envvars: bool = True,
    keyvault_loader: Optional[KeyVaultLoader] = None,
) -> CustomConfigClass:
    """
    Read a YAML file, and create a config object based on its contents.

    Args:
        source: Input stream (as returned by open(...)) or string containing YAML.
        config_type: Class of config type (i.e. your custom subclass of BaseConfig).
        case_style: Casing convention of config file. Valid options are 'snake', 'hyphen' or 'camel'. Should be
            'hyphen'.
        expand_envvars: Substitute values with the pattern ${VAR} with the content of the environment variable VAR
        keyvault_loader: Pre-built loader for keyvault tags. Will be loaded from config if not set.

    Returns:
        An initialized config object.

    Raises:
        InvalidConfigError: If any config field is given as an invalid type, is missing or is unknown
    """
    return _load_yaml(
        source=source,
        config_type=config_type,
        case_style=case_style,
        expand_envvars=expand_envvars,
        keyvault_loader=keyvault_loader,
    )


def load_yaml_dict(
    source: Union[TextIO, str],
    case_style: str = "hyphen",
    expand_envvars: bool = True,
    keyvault_loader: Optional[KeyVaultLoader] = None,
) -> Dict[str, Any]:
    """
    Read a YAML file and return a dictionary from its contents

    Args:
        source: Input stream (as returned by open(...)) or string containing YAML.
        case_style: Casing convention of config file. Valid options are 'snake', 'hyphen' or 'camel'. Should be
            'hyphen'.
        expand_envvars: Substitute values with the pattern ${VAR} with the content of the environment variable VAR
        keyvault_loader: Pre-built loader for keyvault tags. Will be loaded from config if not set.

    Returns:
        A raw dict with the contents of the config file.

    Raises:
        InvalidConfigError: If any config field is given as an invalid type, is missing or is unknown
    """
    return _load_yaml_dict(
        source=source, case_style=case_style, expand_envvars=expand_envvars, keyvault_loader=keyvault_loader
    )


def compile_patterns(ignore_patterns: List[Union[str, IgnorePattern]]) -> list[re.Pattern[str]]:
    """
    List of patterns to compile

    Args:
        ignore_patterns: A list of strings or IgnorePattern to be compiled.

    Returns:
        A list of compiled RegExp patterns.
    """
    compiled = []
    for p in ignore_patterns:
        if isinstance(p, IgnorePattern):
            compiled.append(re.compile(p.compile()))
        else:
            compiled.append(re.compile(p))
    return compiled


class ConfigResolver(Generic[CustomConfigClass]):
    def __init__(self, config_path: str, config_type: Type[CustomConfigClass]):
        self.config_path = config_path
        self.config_type = config_type

        self._config: Optional[CustomConfigClass] = None
        self._next_config: Optional[CustomConfigClass] = None

        self._cognite_client: Optional[CogniteClient] = None

    def _reload_file(self) -> None:
        with open(self.config_path, "r") as stream:
            self._config_text = stream.read()

    @property
    def cognite_client(self) -> Optional[CogniteClient]:
        if self._cognite_client is None and self._config is not None:
            self._cognite_client = self._config.cognite.get_cognite_client("config_resolver")
        return self._cognite_client

    @cognite_client.setter
    def cognite_client(self, client: CogniteClient) -> None:
        if not isinstance(client, CogniteClient):
            raise AttributeError("cognite_client must be set to a CogniteClient instance")
        self._cognite_client = client

    @property
    def is_remote(self) -> bool:
        raw_config_type = load_yaml_dict(self._config_text).get("type")
        if raw_config_type is None:
            _logger.warning("No config type specified, default to local")
            raw_config_type = "local"
        config_type = ConfigType(raw_config_type)
        return config_type == ConfigType.REMOTE

    @property
    def has_changed(self) -> bool:
        try:
            self._resolve_config()
        except Exception:
            _logger.exception("Failed to reload configuration file")
            return False
        return self._config._file_hash != self._next_config._file_hash if self._config else True  # type: ignore

    @property
    def config(self) -> CustomConfigClass:
        if self._config is None:
            self._resolve_config()
            self.accept_new_config()
        return self._config  # type: ignore

    def accept_new_config(self) -> None:
        self._config = self._next_config

    @classmethod
    def from_cli(
        cls, name: str, description: str, version: str, config_type: Type[CustomConfigClass]
    ) -> "ConfigResolver":
        argument_parser = argparse.ArgumentParser(sys.argv[0], description=description)
        argument_parser.add_argument(
            "config", nargs=1, type=str, help="The YAML file containing configuration for the extractor."
        )
        argument_parser.add_argument("-v", "--version", action="version", version=f"{name} v{version}")
        args = argument_parser.parse_args()

        return cls(args.config[0], config_type)

    def _inject_cognite(self, local_part: _BaseConfig, remote_part: Dict[str, Any]) -> Dict[str, Any]:
        # We can not dump 'local_part.cognite' directly because e.g. 'data_set' may be set remote only...
        remote_part.setdefault("cognite", {})
        remote_part["cognite"]["idp_authentication"] = dataclasses.asdict(local_part.cognite.idp_authentication)
        remote_part["cognite"]["extraction-pipeline"] = dataclasses.asdict(
            local_part.cognite.extraction_pipeline  # type: ignore [arg-type]
        )

        if local_part.cognite.host is not None:
            remote_part["cognite"]["host"] = local_part.cognite.host
        remote_part["cognite"]["project"] = local_part.cognite.project

        return remote_part

    def _use_cached_cognite_client(self, tmp_config: _BaseConfig) -> bool:
        # Ideally we'd check tmp_config == self._config, but due to 'is_remote & _inject_...', this is not
        # reliable to avoid new unneeded instantiations of CogniteClient:
        return (
            self.cognite_client is not None
            and self._config is not None
            and tmp_config.cognite.host == self._config.cognite.host
            and tmp_config.cognite.project == self._config.cognite.project
            and tmp_config.cognite.idp_authentication == self._config.cognite.idp_authentication
        )

    def _get_keyvault_loader(self) -> KeyVaultLoader:
        temp_config = _load_yaml_dict_raw(self._config_text)
        return KeyVaultLoader(temp_config.get("azure-keyvault", temp_config.get("key-vault")))

    def _resolve_config(self) -> None:
        self._reload_file()

        if self.is_remote:
            _logger.debug("Loading remote config file")
            tmp_config: _BaseConfig = load_yaml(self._config_text, _BaseConfig)  # type: ignore
            if self._use_cached_cognite_client(tmp_config):
                # Use existing client to avoid invoking a token refresh, if possible. Reason: this is run every 5 min
                # by default ('ConfigReloader' thread) which for certain OAuth providers like Auth0, incurs a cost:
                client = cast(CogniteClient, self.cognite_client)
            else:
                # Credentials towards CDF may have changed, instantiate (and store) a new client:
                client = self.cognite_client = tmp_config.cognite.get_cognite_client("config_resolver")

            response = client.extraction_pipelines.config.retrieve(
                tmp_config.cognite.get_extraction_pipeline(client).external_id  # type: ignore  # ignoring extpipe None
            )

            if response.config is None:
                _logger.error("No config included in response from extraction pipelines")
                return

            self._next_config = _load_yaml(
                source=response.config,
                config_type=self.config_type,
                dict_manipulator=lambda d: self._inject_cognite(tmp_config, d),
                keyvault_loader=self._get_keyvault_loader(),
            )

        else:
            _logger.debug("Loading local config file")
            self._next_config = load_yaml(self._config_text, self.config_type)
