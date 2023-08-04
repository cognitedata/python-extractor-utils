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
import json
import logging
import os
import re
import sys
from enum import Enum
from hashlib import sha256
from typing import Any, Callable, Dict, Generic, Iterable, Optional, TextIO, Type, TypeVar, Union

import dacite
import yaml
from yaml.scanner import ScannerError

from cognite.extractorutils.configtools._util import _to_snake_case
from cognite.extractorutils.configtools.elements import BaseConfig, ConfigType, TimeIntervalConfig, _BaseConfig
from cognite.extractorutils.exceptions import InvalidConfigError

_logger = logging.getLogger(__name__)


CustomConfigClass = TypeVar("CustomConfigClass", bound=BaseConfig)


def _load_yaml(
    source: Union[TextIO, str],
    config_type: Type[CustomConfigClass],
    case_style: str = "hyphen",
    expand_envvars: bool = True,
    dict_manipulator: Callable[[Dict[str, Any]], Dict[str, Any]] = lambda x: x,
) -> CustomConfigClass:
    def env_constructor(_: yaml.SafeLoader, node: yaml.Node) -> bool:
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
        config_dict = yaml.load(source, Loader=loader)  # noqa: S506
    except ScannerError as e:
        location = e.problem_mark or e.context_mark
        formatted_location = f" at line {location.line+1}, column {location.column+1}" if location is not None else ""
        cause = e.problem or e.context
        raise InvalidConfigError(f"Invalid YAML{formatted_location}: {cause or ''}") from e

    config_dict = dict_manipulator(config_dict)
    config_dict = _to_snake_case(config_dict, case_style)

    try:
        config = dacite.from_dict(
            data=config_dict, data_class=config_type, config=dacite.Config(strict=True, cast=[Enum, TimeIntervalConfig])
        )
    except dacite.UnexpectedDataError as e:
        unknowns = [f'"{k.replace("_", "-") if case_style == "hyphen" else k}"' for k in e.keys]
        raise InvalidConfigError(f"Unknown config parameter{'s' if len(unknowns) > 1 else ''} {', '.join(unknowns)}")

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
            )
        raise InvalidConfigError(f'Missing mandatory field "{path}"')

    except dacite.ForwardReferenceError as e:
        raise ValueError(f"Invalid config class: {str(e)}")

    config._file_hash = sha256(json.dumps(config_dict).encode("utf-8")).hexdigest()

    return config


def load_yaml(
    source: Union[TextIO, str],
    config_type: Type[CustomConfigClass],
    case_style: str = "hyphen",
    expand_envvars: bool = True,
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


class ConfigResolver(Generic[CustomConfigClass]):
    def __init__(self, config_path: str, config_type: Type[CustomConfigClass]):
        self.config_path = config_path
        self.config_type = config_type

        self._config: Optional[CustomConfigClass] = None
        self._next_config: Optional[CustomConfigClass] = None

    def _reload_file(self) -> None:
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
        if "cognite" not in remote_part:
            remote_part["cognite"] = {}

        remote_part["cognite"]["idp-authentication"] = {
            "client_id": local_part.cognite.idp_authentication.client_id,
            "scopes": local_part.cognite.idp_authentication.scopes,
            "secret": local_part.cognite.idp_authentication.secret,
            "tenant": local_part.cognite.idp_authentication.tenant,
            "token_url": local_part.cognite.idp_authentication.token_url,
            "resource": local_part.cognite.idp_authentication.resource,
            "authority": local_part.cognite.idp_authentication.authority,
        }
        if local_part.cognite.host is not None:
            remote_part["cognite"]["host"] = local_part.cognite.host
        remote_part["cognite"]["project"] = local_part.cognite.project

        # Ignoring None type, extraction pipelines is required at this point
        remote_part["cognite"]["extraction-pipeline"] = {}
        remote_part["cognite"]["extraction-pipeline"]["id"] = local_part.cognite.extraction_pipeline.id  # type: ignore
        remote_part["cognite"]["extraction-pipeline"][
            "external_id"
        ] = local_part.cognite.extraction_pipeline.external_id  # type: ignore

        return remote_part

    def _resolve_config(self) -> None:
        self._reload_file()

        if self.is_remote:
            _logger.debug("Loading remote config file")
            tmp_config: _BaseConfig = load_yaml(self._config_text, _BaseConfig)  # type: ignore
            client = tmp_config.cognite.get_cognite_client("config_resolver")
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
            )

        else:
            _logger.debug("Loading local config file")
            self._next_config = load_yaml(self._config_text, self.config_type)
