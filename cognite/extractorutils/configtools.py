"""
Module containing tools for loading and verifying config files.
"""

import logging
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, T, TextIO, Tuple, Type, Union

import dacite
import yaml

from cognite.client import CogniteClient

from ._inner_util import _MockLogger

_logger = logging.getLogger(__name__)


class InvalidConfigError(Exception):
    """
    Exception thrown from ``load_yaml`` if config file is invalid. This can be due to

      * Missing fields
      * Incompatible types
      * Unkown fields
    """

    def __init__(self, message: str):
        super(InvalidConfigError, self).__init__()
        self.message = message

    def __str__(self) -> str:
        return f"Invalid config: {self.message}"

    def __repr__(self) -> str:
        return self.__str__()


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

    def fix_dict(dictionary, key_translator):
        new_dict = {}
        for key in dictionary:
            if isinstance(dictionary[key], dict):
                new_dict[key_translator(key)] = fix_dict(dictionary[key], key_translator)
            else:
                new_dict[key_translator(key)] = dictionary[key]
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
    api_key: str
    external_id_prefix: str = ""
    host: str = "https://api.cognitedata.com"

    def get_cognite_client(self, client_name: str) -> CogniteClient:
        return CogniteClient(api_key=self.api_key, project=self.project, base_url=self.host, client_name=client_name)


@dataclass
class _ConsoleLoggingConfig:
    level: str


@dataclass
class _FileLoggingConfig:
    level: str
    path: str


@dataclass
class LoggingConfig:
    """
    Logging settings, such as log levels and path to log file
    """

    console: Optional[_ConsoleLoggingConfig]
    file: Optional[_FileLoggingConfig]


@dataclass
class _PushGatewayConfig:
    host: str
    job_name: str
    username: str
    password: str

    push_interval: int


@dataclass
class _CogniteMetricsConfig:
    external_id_prefix: str
    asset_name: str
    asset_external_id: str

    push_interval: int


@dataclass
class MetricsConfig:
    """
    Destination(s) for metrics, including options for one or several Prometheus push gateways, and pushing as CDF Time
    Series.
    """

    push_gateways: Optional[List[_PushGatewayConfig]]
    cognite: Optional[_CogniteMetricsConfig]


@dataclass
class BaseConfig:
    """
    Basis for an extractor config, containing config version, ``CogniteConfig`` and ``LoggingConfig``
    """

    version: str

    cognite: CogniteConfig
    logger: LoggingConfig


@dataclass
class BaseWithMetricsConfig(BaseConfig):
    """
    An extension of ``BaseConfig`` including ``MetricsConfig``
    """

    metrics: MetricsConfig


class DictValidator:
    """
    DEPRECATED. Will be removed in version 1.0.0. Move to class based configs instead.

    A class for validating dictionaries.

    Args:
        logger (logging.Logger):    (Optional). A logger object to write warnings and errors to to during validation.
                                    Defaults to no logger (ie an instance of the MockLogger in the util module).
        log_prefix:           (Optional). A prefix to add to each log string. Default is no prefix.
        log_suffix:           (Optional). A suffix to add to each log string. Default is no suffix.

    """

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        log_prefix: Optional[str] = None,
        log_suffix: Optional[str] = None,
    ):
        super(DictValidator, self).__init__()

        self._required_keys: List[Any] = []
        self._optional_keys: List[Any] = []
        self._known_keys: List[Any] = []
        self._require_if_present: Dict[Any, List[Any]] = {}
        self._require_only_if_present: Dict[Any, List[Any]] = {}
        self._require_if_value: Dict[Any, Dict[Any, List[Any]]] = {}
        self._require_only_if_value: Dict[Any, Dict[Any, List[Any]]] = {}
        self._defaults: Dict[Any, Any] = {}
        self._legal_values: Dict[Any, List[Any]] = {}

        self.log_prefix = log_prefix if log_prefix is not None else ""
        self.log_suffix = log_suffix if log_suffix is not None else ""

        if logger is not None:
            self.logger = logger
        else:
            # Create a mock logger so later calls to logger.info ... doesn't fail
            self.logger = _MockLogger()  # type: ignore

    def __call__(self, dictionary: Dict[Any, Any], apply_defaults=True) -> bool:
        """
        Calls the validate method.

        Args:
            dictionary:      Dictionary to vaildate.
            apply_defaults (bool):  Wether to add the available defaults to optional keys, or simply warn about them
                                    not existing.

        Returns:
            bool: verification of dictionary
        """
        return self.validate(dictionary, apply_defaults)

    def add_required_keys(self, key_list: List[Any]):
        """
        Add a list of keys that must be present in the dictionary

        Args:
            key_list (list):    List of keys to add to the set of required keys.
        """
        self._required_keys.extend(key_list)

    def add_optional_keys(self, key_list: List[Any]):
        """
        Add a list of keys that should be present, but doesn't have to be. Unlike required keys, the validation will
        still pass if some of these keys are missing, but it will produce a warning.

        Args:
            key_list (list):    List of keys to add to the set of optional keys.
        """
        self._optional_keys.extend(key_list)

    def add_known_keys(self, key_list: List[Any]):
        """
        Add a list of possible keys in the dictionary that are not required or optional. Unknown keys (ie, not
        required, required_if_*, optional or known) are warned about during validation. Unlike optional keys, if known
        keys are missing, it does not produce a warning.

        Args:
            key_list (list):    List of keys to add to the set of known keys.
        """
        self._known_keys.extend(key_list)

    def require_if_present(self, base_key: Any, required_keys: List[Any]):
        """
        Require the precence of certain keys if given key is present in the dictionary.

        Args:
            base_key (Any):         Base key.
            required_keys (list):   List of keys to require if base key is present in the dictionary.
        """
        if base_key in self._require_if_present:
            self._require_if_present[base_key].extend(required_keys)
        else:
            self._require_if_present[base_key] = required_keys

    def require_only_if_present(self, base_key: Any, required_keys: List[Any]):
        """
        Require the precence of certain keys only if given key is present in the dictionary. Unlike require_if_present,
        this produces a warning if any of the required keys are present when the base key is not.

        Args:
            base_key (Any):         Base key.
            required_keys (list):   List of keys to require only if base key is present in the dictionary.
        """
        if base_key in self._require_if_present:
            self._require_only_if_present[base_key].extend(required_keys)
        else:
            self._require_only_if_present[base_key] = required_keys

    def require_if_value(self, base_key: Any, base_value: Any, required_keys: List[Any]):
        """
        Require the precence of certain keys if given key has the given value in the dictionary.

        Args:
            base_key (Any):         Base key.
            base_value (Any):       Base value.
            required_keys (list):   List of keys to require if base key has base value.
        """
        if base_key in self._require_if_present and base_value in self._require_if_present:
            self._require_if_value[base_key][base_value].extend(required_keys)
        elif base_key in self._require_if_value:
            self._require_if_value[base_key][base_value] = required_keys
        else:
            self._require_if_value[base_key] = {base_value: required_keys}

    def require_only_if_value(self, base_key: Any, base_value: Any, required_keys: List[Any]):
        """
        Require the precence of certain keys only if given key has the given value. Unlike require_if_value, this
        produces a warning if any of the required keys are present when the base key does not have the given value.

        Args:
            base_key (Any):         Base key.
            base_value (Any):       Base value.
            required_keys (list):   List of keys to require only if base key has base value.
        """
        if base_key in self._require_only_if_value and base_value in self._require_only_if_value[base_key]:
            self._require_only_if_value[base_key][base_value].extend(required_keys)
        elif base_key in self._require_only_if_value:
            self._require_only_if_value[base_key][base_value] = required_keys
        else:
            self._require_only_if_value[base_key] = {base_value: required_keys}

    def set_default(self, key: Any, default_value: Any):
        """
        Set the default value of optional keys. If key is not already specified as an optional key, it is added. This
        changes the warning message, and depending on the args to validate, adds it to the dictionary.

        Args:
            key (Any):              Key.
            default_value (Any):    Default value of key.
        """
        if not key in self._optional_keys:
            self._optional_keys.append(key)

        self._defaults[key] = default_value

    def set_legal_values(self, key: Any, legal_values: List[Any]):
        """
        Restrict range of possible values for a key. If the key is previously unknown, it is added to the set of known
        keys. Illegal values of a key produces an error and fails to validate.

        Args:
            key (Any):              Key.
            legal_values (list):    All the legal values for key.
        """
        if not key in self.get_all_known_keys():
            self._known_keys.append(key)

        self._legal_values[key] = legal_values

    def get_all_known_keys(self):
        """
        Returns a set of all the known keys (ie, required, required_if_*, optional or known keys).

        Returns:
            set: All keys known by the validator.
        """
        keys = set(
            self._required_keys
            + self._optional_keys
            + self._known_keys
            + list(self._require_if_present.keys())
            + list(self._require_only_if_present.keys())
            + list(self._require_if_value.keys())
            + list(self._require_only_if_value.keys())
        )

        for req in self._require_if_present:
            keys = keys.union(self._require_if_present[req])
        for req in self._require_only_if_present:
            keys = keys.union(self._require_only_if_present[req])
        for req in self._require_if_value:
            keys = keys.union(*list(self._require_if_value[req].values()))
        for req in self._require_only_if_value:
            keys = keys.union(*list(self._require_only_if_value[req].values()))

        return keys

    def validate(self, dictionary: Dict[Any, Any], apply_defaults: bool = True):
        """
        Performs the verification. Checks if the given dictionary satisfies the given requirements.

        Args:
            dictionary:      Dictionary to vaildate.
            apply_defaults (bool):  Wether to add the available defaults to optional keys, or simply warn about them
                                    not existing.

        Returns:
            bool: verification of dictionary
        """
        is_ok = True

        for key in dictionary:
            if not key in self.get_all_known_keys():
                self.logger.warning("%sUnknown key: '%s'.%s", self.log_prefix, str(key), self.log_suffix)

        for key in self._required_keys:
            if not key in dictionary:
                self.logger.error("%sRequired key '%s' is missing.%s", self.log_prefix, str(key), self.log_suffix)
                is_ok = False

        for key in self._optional_keys:
            if not key in dictionary:
                if key in self._defaults:
                    self.logger.warning(
                        "%sNo '%s' specified, defaulting to '%s'.%s",
                        self.log_prefix,
                        str(key),
                        str(self._defaults[key]),
                        self.log_suffix,
                    )
                    if apply_defaults:
                        dictionary[key] = self._defaults[key]
                else:
                    self.logger.warning("%sNo '%s' specified.%s", self.log_prefix, str(key), self.log_suffix)

        for base_key in self._require_if_present:
            for key in self._require_if_present[base_key]:
                if not key in dictionary and base_key in dictionary:
                    self.logger.error(
                        "%sMissing key '%s' required by '%s'.%s",
                        self.log_prefix,
                        str(key),
                        str(base_key),
                        self.log_suffix,
                    )
                    is_ok = False

        for base_key in self._require_only_if_present:
            for key in self._require_only_if_present[base_key]:
                if key in dictionary and not base_key in dictionary:
                    self.logger.warning(
                        "%s'%s' is only required when key '%s' is present.%s",
                        self.log_prefix,
                        str(key),
                        str(base_key),
                        self.log_suffix,
                    )
                if not key in dictionary and base_key in dictionary:
                    self.logger.error(
                        "%s'%s' is required when key '%s' is present.%s",
                        self.log_prefix,
                        str(key),
                        str(base_key),
                        self.log_suffix,
                    )
                    is_ok = False

        for base_key in self._require_if_value:
            for base_value in self._require_if_value[base_key]:
                if dictionary.get(base_key) == base_value:
                    for key in self._require_if_value[base_key][base_value]:
                        if not key in dictionary:
                            self.logger.error(
                                "%s'%s' is required when '%s' is set to '%s'.%s",
                                self.log_prefix,
                                key,
                                base_key,
                                base_value,
                                self.log_suffix,
                            )
                            is_ok = False

        for base_key in self._require_only_if_value:
            for base_value in self._require_only_if_value[base_key]:
                if dictionary.get(base_key) == base_value:
                    for key in self._require_only_if_value[base_key][base_value]:
                        if not key in dictionary:
                            self.logger.error(
                                "%s'%s' is required when '%s' is set to '%s'.%s",
                                self.log_prefix,
                                key,
                                base_key,
                                base_value,
                                self.log_suffix,
                            )
                            is_ok = False
                else:
                    for key in self._require_only_if_value[base_key][base_value]:
                        if key in dictionary:
                            self.logger.warning(
                                "%s'%s' is only required when '%s' is set to '%s'.%s",
                                self.log_prefix,
                                key,
                                base_key,
                                base_value,
                                self.log_suffix,
                            )

        for key in self._legal_values:
            if key in dictionary and dictionary[key] not in self._legal_values[key]:
                self.logger.error(
                    "%s'%s' is not a valid value for key '%s'%s",
                    self.log_prefix,
                    str(dictionary[key]),
                    str(key),
                    self.log_suffix,
                )
                is_ok = False

        return is_ok


def import_missing(from_dict: Dict[Any, Any], to_dict: Dict[Any, Any], keys: Optional[Iterable[Any]] = None):
    """
    DEPRECATED. Will be removed in version 1.0.0. Move to class based configs instead.

    Import missing key/value pairs from one dictionary to another.

    Args:
        from_dict:   Dictionary to copy from
        to_dict:     Dictionary to copy to (in place)
        keys (Iterable):    (Optional). If present, only the key/value pairs corresponding to these keys will be copied.
    """
    if keys is None:
        keys = from_dict.keys()

    for key in keys:
        if key in from_dict and key not in to_dict:
            to_dict[key] = from_dict[key]


def recursive_none_check(collection: Any) -> Tuple[bool, Any]:
    """
    DEPRECATED. Will be removed in version 1.0.0. Move to class based configs instead.

    Returns true if any value in the dictionary tree is None. That is, if any value in the given dictionary is None, or
    if any lists or dictionaries as values in this dictionary contains None, and so on.

    Args:
        collection:  Dictionary to check

    Returns:
        Tuple[bool, Any]: True if any value is None, and the local index / key of the value that is None
    """

    if isinstance(collection, dict):
        for key in collection:
            if collection[key] is None:
                return True, key

            res = recursive_none_check(collection[key])
            if res[0]:
                return res

    elif isinstance(collection, list):
        for i, element in enumerate(collection):
            if element is None:
                return True, i

            res = recursive_none_check(collection[i])
            if res[0]:
                return res

    return False, None
