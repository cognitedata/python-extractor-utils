import logging
from typing import Any, Dict, List, Optional

from .util import MockLogger


class DictValidator:
    """
    docstring for DictValidator
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        super(DictValidator, self).__init__()

        self._required_keys: List[Any] = []
        self._optional_keys: List[Any] = []
        self._known_keys: List[Any] = []
        self._require_if_present: Dict[Any, List[Any]] = {}
        self._require_only_if_present: Dict[Any, List[Any]] = {}
        self._require_if_value: Dict[Any, Dict[Any, List[Any]]] = {}
        self._require_only_if_value: Dict[Any, Dict[Any, List[Any]]] = {}
        self._defaults: Dict[Any, Any] = {}

        if logger is not None:
            self.logger = logger
        else:
            # Create a mock logger so later calls to logger.info ... doesn't fail
            self.logger = MockLogger()  # type: ignore

    def __call__(self, dictionary: Dict[Any, Any]):
        return self.validate(dictionary)

    def add_required_keys(self, key_list: List[Any]):
        self._required_keys.extend(key_list)

    def add_optional_keys(self, key_list: List[Any]):
        self._optional_keys.extend(key_list)

    def add_known_keys(self, key_list: List[Any]):
        self._known_keys.extend(key_list)

    def require_if_present(self, base_key: Any, required_keys: List[Any]):
        if base_key in self._require_if_present:
            self._require_if_present[base_key].extend(required_keys)
        else:
            self._require_if_present[base_key] = required_keys

    def require_only_if_present(self, base_key: Any, required_keys: List[Any]):
        if base_key in self._require_if_present:
            self._require_only_if_present[base_key].extend(required_keys)
        else:
            self._require_only_if_present[base_key] = required_keys

    def require_if_value(self, base_key: Any, base_value: Any, required_keys: List[Any]):
        if base_key in self._require_if_present and base_value in self._require_if_present:
            self._require_if_value[base_key][base_value].extend(required_keys)
        elif base_key in self._require_if_value:
            self._require_if_value[base_key][base_value] = required_keys
        else:
            self._require_if_value[base_key] = {base_value: required_keys}

    def require_only_if_value(self, base_key: Any, base_value: Any, required_keys: List[Any]):
        if base_key in self._require_only_if_value and base_value in self._require_only_if_value[base_key]:
            self._require_only_if_value[base_key][base_value].extend(required_keys)
        elif base_key in self._require_only_if_value:
            self._require_only_if_value[base_key][base_value] = required_keys
        else:
            self._require_only_if_value[base_key] = {base_value: required_keys}

    def set_default(self, key, default_value):
        if not key in self._optional_keys:
            self._optional_keys.append(key)

        self._defaults[key] = default_value

    def _get_all_known_keys(self):
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

    def validate(self, dictionary: Dict[Any, Any]):
        is_ok = True

        for key in dictionary:
            if not key in self._get_all_known_keys():
                self.logger.warning("Unknown key: '%s'.", str(key))

        for key in self._required_keys:
            if not key in dictionary:
                self.logger.error("Required key '%s' is missing.", str(key))
                is_ok = False

        for key in self._optional_keys:
            if not key in dictionary:
                if key in self._defaults:
                    self.logger.warning("No '%s' specified, defaulting to '%s'.", str(key), str(self._defaults[key]))
                else:
                    self.logger.warning("No '%s' specified.", key)

        for base_key in self._require_if_present:
            for key in self._require_if_present[base_key]:
                if not key in dictionary:
                    self.logger.error("Missing key '%s' required by '%s'.", str(key), str(base_key))
                    is_ok = False

        for base_key in self._require_only_if_present:
            for key in self._require_only_if_present[base_key]:
                if key in dictionary and not base_key in dictionary:
                    self.logger.warning("Key '%s' is only required when key '%s' is present.", key, base_key)
                if not key in dictionary and base_key in dictionary:
                    self.logger.error("Key '%s' is required when key '%s' is present.", key, base_key)
                    is_ok = False

        for base_key in self._require_if_value:
            for base_value in self._require_if_value[base_key]:
                if dictionary[base_key] == base_value:
                    for key in self._require_if_value[base_key][base_value]:
                        if not key in dictionary:
                            self.logger.error(
                                "Missing key '%s' is required when key '%s' is set to '%s'.", key, base_key, base_value
                            )
                            is_ok = False

        for base_key in self._require_only_if_value:
            for base_value in self._require_only_if_value[base_key]:
                if dictionary[base_key] == base_value:
                    for key in self._require_only_if_value[base_key][base_value]:
                        if not key in dictionary:
                            self.logger.error(
                                "Missing key '%s' is required when key '%s' is set to '%s'.", key, base_key, base_value
                            )
                            is_ok = False
                else:
                    for key in self._require_only_if_value[base_key][base_value]:
                        if key in dictionary:
                            self.logger.warning(
                                "Key '%s' is only required when key '%s' is set to '%s'.", key, base_key, base_value
                            )

        return is_ok
