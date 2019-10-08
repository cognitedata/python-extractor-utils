"""
Module containing tools config verification and manipulation.
"""

import logging
from collections.abc import Iterable
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._inner_util import _MockLogger


class DictValidator:
    """
    A class for validating dictionaries.

    Args:
        logger (logging.Logger):    (Optional). A logger object to write warnings and errors to to during validation.
                                    Defaults to no logger (ie an instance of the MockLogger in the util module).
        log_prefix (str):           (Optional). A prefix to add to each log string. Default is no prefix.
        log_suffix (str):           (Optional). A suffix to add to each log string. Default is no suffix.

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
            dictionary (dict):      Dictionary to vaildate.
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
            dictionary (dict):      Dictionary to vaildate.
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
    Import missing key/value pairs from one dictionary to another.

    Args:
        from_dict (dict):   Dictionary to copy from
        to_dict (dict):     Dictionary to copy to (in place)
        keys (Iterable):    (Optional). If present, only the key/value pairs corresponding to these keys will be copied.
    """
    if keys is None:
        keys = from_dict.keys()

    for key in keys:
        if key in from_dict and key not in to_dict:
            to_dict[key] = from_dict[key]


def recursive_none_check(collection: Any) -> Tuple[bool, Any]:
    """
    Returns true if any value in the dictionary tree is None. That is, if any value in the given dictionary is None, or
    if any lists or dictionaries as values in this dictionary contains None, and so on.

    Args:
        collection (dict):  Dictionary to check

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
