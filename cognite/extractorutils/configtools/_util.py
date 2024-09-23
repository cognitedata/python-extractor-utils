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
import base64
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives import serialization as serialization
from cryptography.hazmat.primitives.serialization import pkcs12 as pkcs12
from cryptography.x509 import load_pem_x509_certificate

from cognite.extractorutils.exceptions import InvalidConfigError


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

    def fix_list(list_: List[Any], key_translator: Callable[[str], str]) -> List[Any]:
        if list_ is None:
            return []

        new_list: List[Any] = [None] * len(list_)
        for i, element in enumerate(list_):
            if isinstance(element, dict):
                new_list[i] = fix_dict(element, key_translator)
            elif isinstance(element, list):
                new_list[i] = fix_list(element, key_translator)
            else:
                new_list[i] = element
        return new_list

    def fix_dict(dict_: Dict[str, Any], key_translator: Callable[[str], str]) -> Dict[str, Any]:
        if dict_ is None:
            return {}

        new_dict: Dict[str, Any] = {}
        for key in dict_:
            if isinstance(dict_[key], dict):
                new_dict[key_translator(key)] = fix_dict(dict_[key], key_translator)
            elif isinstance(dict_[key], list):
                new_dict[key_translator(key)] = fix_list(dict_[key], key_translator)
            else:
                new_dict[key_translator(key)] = dict_[key]
        return new_dict

    def translate_hyphen(key: str) -> str:
        return key.replace("-", "_")

    def translate_camel(key: str) -> str:
        return re.sub(r"([A-Z]+)", r"_\1", key).strip("_").lower()

    if case_style == "snake" or case_style == "underscore":
        return dictionary
    elif case_style == "hyphen" or case_style == "kebab":
        return fix_dict(dictionary, translate_hyphen)
    elif case_style == "camel" or case_style == "pascal":
        return fix_dict(dictionary, translate_camel)
    else:
        raise ValueError(f"Invalid case style: {case_style}")


def _load_certificate_data(
    cert_path: str | Path, password: Optional[str]
) -> Union[Tuple[str, str], Tuple[bytes, bytes]]:
    path = Path(cert_path) if isinstance(cert_path, str) else cert_path
    cert_data = Path(path).read_bytes()

    if path.suffix == ".pem":
        cert = load_pem_x509_certificate(cert_data)
        private_key = serialization.load_pem_private_key(cert_data, password=password.encode() if password else None)
        private_key_str = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return base64.b16encode(cert.fingerprint(hashes.SHA1())), private_key_str
    elif path.suffix == ".pfx":
        (private_key_pfx, cert_pfx, _) = pkcs12.load_key_and_certificates(
            cert_data, password=password.encode() if password else None
        )

        if private_key_pfx is None:
            raise InvalidConfigError(f"Can't load private key from {cert_path}")
        if cert_pfx is None:
            raise InvalidConfigError(f"Can't load certificate from {cert_path}")

        private_key_str = private_key_pfx.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )
        return base64.b16encode(cert_pfx.fingerprint(hashes.SHA1())), private_key_str
    else:
        raise InvalidConfigError(f"Unknown certificate format '{path.suffix}'. Allowed formats are 'pem' and 'pfx'")
