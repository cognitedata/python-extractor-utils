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
A module containing utilities meant for use inside the extractor-utils package
"""

import json
from decimal import Decimal
from typing import Any, Dict, Union


def _resolve_log_level(level: str) -> int:
    return {"NOTSET": 0, "DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}[level.upper()]


class _DecimalEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Dict[str, str]:
        if isinstance(obj, Decimal):
            return {"type": "decimal_encoded", "value": str(obj)}
        return super(_DecimalEncoder, self).default(obj)


class _DecimalDecoder(json.JSONDecoder):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        json.JSONDecoder.__init__(self, *args, object_hook=self.object_hook, **kwargs)

    def object_hook(self, obj_dict: Dict[str, str]) -> Union[Dict[str, str], Decimal]:
        if obj_dict.get("type") == "decimal_encoded":
            return Decimal(obj_dict["value"])
        return obj_dict
