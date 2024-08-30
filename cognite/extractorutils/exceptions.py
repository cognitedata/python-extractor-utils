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


from typing import List, Optional


class InvalidConfigError(Exception):
    """
    Exception thrown from ``load_yaml`` and ``load_yaml_dict`` if config file is invalid. This can be due to

      * Missing fields
      * Incompatible types
      * Unkown fields
    """

    def __init__(self, message: str, details: Optional[List[str]] = None):
        super(InvalidConfigError, self).__init__()
        self.message = message
        self.details = details

    def __str__(self) -> str:
        return f"Invalid config: {self.message}"

    def __repr__(self) -> str:
        return self.__str__()
