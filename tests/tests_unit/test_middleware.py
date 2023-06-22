#  Copyright 2022 Cognite AS
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
import unittest

from cognite.client.data_classes import Row
from cognite.extractorutils.middleware import JQMiddleware


class TestMiddlewareClass(unittest.TestCase):
    jq_rules = """
            {
                "foo": ._foo,
                "bar": ._bar,
                "baz": ._baz,
                "new_col": "some value"
            }
        """
    data = {"_foo": "foo", "_bar": "bar", "_baz": "baz"}

    expected_result = {"foo": "foo", "bar": "bar", "baz": "baz", "new_col": "some value"}

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.mw = JQMiddleware(jq_rules=self.jq_rules)

    def test_jq_mw_dict(self):
        actual_result = self.mw(self.data)
        self.assertDictEqual(actual_result, self.expected_result)

    def test_jq_mw_raw_row(self):
        raw_row = Row(key="dummy", columns=self.data)
        actual_result = self.mw(raw_row)
        self.assertDictEqual(self.expected_result, actual_result.columns)
