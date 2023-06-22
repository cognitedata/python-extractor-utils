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

import time
import unittest

from cognite.client.testing import monkeypatch_cognite_client
from cognite.extractorutils.util import add_extraction_pipeline


class TestExtractionPipelines(unittest.TestCase):
    with monkeypatch_cognite_client() as m_client:

        def test_work_as_expected(self):
            @add_extraction_pipeline(
                extraction_pipeline_ext_id="1",
                cognite_client=self.m_client,
            )
            def test_success():
                print("Starting function 'test_work_as_expected'")
                print("Stopping function 'test_work_as_expected'")

            test_success()

            print(f"{self.m_client.extraction_pipelines.runs.create.call_count=}")
            self.assertEqual(self.m_client.extraction_pipelines.runs.create.call_count, 2)

    with monkeypatch_cognite_client() as m2_client:

        def test_raise_error(self):
            @add_extraction_pipeline(
                extraction_pipeline_ext_id="2",
                cognite_client=self.m2_client,
            )
            def test_failure():
                print("Starting function 'test_raise_error'")
                raise Exception("Testing exceptions")
                print("Stopping function 'test_raise_error'")

            with self.assertRaises(Exception):
                test_failure()

    with monkeypatch_cognite_client() as m3_client:

        def test_2_heartbeats(self):
            @add_extraction_pipeline(
                extraction_pipeline_ext_id="3", cognite_client=self.m3_client, heartbeat_waiting_time=1
            )
            def test_success_2():
                print("Starting function 'test_2_heartbeats'")
                time.sleep(1.5)
                print("Stopping function 'test_2_heartbeats'")

            test_success_2()

            print(f"{self.m3_client.extraction_pipelines.runs.create.call_count=}")
            self.assertEqual(self.m3_client.extraction_pipelines.runs.create.call_count, 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
