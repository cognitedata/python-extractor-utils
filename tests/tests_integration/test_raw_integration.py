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

import os
import random
import time
from typing import Tuple

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.extractorutils.uploader import RawUploadQueue
from tests.conftest import TestParameter, TestType


@pytest.fixture
def set_test_parameters() -> TestParameter:
    test_id = random.randint(0, 2**31)
    test_parameter = TestParameter(test_type=TestType.RAW)
    test_parameter.database_name = "integrationTests"
    test_parameter.table_name = f"extractorUtils-{test_id}"
    return test_parameter


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_raw_upload_queue(set_upload_test: Tuple[CogniteClient, TestParameter], functions_runtime: str):
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = RawUploadQueue(cdf_client=client, max_queue_size=500)

    uploaded = []

    for i in range(500):
        r = Row("key{:03}".format(i), {"col": "val{}".format(i)})

        queue.add_to_upload_queue(test_parameter.database_name, test_parameter.table_name, r)
        uploaded.append(r)

    queue.upload()

    time.sleep(10)

    rows_in_cdf = sorted(
        client.raw.rows.list(db_name=test_parameter.database_name, table_name=test_parameter.table_name, limit=None),
        key=lambda row: row.key,
    )
    assert [{k: r.__dict__[k] for k in ["key", "columns"]} for r in uploaded] == [
        {k: r.__dict__[k] for k in ["key", "columns"]} for r in rows_in_cdf
    ]
