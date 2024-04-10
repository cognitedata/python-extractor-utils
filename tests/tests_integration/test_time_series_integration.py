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
import string
import time
from datetime import datetime, timezone
from typing import Tuple

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.extractorutils.uploader import TimeSeriesUploadQueue
from tests.conftest import ETestType, ParamTest


@pytest.fixture
def set_test_parameters() -> ParamTest:
    test_id = random.randint(0, 2**31)
    test_parameter = ParamTest(test_type=ETestType.TIME_SERIES)
    test_parameter.external_ids = [
        f"util_integration_ts_test_1-{test_id}",
        f"util_integration_ts_test_2-{test_id}",
        f"util_integration_ts_test_3-{test_id}",
    ]
    return test_parameter


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_time_series_upload_queue1(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str):
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    created = client.time_series.create(
        [
            TimeSeries(external_id=test_parameter.external_ids[0]),
            TimeSeries(external_id=test_parameter.external_ids[1], is_string=True),
        ]
    )

    last_point = {"timestamp": 0}

    def store_latest(points):
        last_point["timestamp"] = max(last_point["timestamp"], *[ts["datapoints"][-1][0] for ts in points])

    queue = TimeSeriesUploadQueue(cdf_client=client, post_upload_function=store_latest, max_upload_interval=1)
    queue.start()

    # Create some synthetic data
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    points1_1 = [(now + i * 107, random.randint(0, 10)) for i in range(10)]
    points1_2 = [(now + i * 107, random.randint(0, 10)) for i in range(10, 100)]
    points2 = [(now + i * 93, chr(97 + i)) for i in range(26)]

    queue.add_to_upload_queue(external_id=test_parameter.external_ids[0], datapoints=points1_1)
    queue.add_to_upload_queue(external_id=test_parameter.external_ids[0], datapoints=points1_2)
    queue.add_to_upload_queue(id=created[1].id, datapoints=points2)

    time.sleep(30)

    recv_points1 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[0], start="1w-ago", end="now", limit=None
    )
    recv_points2 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[1], start="1w-ago", end="now", limit=None
    )

    assert [int(p) for p in recv_points1.value] == [p[1] for p in points1_1 + points1_2]
    assert recv_points2.value == [p[1] for p in points2]
    assert last_point["timestamp"] == points1_2[-1][0]

    queue.stop()


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_time_series_upload_queue2(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str):
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    client.time_series.create(TimeSeries(external_id=test_parameter.external_ids[0]))

    queue = TimeSeriesUploadQueue(cdf_client=client, max_upload_interval=1)
    queue.start()

    # Create some synthetic data
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    points1 = [(now + i * 107, random.randint(0, 10)) for i in range(10)]
    points2 = [(now + i * 107, random.randint(0, 10)) for i in range(10, 20)]

    queue.add_to_upload_queue(external_id=test_parameter.external_ids[0], datapoints=points1)
    queue.add_to_upload_queue(external_id="noSuchExternalId", datapoints=points2)

    time.sleep(20)

    recv_points1 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[0], start="1w-ago", end="now", limit=None
    )

    assert [int(p) for p in recv_points1.value] == [p[1] for p in points1]

    queue.stop()


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_time_series_upload_queue_create_missing(
    set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str
):
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test

    queue = TimeSeriesUploadQueue(cdf_client=client, create_missing=True)

    # Create some synthetic data
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    points1 = [(now + i * 107, random.randint(0, 10)) for i in range(10)]
    points2 = [(now + i * 107, "".join([random.choice(string.printable) for j in range(16)])) for i in range(10, 20)]
    points3 = [{"timestamp": now + i * 107, "value": random.randint(0, 10)} for i in range(10)]

    queue.add_to_upload_queue(external_id=test_parameter.external_ids[0], datapoints=points1)
    queue.add_to_upload_queue(external_id=test_parameter.external_ids[1], datapoints=points2)
    queue.add_to_upload_queue(external_id=test_parameter.external_ids[2], datapoints=points3)

    queue.upload()
    time.sleep(3)

    recv_points1 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[0], start="1w-ago", end="now", limit=None
    )
    recv_points2 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[1], start="1w-ago", end="now", limit=None
    )
    recv_points3 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[2], start="1w-ago", end="now", limit=None
    )

    assert [int(p) for p in recv_points1.value] == [p[1] for p in points1]
    assert [p for p in recv_points2.value] == [p[1] for p in points2]
    assert [int(p) for p in recv_points3.value] == [p["value"] for p in points3]

    queue.stop()
