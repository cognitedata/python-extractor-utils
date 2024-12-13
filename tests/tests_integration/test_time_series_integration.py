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

import random
import string
import time
from datetime import datetime, timezone
from typing import Any

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import StatusCode, TimeSeries
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


def test_time_series_upload_queue1(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    client, test_parameter = set_upload_test
    created = client.time_series.create(
        [
            TimeSeries(external_id=test_parameter.external_ids[0]),
            TimeSeries(external_id=test_parameter.external_ids[1], is_string=True),
        ]
    )

    last_point = {"timestamp": 0}

    def store_latest(points: Any) -> None:
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

    time.sleep(15)

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


def test_time_series_upload_queue2(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
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

    time.sleep(15)

    recv_points1 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[0], start="1w-ago", end="now", limit=None
    )

    assert [int(p) for p in recv_points1.value] == [p[1] for p in points1]

    queue.stop()


def test_time_series_upload_queue_create_missing(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
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


def test_time_seires_with_status(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    client, test_parameter = set_upload_test

    queue = TimeSeriesUploadQueue(cdf_client=client, create_missing=True)

    start = int(datetime.now(tz=timezone.utc).timestamp() * 1000) - 5_000

    # Create some data with status codes
    statuses = [
        StatusCode.Good,
        StatusCode.Uncertain,
        StatusCode.Bad,
        3145728,  # GoodClamped
    ]
    points1 = [(start + i * 42, random.random(), random.choice(statuses)) for i in range(30)]
    queue.add_to_upload_queue(external_id=test_parameter.external_ids[0], datapoints=points1)

    points2 = [(start + i * 24, random.random(), random.choice(statuses)) for i in range(50)]
    queue.add_to_upload_queue(external_id=test_parameter.external_ids[1], datapoints=points2)

    queue.upload()
    time.sleep(10)

    recv_points1 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[0],
        start=start - 100,
        end="now",
        limit=None,
        include_status=True,
        treat_uncertain_as_bad=False,
        ignore_bad_datapoints=False,
    )
    recv_points2 = client.time_series.data.retrieve(
        external_id=test_parameter.external_ids[1],
        start=start - 100,
        end="now",
        limit=None,
        include_status=True,
        treat_uncertain_as_bad=False,
        ignore_bad_datapoints=False,
    )

    assert len(recv_points1) == len(points1)
    assert len(recv_points2) == len(points2)

    for point, recv_point in zip(points1, recv_points1):  # noqa: B905
        assert point[0] == recv_point.timestamp
        assert point[1] == recv_point.value
        assert point[2] == recv_point.status_code

    for point, recv_point in zip(points2, recv_points2):  # noqa: B905
        assert point[0] == recv_point.timestamp
        assert point[1] == recv_point.value
        assert point[2] == recv_point.status_code
