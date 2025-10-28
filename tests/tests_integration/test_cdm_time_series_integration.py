from __future__ import annotations

import random
import string
import time
from datetime import datetime, timezone
from typing import Literal

import pytest
from _pytest.logging import LogCaptureFixture
from conftest import ETestType, ParamTest

from cognite.client import CogniteClient
from cognite.client.data_classes import StatusCode
from cognite.client.data_classes.data_modeling import NodeApply, NodeId
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import CogniteExtractorTimeSeriesApply
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.extractorutils.uploader.time_series import (
    MAX_DATAPOINT_STRING_BYTES,
    MAX_DATAPOINT_VALUE,
    MIN_DATAPOINT_TIMESTAMP,
    MIN_DATAPOINT_VALUE,
    CDMTimeSeriesUploadQueue,
)


@pytest.fixture
def set_test_parameters() -> ParamTest:
    rnd = random.randint(0, 2**31)
    ext_ids = [f"util_integration_cdm_ts_{i}-{rnd}" for i in range(3)]
    return ParamTest(
        test_type=ETestType.CDM_TIME_SERIES,
        external_ids=ext_ids,
        space="ExtractorUtilsTests",
    )


def _rand_numeric_points(n: int, start_ms: int) -> list[tuple[int, int]]:
    """Return n datapoints with increasing timestamps."""
    return [(start_ms + i * 101, random.randint(0, 1_000)) for i in range(n)]


def _rand_string_points(n: int, start_ms: int) -> list[tuple[int, str]]:
    """Return n string datapoints."""
    letters = string.ascii_letters + string.digits
    return [(start_ms + i * 97, "".join(random.choices(letters, k=12))) for i in range(n)]


def _apply_node(space: str, external_id: str, time_series_type: Literal["numeric", "string"]) -> NodeApply:
    """
    Create a minimal CogniteExtractorTimeSeriesApply that results in a real time-series in CDF.
    """
    return CogniteExtractorTimeSeriesApply(
        space=space,
        external_id=external_id,
        time_series_type=time_series_type,
        is_step=False,
    )


def test_cdm_queue_single_series_numeric(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    """
    Create one numeric CDM time-series, push datapoints, upload, and verify.
    """
    client, params = set_upload_test
    ext_id = params.external_ids[0]

    queue = CDMTimeSeriesUploadQueue(cdf_client=client, create_missing=True)
    queue.start()

    instance_id = NodeId(space=params.space, external_id=ext_id)

    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)
    datapoints_1 = _rand_numeric_points(5, now)
    datapoints_2 = _rand_numeric_points(5, now + 1000)

    queue.add_to_upload_queue(instance_id=instance_id, datapoints=datapoints_1)
    queue.add_to_upload_queue(instance_id=instance_id, datapoints=datapoints_2)
    queue.upload()

    time.sleep(5)

    recv_points = client.time_series.data.retrieve(
        instance_id=NodeId(params.space, ext_id), start="1w-ago", end="now", limit=None
    )

    assert [int(p) for p in recv_points.value] == [p[1] for p in datapoints_1 + datapoints_2]
    queue.stop()


def test_create_missing_false_failure(
    set_upload_test: tuple[CogniteClient, ParamTest], caplog: LogCaptureFixture
) -> None:
    """
    Tests that if create_missing=False and TS doesn't exist, it's not created,
    an error is logged, and data is not uploaded.
    """
    client, params = set_upload_test
    ext_id = params.external_ids[0]

    queue = CDMTimeSeriesUploadQueue(cdf_client=client)
    queue.start()

    instance_id = NodeId(space=params.space, external_id=ext_id)

    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)
    datapoints_1 = _rand_numeric_points(5, now)
    datapoints_2 = _rand_numeric_points(5, now + 1000)

    queue.add_to_upload_queue(instance_id=instance_id, datapoints=datapoints_1)
    queue.add_to_upload_queue(instance_id=instance_id, datapoints=datapoints_2)
    queue.upload()

    time.sleep(5)
    queue.stop()

    assert "Could not upload data points to" in caplog.text
    assert f"'space': '{params.space}', 'externalId': '{ext_id}'" in caplog.text

    with pytest.raises(CogniteNotFoundError):
        client.time_series.data.retrieve(
            instance_id=NodeId(space=params.space, external_id=ext_id), start="1w-ago", end="now"
        )


def test_cdm_queue_single_series_string(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    """
    Create one string CDM time-series, push datapoints, upload, and verify.
    """
    client, params = set_upload_test
    ext_id = params.external_ids[1]

    queue = CDMTimeSeriesUploadQueue(cdf_client=client, create_missing=True)
    queue.start()

    instance_id = NodeId(space=params.space, external_id=ext_id)

    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)
    datapoints = _rand_string_points(6, now)

    queue.add_to_upload_queue(instance_id=instance_id, datapoints=datapoints)
    queue.upload()
    time.sleep(5)

    recv_points = client.time_series.data.retrieve(
        instance_id=NodeId(params.space, ext_id), start="1w-ago", end="now", limit=None
    )

    assert list(recv_points.value) == [dp[1] for dp in datapoints]
    queue.stop()


def test_cdm_queue_multiple_series_batched(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    """
    Push datapoints to two​ CDM time-series and ensure the uploader batches them.
    """
    client, params = set_upload_test
    ext_a, ext_b = params.external_ids[:2]

    queue = CDMTimeSeriesUploadQueue(cdf_client=client, create_missing=True)

    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)

    dp_a = _rand_numeric_points(4, now)
    dp_b = _rand_string_points(3, now)

    queue.add_to_upload_queue(instance_id=NodeId(params.space, ext_a), datapoints=dp_a)
    queue.add_to_upload_queue(instance_id=NodeId(params.space, ext_b), datapoints=dp_b)
    queue.upload()
    time.sleep(5)

    rec_a = client.time_series.data.retrieve(
        instance_id=NodeId(params.space, ext_a), start=now - 1_000, end="now", limit=None
    )

    rec_b = client.time_series.data.retrieve(
        instance_id=NodeId(params.space, ext_b), start=now - 1_000, end="now", limit=None
    )

    assert [int(v) for v in rec_a.value] == [dp[1] for dp in dp_a]
    assert list(rec_b.value) == [dp[1] for dp in dp_b]


def test_cdm_queue_discards_invalid_values(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    """
    Feed a mix of valid + invalid datapoints and verify only the valid one survives.
    """
    client, params = set_upload_test
    ext_id_1 = params.external_ids[1]
    ext_id_2 = params.external_ids[2]

    queue = CDMTimeSeriesUploadQueue(cdf_client=client, create_missing=True)
    queue.start()

    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)
    good = (now, 123)
    bad_time = (MIN_DATAPOINT_TIMESTAMP - 1, 9)
    bad_val_inf = (now + 1_000, float("inf"))
    bad_val_max = (now + 1_000, MAX_DATAPOINT_VALUE * 10)
    bad_val_min = (now + 1_000, MIN_DATAPOINT_VALUE * 10)
    too_long_str = (now + 2_000, "x" * (MAX_DATAPOINT_STRING_BYTES + 1))
    valid_temp_str_dp = (now + 3_000, "valid_short_string")

    queue.add_to_upload_queue(
        instance_id=NodeId(params.space, ext_id_1), datapoints=[good, bad_time, bad_val_inf, bad_val_max, bad_val_min]
    )
    queue.add_to_upload_queue(instance_id=NodeId(params.space, ext_id_2), datapoints=[too_long_str, valid_temp_str_dp])
    queue.upload()
    time.sleep(5)

    recv_points_1 = client.time_series.data.retrieve(instance_id=NodeId(params.space, ext_id_1))

    recv_points_2 = client.time_series.data.retrieve(instance_id=NodeId(params.space, ext_id_2))

    assert [int(v) for v in recv_points_1.value] == [good[1]]
    assert [str(v) for v in recv_points_2.value] == [valid_temp_str_dp[1]]
    assert too_long_str[1] not in recv_points_2.value
    queue.stop()


@pytest.mark.parametrize(
    "ts_type, invalid_dps_function, expected_error_msg",
    [
        ("numeric", _rand_string_points, "Expected numeric value"),
        ("string", _rand_numeric_points, "Expected string value"),
    ],
    ids=["string_into_numeric_fails", "numeric_into_string_fails"],
)
def test_cdm_queue_fails_on_data_type_mismatch(
    set_upload_test: tuple[CogniteClient, ParamTest],
    ts_type: Literal["numeric", "string"],
    invalid_dps_function: callable,
    expected_error_msg: str,
) -> None:
    """
    Tests that the uploader fails with a specific error when the datapoint type
    does not match the time series type. This test is parametrized to cover:
    1. Ingesting string datapoints into a numeric time series.
    2. Ingesting numeric datapoints into a string time series.
    """
    client, params = set_upload_test
    ext_id = params.external_ids[0]
    instance_id = NodeId(space=params.space, external_id=ext_id)

    node = _apply_node(params.space, ext_id, ts_type)
    client.data_modeling.instances.apply(nodes=node)
    time.sleep(2)

    queue = CDMTimeSeriesUploadQueue(cdf_client=client, create_missing=False)
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)
    invalid_dps = invalid_dps_function(n=5, start_ms=now)
    queue.add_to_upload_queue(instance_id=instance_id, datapoints=invalid_dps)

    with pytest.raises(CogniteAPIError) as excinfo:
        queue.upload()

    assert excinfo.value.code == 400
    assert expected_error_msg in excinfo.value.message

    retrieved_dps = client.time_series.data.retrieve(instance_id=instance_id)
    assert len(retrieved_dps) == 0


def test_cdm_queue_with_status_codes(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    """
    Tests ingesting datapoints that include status codes into a numeric CDM time series.
    """
    client, params = set_upload_test

    queue = CDMTimeSeriesUploadQueue(cdf_client=client, create_missing=True)
    queue.start()

    start = int(datetime.now(tz=timezone.utc).timestamp() * 1000) - 5_000

    statuses = [
        StatusCode.Good,
        StatusCode.Uncertain,
        StatusCode.Bad,
        3145728,  # GoodClamped
    ]

    points1 = [(start + i * 42, random.random(), random.choice(statuses)) for i in range(30)]
    queue.add_to_upload_queue(
        instance_id=NodeId(space=params.space, external_id=params.external_ids[0]), datapoints=points1
    )

    points2 = [(start + i * 24, random.random(), random.choice(statuses)) for i in range(50)]
    queue.add_to_upload_queue(
        instance_id=NodeId(space=params.space, external_id=params.external_ids[1]), datapoints=points2
    )

    queue.upload()
    time.sleep(5)

    recv_points1 = client.time_series.data.retrieve(
        instance_id=NodeId(space=params.space, external_id=params.external_ids[0]),
        start=start - 100,
        end="now",
        limit=None,
        include_status=True,
        treat_uncertain_as_bad=False,
        ignore_bad_datapoints=False,
    )

    recv_points2 = client.time_series.data.retrieve(
        instance_id=NodeId(space=params.space, external_id=params.external_ids[1]),
        start=start - 100,
        end="now",
        limit=None,
        include_status=True,
        treat_uncertain_as_bad=False,
        ignore_bad_datapoints=False,
    )
    queue.stop()

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
