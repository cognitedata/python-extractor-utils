from __future__ import annotations

import random
import string
import time
from datetime import datetime, timezone
from typing import Literal

import pytest
from conftest import ETestType, ParamTest

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import NodeApply, NodeId
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import CogniteExtractorTimeSeriesApply
from cognite.extractorutils.uploader.time_series import CDMTimeSeriesUploadQueue

MIN_DATAPOINT_TIMESTAMP = -2208988800000
MAX_DATAPOINT_STRING_LENGTH = 255
MAX_DATAPOINT_VALUE = 1e100
MIN_DATAPOINT_VALUE = -1e100


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

    queue = CDMTimeSeriesUploadQueue(cdf_client=client)
    queue.start()

    ts_apply = _apply_node(params.space, ext_id, time_series_type="numeric")

    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)
    datapoints_1 = _rand_numeric_points(5, now)
    datapoints_2 = _rand_numeric_points(5, now + 1000)

    queue.add_to_upload_queue(timeseries_apply=ts_apply, datapoints=datapoints_1)
    queue.add_to_upload_queue(timeseries_apply=ts_apply, datapoints=datapoints_2)
    queue.upload()

    time.sleep(5)

    recv_points = client.time_series.data.retrieve(
        instance_id=NodeId(params.space, ext_id), start="1w-ago", end="now", limit=None
    )

    assert [int(p) for p in recv_points.value] == [p[1] for p in datapoints_1 + datapoints_2]
    queue.stop()


def test_cdm_queue_single_series_string(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    """
    Create one string CDM time-series, push datapoints, upload, and verify.
    """
    client, params = set_upload_test
    ext_id = params.external_ids[1]

    queue = CDMTimeSeriesUploadQueue(cdf_client=client)
    queue.start()

    ts_apply = _apply_node(params.space, ext_id, time_series_type="string")

    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)
    datapoints = _rand_string_points(6, now)

    queue.add_to_upload_queue(timeseries_apply=ts_apply, datapoints=datapoints)
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

    queue = CDMTimeSeriesUploadQueue(cdf_client=client)

    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)

    dp_a = _rand_numeric_points(4, now)
    dp_b = _rand_string_points(3, now)

    queue.add_to_upload_queue(timeseries_apply=_apply_node(params.space, ext_a, "numeric"), datapoints=dp_a)
    queue.add_to_upload_queue(timeseries_apply=_apply_node(params.space, ext_b, "string"), datapoints=dp_b)
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

    queue = CDMTimeSeriesUploadQueue(cdf_client=client)
    queue.start()

    ts_apply_numeric = _apply_node(params.space, ext_id_1, "numeric")
    ts_apply_string = _apply_node(params.space, ext_id_2, "string")
    now = int(datetime.now(tz=timezone.utc).timestamp() * 1_000)
    good = (now, 123)
    bad_time = (MIN_DATAPOINT_TIMESTAMP - 1, 9)
    bad_val_inf = (now + 1_000, float("inf"))
    bad_val_max = (now + 1_000, MAX_DATAPOINT_VALUE * 10)
    bad_val_min = (now + 1_000, MIN_DATAPOINT_VALUE * 10)
    too_long_str = (now + 2_000, "x" * (MAX_DATAPOINT_STRING_LENGTH + 1))

    queue.add_to_upload_queue(
        timeseries_apply=ts_apply_numeric, datapoints=[good, bad_time, bad_val_inf, bad_val_max, bad_val_min]
    )
    queue.add_to_upload_queue(timeseries_apply=ts_apply_string, datapoints=[too_long_str])
    queue.upload()
    time.sleep(5)

    recv_points_1 = client.time_series.data.retrieve(instance_id=NodeId(params.space, ext_id_1))

    recv_points_2 = client.time_series.data.retrieve(instance_id=NodeId(params.space, ext_id_2))

    assert [int(v) for v in recv_points_1.value] == [good[1]]
    assert [int(v) for v in recv_points_2.value] == []
    queue.stop()
