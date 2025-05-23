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
import datetime
from typing import Literal
from unittest.mock import Mock, patch

from cognite.client.data_classes import Row
from cognite.client.data_classes.data_modeling import NodeApply, NodeId
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import CogniteExtractorTimeSeriesApply
from cognite.extractorutils.uploader import (
    CDMTimeSeriesUploadQueue,
    EventUploadQueue,
    RawUploadQueue,
    TimeSeriesUploadQueue,
)
from cognite.extractorutils.uploader_extractor import UploaderExtractor, UploaderExtractorConfig
from cognite.extractorutils.uploader_types import Event, InsertCDMDatapoints, InsertDatapoints, RawRow


@patch("cognite.client.CogniteClient")
def test_handle_events(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    ex = UploaderExtractor[UploaderExtractorConfig](
        name="ext_extractor1", description="description", config_class=UploaderExtractorConfig
    )
    ex.event_queue = EventUploadQueue(client)

    # Single
    evt = Event(external_id="some-event")
    ex.handle_output(evt)

    ex.event_queue.upload()
    client.events.create.assert_called_with([evt])

    # Iterable
    evts = [Event(external_id="some-event"), Event(external_id="some-other-event")]
    ex.handle_output(evts)

    ex.event_queue.upload()
    client.events.create.assert_called_with(evts)


@patch("cognite.client.CogniteClient")
def test_handle_raw_rows(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    ex = UploaderExtractor[UploaderExtractorConfig](
        name="ext_extractor2", description="description", config_class=UploaderExtractorConfig
    )
    ex.raw_queue = RawUploadQueue(client)

    # Single
    r = Row()
    row = RawRow(db_name="some-db", table_name="some-table", row=r)
    ex.handle_output(row)

    ex.raw_queue.upload()
    client.raw.rows.insert.assert_called_with(db_name="some-db", table_name="some-table", row=[r], ensure_parent=True)

    # Iterable
    r2 = Row()
    rows = [
        RawRow(db_name="some-db", table_name="some-table", row=r),
        RawRow(db_name="some-db", table_name="some-table", row=r2),
    ]
    ex.handle_output(rows)

    ex.raw_queue.upload()
    client.raw.rows.insert.assert_called_with(db_name="some-db", table_name="some-table", row=[r], ensure_parent=True)
    client.raw.rows.insert.assert_called_with(db_name="some-db", table_name="some-table", row=[r2], ensure_parent=True)


@patch("cognite.client.CogniteClient")
def test_handle_timeseries(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    ex = UploaderExtractor[UploaderExtractorConfig](
        name="ext_extractor3", description="description", config_class=UploaderExtractorConfig
    )
    ex.time_series_queue = TimeSeriesUploadQueue(client)

    start: float = datetime.datetime.now().timestamp() * 1000.0

    # Single
    ts = InsertDatapoints(external_id="some-id", datapoints=[(start, 100)])
    ex.handle_output(ts)

    ex.time_series_queue.upload()
    client.time_series.data.insert_multiple.assert_called_with(
        [
            {"externalId": "some-id", "datapoints": ts.datapoints},
        ]
    )
    # Iterable
    tss = [
        InsertDatapoints(external_id="some-id", datapoints=[(start, 100), (start + 1, 101)]),
        InsertDatapoints(external_id="some-other-id", datapoints=[(start + 2, 102), (start + 3, 103)]),
    ]
    ex.handle_output(tss)

    ex.time_series_queue.upload()
    client.time_series.data.insert_multiple.assert_called_with(
        [
            {"externalId": "some-id", "datapoints": tss[0].datapoints},
            {"externalId": "some-other-id", "datapoints": tss[1].datapoints},
        ]
    )


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


def _make_apply_result(node_id: NodeId) -> Mock:
    fake_node = Mock()
    fake_node.as_id.return_value = node_id  # .nodes[0].as_id()
    result = Mock()
    result.nodes = [fake_node]  # .nodes[0]
    return result


@patch("cognite.client.CogniteClient")
def test_handle_cdm_timeseries(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()
    ex = UploaderExtractor[UploaderExtractorConfig](
        name="ext_extractor4",
        description="description",
        config_class=UploaderExtractorConfig,
    )
    ex.cdm_time_series_queue = CDMTimeSeriesUploadQueue(client)
    start: float = datetime.datetime.now().timestamp() * 1000.0

    # single
    node_single = NodeId("some-space", "some-id")
    client.data_modeling.instances.apply.return_value = _make_apply_result(node_single)

    ts_apply_single = _apply_node("some-space", "some-id", time_series_type="numeric")
    item_single = InsertCDMDatapoints(timeseries_apply=ts_apply_single, datapoints=[(start, 100)])

    ex.handle_output(item_single)
    ex.cdm_time_series_queue.upload()

    client.time_series.data.insert_multiple.assert_called_with(
        [{"instanceId": node_single, "datapoints": item_single.datapoints}]
    )

    # iterable
    node_iter1 = NodeId("some-space", "some-id")
    node_iter2 = NodeId("some-space", "some-other-id")

    client.data_modeling.instances.apply.side_effect = [
        _make_apply_result(node_iter1),
        _make_apply_result(node_iter2),
    ]

    ts_apply_1 = _apply_node("some-space", "some-id", time_series_type="numeric")
    ts_apply_2 = _apply_node("some-space", "some-other-id", time_series_type="numeric")

    item1 = InsertCDMDatapoints(timeseries_apply=ts_apply_1, datapoints=[(start, 100), (start + 1, 101)])
    item2 = InsertCDMDatapoints(timeseries_apply=ts_apply_2, datapoints=[(start + 2, 102), (start + 3, 103)])

    iterable_items = [item1, item2]
    ex.handle_output(iterable_items)

    ex.cdm_time_series_queue.upload()

    client.time_series.data.insert_multiple.assert_called_with(
        [
            {"instanceId": node_iter1, "datapoints": item1.datapoints},
            {"instanceId": node_iter2, "datapoints": item2.datapoints},
        ]
    )
