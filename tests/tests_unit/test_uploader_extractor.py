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
from datetime import datetime, timezone
from unittest.mock import Mock, patch

from cognite.client.data_classes import Row
from cognite.extractorutils.uploader import EventUploadQueue, RawUploadQueue, TimeSeriesUploadQueue
from cognite.extractorutils.uploader_extractor import UploaderExtractor, UploaderExtractorConfig
from cognite.extractorutils.uploader_types import Event, InsertDatapoints, RawRow


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

    start: float = datetime.now(tz=timezone.utc).timestamp() * 1000.0

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
