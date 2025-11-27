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

import math
import time
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, BinaryIO
from unittest.mock import Mock, patch

from httpx import URL, Request

from cognite.client.data_classes import Event, FileMetadata, Row
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import (
    CogniteExtractorFileApply,
)
from cognite.client.testing import CogniteClientMock
from cognite.extractorutils.uploader import (
    EventUploadQueue,
    IOFileUploadQueue,
    RawUploadQueue,
    SequenceUploadQueue,
    TimeSeriesUploadQueue,
)
from cognite.extractorutils.uploader.time_series import MAX_DATAPOINT_STRING_BYTES


@patch("cognite.client.CogniteClient")
def test_raw_uploader1(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    queue = RawUploadQueue(client)

    row1 = Row("key1", {"col1": "val1", "col2": "val2"})
    row2 = Row("key2", {"col1": "val1", "col2": "val2"})

    queue.add_to_upload_queue("db", "table", row1)
    queue.add_to_upload_queue("db", "table", row2)

    client.raw.rows.insert.assert_not_called()

    queue.upload()

    client.raw.rows.insert.assert_called_with(db_name="db", table_name="table", row=[row1, row2], ensure_parent=True)

    queue.upload()
    client.raw.rows.insert.assert_called_once()


@patch("cognite.client.CogniteClient")
def test_raw_uploader2(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    post_upload_test = {"value": False}

    def post(_x: Any) -> None:
        post_upload_test["value"] = True

    queue = RawUploadQueue(client, post_upload_function=post, max_queue_size=2)
    queue.add_to_upload_queue("db", "table", Row("key1", {"val": "a"}))

    client.raw.rows.insert.assert_not_called()

    queue.add_to_upload_queue("db", "table", Row("key2", {"val": "a" * 100}))

    client.raw.rows.insert.assert_called_once()
    assert post_upload_test["value"]


@patch("cognite.client.CogniteClient")
def test_ts_uploader1(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    queue = TimeSeriesUploadQueue(client)

    start: float = datetime.now(tz=timezone.utc).timestamp() * 1000.0

    queue.add_to_upload_queue(id=1, datapoints=[(start + 1, 1), (start + 2, 2)])
    queue.add_to_upload_queue(id=2, datapoints=[(start + 3, 3), (start + 4, 4)])
    queue.add_to_upload_queue(id=1, datapoints=[(start + 5, 5), (start + 6, 6)])
    queue.add_to_upload_queue(id=3, datapoints=[(start + 7, 7), (start + 8, 8)])

    client.time_series.data.insert_multiple.assert_not_called()
    queue.upload()
    client.time_series.data.insert_multiple.assert_called_with(
        [
            {"id": 1, "datapoints": [(start + 1, 1), (start + 2, 2), (start + 5, 5), (start + 6, 6)]},
            {"id": 2, "datapoints": [(start + 3, 3), (start + 4, 4)]},
            {"id": 3, "datapoints": [(start + 7, 7), (start + 8, 8)]},
        ]
    )


@patch("cognite.client.CogniteClient")
def test_ts_uploader2(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    post_upload_test = {"value": False}

    def post(_x: Any) -> None:
        post_upload_test["value"] = True

    queue = TimeSeriesUploadQueue(client, max_upload_interval=2, post_upload_function=post)
    queue.start()

    start: float = datetime.now(tz=timezone.utc).timestamp() * 1000.0

    queue.add_to_upload_queue(id=1, datapoints=[(start + 1, 1), (start + 2, 2)])
    queue.add_to_upload_queue(id=2, datapoints=[(start + 3, 3), (start + 4, 4)])
    queue.add_to_upload_queue(id=1, datapoints=[(start + 5, 5), (start + 6, 6)])
    queue.add_to_upload_queue(id=3, datapoints=[(start + 7, 7), (start + 8, 8)])

    time.sleep(2.1)

    client.time_series.data.insert_multiple.assert_called_with(
        [
            {"id": 1, "datapoints": [(start + 1, 1), (start + 2, 2), (start + 5, 5), (start + 6, 6)]},
            {"id": 2, "datapoints": [(start + 3, 3), (start + 4, 4)]},
            {"id": 3, "datapoints": [(start + 7, 7), (start + 8, 8)]},
        ]
    )
    assert post_upload_test["value"]

    queue.stop()


@patch("cognite.client.CogniteClient")
def test_ts_uploader_discard(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    post_upload_test = {"value": False}

    def post(_x: Any) -> None:
        post_upload_test["value"] = True

    queue = TimeSeriesUploadQueue(client, max_upload_interval=2, post_upload_function=post)
    queue.start()

    start: float = datetime.now(tz=timezone.utc).timestamp() * 1000.0

    queue.add_to_upload_queue(id=1, datapoints=[(start + 1, 1), (math.nan, 1), (start + 1, math.nan), (start + 2, 2)])
    queue.add_to_upload_queue(
        id=2, datapoints=[(start + 3, 3), (start + 1, 1e101), (start + 1, -1e101), (start + 4, 4)]
    )
    queue.add_to_upload_queue(
        id=1, datapoints=[(start + 5, 5), (start + 1, math.inf), (start + 2, -math.inf), (start + 6, 6)]
    )
    queue.add_to_upload_queue(
        id=3,
        datapoints=[(start + 7, "str1"), (start + 9, ("t" * (MAX_DATAPOINT_STRING_BYTES + 1))), (start + 8, "str2")],
    )

    time.sleep(2.1)

    client.time_series.data.insert_multiple.assert_called_with(
        [
            {"id": 1, "datapoints": [(start + 1, 1), (start + 2, 2), (start + 5, 5), (start + 6, 6)]},
            {"id": 2, "datapoints": [(start + 3, 3), (start + 4, 4)]},
            {"id": 3, "datapoints": [(start + 7, "str1"), (start + 8, "str2")]},
        ]
    )
    assert post_upload_test["value"]

    queue.stop()


@patch("cognite.client.CogniteClient")
def test_event_uploader1(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    queue = EventUploadQueue(client)

    event1 = Event(start_time=1, end_time=2, description="test event")
    event2 = Event(start_time=3, end_time=4, description="another test event")

    queue.add_to_upload_queue(event1)
    queue.add_to_upload_queue(event2)

    client.events.create.assert_not_called()
    queue.upload()
    client.events.create.assert_called_with([event1, event2])


@patch("cognite.client.CogniteClient")
def test_event_uploader2(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    post_upload_test = {"value": False}

    def post(_x: Any) -> None:
        post_upload_test["value"] = True

    queue = EventUploadQueue(client, max_upload_interval=2, post_upload_function=post)
    queue.start()

    event1 = Event(start_time=1, end_time=2, description="test event")
    event2 = Event(start_time=3, end_time=4, description="another test event")

    queue.add_to_upload_queue(event1)
    queue.add_to_upload_queue(event2)

    time.sleep(2.1)

    client.events.create.assert_called_with([event1, event2])
    assert post_upload_test["value"]

    queue.stop()


@patch("cognite.client.CogniteClient")
def test_sequence_uploader1(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    post_upload_test = {"value": 0, "rows": 0}

    def post(x: Any) -> None:
        post_upload_test["value"] += 1
        post_upload_test["rows"] += sum([len(e.values) for e in x])

    queue = SequenceUploadQueue(client, max_upload_interval=2, post_upload_function=post, create_missing=True)
    queue.start()

    queue.add_to_upload_queue(
        rows=[{"rowNumber": 1, "values": ["Hello"]}],
        column_external_ids=[{"externalId": "field", "valueType": "String"}],
        external_id="seq-1",
    )

    queue.add_to_upload_queue(
        rows=[{"rowNumber": 2, "values": ["World"]}],
        column_external_ids=[{"externalId": "field", "valueType": "String"}],
        external_id="seq-1",
    )

    time.sleep(2.1)

    assert post_upload_test["value"] == 1
    assert post_upload_test["rows"] == 2

    queue.stop()


@patch("cognite.client.CogniteClient")
def test_sequence_uploader2(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()

    post_upload_test = {"value": 0, "rows": 0}

    def post(x: Any) -> None:
        post_upload_test["value"] += 1
        post_upload_test["rows"] += sum([len(e.values) for e in x])

    queue = SequenceUploadQueue(client, max_upload_interval=2, post_upload_function=post, create_missing=True)
    queue.start()

    queue.add_to_upload_queue(
        rows=[{"rowNumber": 1, "values": ["Hello"]}],
        column_external_ids=[{"externalId": "field", "valueType": "String"}],
        external_id="seq-1",
    )

    queue.add_to_upload_queue(
        rows=[{"rowNumber": 2, "values": ["World"]}],
        column_external_ids=[{"externalId": "field", "valueType": "String"}],
        external_id="seq-2",
    )

    time.sleep(2.1)

    assert post_upload_test["value"] == 1
    assert post_upload_test["rows"] == 2

    queue.stop()


@patch("cognite.client.CogniteClient")
def test_mock_private_link_upload(MockCogniteClient: Mock) -> None:
    # mocking privatelink behavior, testing the URL swap
    client = MockCogniteClient()
    base_url_str = "https://qweasd-666.gremiocampeao.cognitedata.com"
    base_url = URL(base_url_str)

    client.config.base_url = "https://qweasd-666.gremiocampeao.cognitedata.com"
    client._config.client_name = "extutil-unit-test"

    queue = IOFileUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1, max_parallelism=1)

    mock_download_url = "https://restricted-api.gremiocampeao.cognitedata.com/downloadUrl/myfile"

    bytes_ = "Até a pé nós iremos, para o que der e vier, mas o certo, mas o certo é que nós estaremos, com o Grêmio onde o Grêmio estiver".encode()
    mock_stream = BytesIO(bytes_)

    response: Request = queue._get_file_upload_request(mock_download_url, mock_stream, len(bytes_))

    assert response.url.netloc == base_url.netloc


@patch("cognite.client.CogniteClient")
def test_manually_set_is_uploaded(mock_client: CogniteClientMock) -> None:
    mock_client.config.max_workers = 4

    queue = IOFileUploadQueue(
        cdf_client=mock_client,
        max_queue_size=10,
    )

    file_apply = CogniteExtractorFileApply(
        external_id="test-file",
        name="test-file.txt",
        space="test-space",
    )

    # Record the initial state of is_uploaded
    initial_is_uploaded = file_apply.is_uploaded

    def read_file() -> BinaryIO:
        return BytesIO(b"test content")

    # Mock the upload methods
    with patch.object(queue, "_upload_bytes"), patch.object(queue, "_upload_only_metadata") as mock_upload_metadata:
        mock_file_metadata = Mock(spec=FileMetadata)
        mock_file_metadata.mime_type = "text/plain"
        mock_upload_metadata.return_value = (mock_file_metadata, "https://upload.url")

        with queue:
            queue.add_io_to_upload_queue(file_apply, read_file)
            queue.upload()

    # Verify that is_uploaded was NOT manually changed by the uploader
    assert file_apply.is_uploaded == initial_is_uploaded, (
        "is_uploaded should not be manually changed by the uploader, it should be managed by the SDK"
    )
