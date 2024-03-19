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
import pathlib
import random
import string
import time
import unittest
from datetime import datetime, timezone

from parameterized import parameterized_class

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import Event, FileMetadata, Row, TimeSeries
from cognite.client.data_classes.assets import Asset
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.extractorutils.uploader import RawUploadQueue, TimeSeriesUploadQueue
from cognite.extractorutils.uploader.assets import AssetUploadQueue
from cognite.extractorutils.uploader.events import EventUploadQueue
from cognite.extractorutils.uploader.files import BytesUploadQueue, FileUploadQueue

test_id = random.randint(0, 2**31)


@parameterized_class(
    [
        {"functions_runtime": "true"},
        {"functions_runtime": "false"},
    ]
)
class IntegrationTests(unittest.TestCase):
    database_name: str = "integrationTests"
    table_name: str = f"extractorUtils-{test_id}"

    time_series1: str = f"util_integration_ts_test_1-{test_id}"
    time_series2: str = f"util_integration_ts_test_2-{test_id}"
    time_series3: str = f"util_integration_ts_test_3-{test_id}"

    event1: str = f"util_integration_event_test_1-{test_id}"
    event2: str = f"util_integration_event_test_2-{test_id}"
    event3: str = f"util_integration_event_test_3-{test_id}"

    asset1: str = f"util_integration_asset_test_1-{test_id}"
    asset2: str = f"util_integration_asset_test_2-{test_id}"
    asset3: str = f"util_integration_asset_test_3-{test_id}"

    file1: str = f"util_integration_file_test_1-{test_id}"
    file2: str = f"util_integration_file_test_2-{test_id}"
    bigfile: str = f"util_integration_file-big-{test_id}"
    empty_file: str = f"util_integration_file_test_3-{test_id}"

    def setUp(self):
        os.environ["COGNITE_FUNCTION_RUNTIME"] = self.functions_runtime
        cognite_project = os.environ["COGNITE_PROJECT"]
        cognite_base_url = os.environ["COGNITE_BASE_URL"]
        cognite_token_url = os.environ["COGNITE_TOKEN_URL"]
        cognite_client_id = os.environ["COGNITE_CLIENT_ID"]
        cognite_client_secret = os.environ["COGNITE_CLIENT_SECRET"]
        cognite_project_scopes = os.environ["COGNITE_TOKEN_SCOPES"].split(",")
        client_config = ClientConfig(
            project=cognite_project,
            base_url=cognite_base_url,
            credentials=OAuthClientCredentials(
                cognite_token_url, cognite_client_id, cognite_client_secret, cognite_project_scopes
            ),
            client_name="extractor-utils-integration-tests",
        )
        self.client = CogniteClient(client_config)

        # Delete stuff we will use if it exists
        try:
            self.client.raw.tables.delete(self.database_name, self.table_name)
        except CogniteAPIError:
            pass
        try:
            self.client.time_series.delete(external_id=self.time_series1)
        except CogniteNotFoundError:
            pass
        try:
            self.client.time_series.delete(external_id=self.time_series2)
        except CogniteNotFoundError:
            pass
        try:
            self.client.assets.delete(external_id=[self.asset1, self.asset2, self.asset3], ignore_unknown_ids=True)
        except CogniteNotFoundError:
            pass

        # No ignore_unknown_ids in files, so we need to delete them one at a time
        for file in [self.file1, self.file2]:
            try:
                self.client.files.delete(external_id=file)
            except CogniteNotFoundError:
                pass

    def tearDown(self):
        try:
            self.client.raw.tables.delete(self.database_name, self.table_name)
        except CogniteAPIError:
            pass
        self.client.time_series.delete(external_id=[self.time_series1, self.time_series2], ignore_unknown_ids=True)
        self.client.events.delete(external_id=[self.event1, self.event2, self.event3], ignore_unknown_ids=True)
        self.client.assets.delete(external_id=[self.asset1, self.asset2, self.asset3], ignore_unknown_ids=True)
        # No ignore_unknown_ids in files, so we need to delete them one at a time
        for file in [self.file1, self.file2, self.bigfile, self.empty_file]:
            try:
                self.client.files.delete(external_id=file)
            except CogniteNotFoundError:
                pass

    def await_is_uploaded_status(self, external_id):
        for _ in range(10):
            if self.client.files.retrieve(external_id=external_id).uploaded:
                return
            time.sleep(1)

    def test_raw_upload_queue(self):
        queue = RawUploadQueue(cdf_client=self.client, max_queue_size=500)

        uploaded = []

        for i in range(500):
            r = Row("key{:03}".format(i), {"col": "val{}".format(i)})

            queue.add_to_upload_queue(self.database_name, self.table_name, r)
            uploaded.append(r)

        queue.upload()

        time.sleep(10)

        rows_in_cdf = sorted(
            self.client.raw.rows.list(db_name=self.database_name, table_name=self.table_name, limit=None),
            key=lambda row: row.key,
        )

        self.assertListEqual(
            [{k: r.__dict__[k] for k in ["key", "columns"]} for r in uploaded],
            [{k: r.__dict__[k] for k in ["key", "columns"]} for r in rows_in_cdf],
        )

    def test_time_series_upload_queue1(self):
        created = self.client.time_series.create(
            [TimeSeries(external_id=self.time_series1), TimeSeries(external_id=self.time_series2, is_string=True)]
        )

        last_point = {"timestamp": 0}

        def store_latest(points):
            last_point["timestamp"] = max(last_point["timestamp"], *[ts["datapoints"][-1][0] for ts in points])

        queue = TimeSeriesUploadQueue(cdf_client=self.client, post_upload_function=store_latest, max_upload_interval=1)
        queue.start()

        # Create some synthetic data
        now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        points1_1 = [(now + i * 107, random.randint(0, 10)) for i in range(10)]
        points1_2 = [(now + i * 107, random.randint(0, 10)) for i in range(10, 100)]
        points2 = [(now + i * 93, chr(97 + i)) for i in range(26)]

        queue.add_to_upload_queue(external_id=self.time_series1, datapoints=points1_1)
        queue.add_to_upload_queue(external_id=self.time_series1, datapoints=points1_2)
        queue.add_to_upload_queue(id=created[1].id, datapoints=points2)

        time.sleep(30)

        recv_points1 = self.client.time_series.data.retrieve(
            external_id=self.time_series1, start="1w-ago", end="now", limit=None
        )
        recv_points2 = self.client.time_series.data.retrieve(
            external_id=self.time_series2, start="1w-ago", end="now", limit=None
        )

        self.assertListEqual([int(p) for p in recv_points1.value], [p[1] for p in points1_1 + points1_2])
        self.assertListEqual(recv_points2.value, [p[1] for p in points2])
        self.assertEqual(last_point["timestamp"], points1_2[-1][0])

        queue.stop()

    def test_time_series_upload_queue2(self):
        self.client.time_series.create(TimeSeries(external_id=self.time_series1))

        queue = TimeSeriesUploadQueue(cdf_client=self.client, max_upload_interval=1)
        queue.start()

        # Create some synthetic data
        now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        points1 = [(now + i * 107, random.randint(0, 10)) for i in range(10)]
        points2 = [(now + i * 107, random.randint(0, 10)) for i in range(10, 20)]

        queue.add_to_upload_queue(external_id=self.time_series1, datapoints=points1)
        queue.add_to_upload_queue(external_id="noSuchExternalId", datapoints=points2)

        time.sleep(20)

        recv_points1 = self.client.time_series.data.retrieve(
            external_id=self.time_series1, start="1w-ago", end="now", limit=None
        )

        self.assertListEqual([int(p) for p in recv_points1.value], [p[1] for p in points1])

        queue.stop()

    def test_time_series_upload_queue_create_missing(self):
        id1 = self.time_series1 + "_missing"
        id2 = self.time_series2 + "_missing"
        id3 = self.time_series3 + "_missing"

        self.client.time_series.delete(external_id=[id1, id2], ignore_unknown_ids=True)

        queue = TimeSeriesUploadQueue(cdf_client=self.client, create_missing=True)

        # Create some synthetic data
        now = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        points1 = [(now + i * 107, random.randint(0, 10)) for i in range(10)]
        points2 = [
            (now + i * 107, "".join([random.choice(string.printable) for j in range(16)])) for i in range(10, 20)
        ]
        points3 = [{"timestamp": now + i * 107, "value": random.randint(0, 10)} for i in range(10)]

        queue.add_to_upload_queue(external_id=id1, datapoints=points1)
        queue.add_to_upload_queue(external_id=id2, datapoints=points2)
        queue.add_to_upload_queue(external_id=id3, datapoints=points3)

        queue.upload()
        time.sleep(3)

        recv_points1 = self.client.time_series.data.retrieve(external_id=id1, start="1w-ago", end="now", limit=None)
        recv_points2 = self.client.time_series.data.retrieve(external_id=id2, start="1w-ago", end="now", limit=None)
        recv_points3 = self.client.time_series.data.retrieve(external_id=id3, start="1w-ago", end="now", limit=None)

        self.assertListEqual([int(p) for p in recv_points1.value], [p[1] for p in points1])
        self.assertListEqual([p for p in recv_points2.value], [p[1] for p in points2])
        self.assertListEqual([int(p) for p in recv_points3.value], [p["value"] for p in points3])

        queue.stop()
        self.client.time_series.delete(external_id=[id1, id2, id3], ignore_unknown_ids=True)

    def test_events_upload_queue_upsert(self):
        queue = EventUploadQueue(cdf_client=self.client)

        # Upload a pair of events
        queue.add_to_upload_queue(Event(external_id=self.event1, description="desc"))
        queue.add_to_upload_queue(Event(external_id=self.event2, description="desc"))

        queue.upload()

        # This should result in an update and a create
        queue.add_to_upload_queue(Event(external_id=self.event2, description="new desc"))
        queue.add_to_upload_queue(Event(external_id=self.event3, description="new desc"))

        queue.upload()

        retrieved = self.client.events.retrieve_multiple(external_ids=[self.event1, self.event2, self.event3])
        assert retrieved[0].description == "desc"
        assert retrieved[1].description == "new desc"
        assert retrieved[2].description == "new desc"

    def test_assets_upload_queue_upsert(self):
        queue = AssetUploadQueue(cdf_client=self.client)

        # Upload a pair of events
        queue.add_to_upload_queue(Asset(external_id=self.asset1, description="desc", name="name"))
        queue.add_to_upload_queue(Asset(external_id=self.asset2, description="desc", name="name"))

        queue.upload()

        # This should result in an update and a create
        queue.add_to_upload_queue(Asset(external_id=self.asset2, description="new desc", name="new name"))
        queue.add_to_upload_queue(Asset(external_id=self.asset3, description="new desc", name="new name"))

        queue.upload()

        retrieved = self.client.assets.retrieve_multiple(external_ids=[self.asset1, self.asset2, self.asset3])
        assert retrieved[0].description == "desc"
        assert retrieved[1].description == "new desc"
        assert retrieved[2].description == "new desc"
        assert retrieved[1].name == "new name"
        assert retrieved[2].name == "new name"

    def test_file_upload_queue(self):
        queue = FileUploadQueue(cdf_client=self.client, overwrite_existing=True, max_queue_size=2)

        current_dir = pathlib.Path(__file__).parent.resolve()

        # Upload a pair of actual files
        queue.add_to_upload_queue(
            file_meta=FileMetadata(external_id=self.file1, name=self.file1),
            file_name=current_dir.joinpath("test_file_1.txt"),
        )
        queue.add_to_upload_queue(
            file_meta=FileMetadata(external_id=self.file2, name=self.file2),
            file_name=current_dir.joinpath("test_file_2.txt"),
        )
        # Upload the Filemetadata of an empty file without trying to upload the "content"
        queue.add_to_upload_queue(
            file_meta=FileMetadata(external_id=self.empty_file, name=self.empty_file),
            file_name=current_dir.joinpath("empty_file.txt"),
        )

        queue.upload()

        self.await_is_uploaded_status(self.file1)
        self.await_is_uploaded_status(self.file2)
        file1 = self.client.files.download_bytes(external_id=self.file1)
        file2 = self.client.files.download_bytes(external_id=self.file2)
        file3 = self.client.files.retrieve(external_id=self.empty_file)

        assert file1 == b"test content\n"
        assert file2 == b"other test content\n"
        assert file3.name == self.empty_file

    def test_bytes_upload_queue(self):
        queue = BytesUploadQueue(cdf_client=self.client, overwrite_existing=True, max_queue_size=1)

        queue.add_to_upload_queue(
            content=b"bytes content",
            metadata=FileMetadata(external_id=self.file1, name=self.file1),
        )
        queue.add_to_upload_queue(
            content=b"other bytes content",
            metadata=FileMetadata(external_id=self.file2, name=self.file2),
        )

        queue.upload()
        self.await_is_uploaded_status(self.file1)
        self.await_is_uploaded_status(self.file2)
        file1 = self.client.files.download_bytes(external_id=self.file1)
        file2 = self.client.files.download_bytes(external_id=self.file2)

        assert file1 == b"bytes content"
        assert file2 == b"other bytes content"

    def test_big_file_upload_queue(self):
        queue = BytesUploadQueue(cdf_client=self.client, overwrite_existing=True, max_queue_size=1)
        queue.max_file_chunk_size = 6_000_000
        queue.max_single_chunk_file_size = 6_000_000

        content = b"large" * 2_000_000

        queue.add_to_upload_queue(content=content, metadata=FileMetadata(external_id=self.bigfile, name=self.bigfile))

        queue.upload()

        self.await_is_uploaded_status(self.bigfile)
        bigfile = self.client.files.download_bytes(external_id=self.bigfile)

        assert len(bigfile) == 10_000_000
