import os
import random
import time
import unittest
from datetime import datetime, timezone

from cognite.client import CogniteClient
from cognite.client.data_classes import Row, TimeSeries
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.extractorutils.uploader import EventUploadQueue, RawUploadQueue, TimeSeriesUploadQueue


class IntegrationTests(unittest.TestCase):
    database_name: str = "integrationTests"
    table_name: str = "extractorUtils"

    time_series1: str = "util_integration_ts_test_1"
    time_series2: str = "util_integration_ts_test_2"

    def setUp(self):
        self.client = CogniteClient(
            api_key=os.getenv("COGNITE_API_KEY"),
            project="extractor-tests",
            base_url="https://greenfield.cognitedata.com",
            client_name="extractor-utils-integration-tests",
        )

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

    def tearDown(self):
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

    def test_raw_upload_queue(self):
        queue = RawUploadQueue(cdf_client=self.client, queue_threshold=500)

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
        pass

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

        recv_points1 = self.client.datapoints.retrieve(
            external_id=self.time_series1, start="1w-ago", end="now", limit=None
        )
        recv_points2 = self.client.datapoints.retrieve(
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

        recv_points1 = self.client.datapoints.retrieve(
            external_id=self.time_series1, start="1w-ago", end="now", limit=None
        )

        self.assertListEqual([int(p) for p in recv_points1.value], [p[1] for p in points1])

        queue.stop()
