import time
import unittest
from unittest.mock import patch

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, Row
from cognite.extractorutils.uploader import EventUploadQueue, RawUploadQueue, TimeSeriesUploadQueue


class TestUploadQueue(unittest.TestCase):
    @patch("cognite.client.CogniteClient")
    def test_raw_uploader1(self, MockCogniteClient):
        client: CogniteClient = MockCogniteClient()

        queue = RawUploadQueue(client)

        row1 = Row("key1", {"col1": "val1", "col2": "val2"})
        row2 = Row("key2", {"col1": "val1", "col2": "val2"})

        queue.add_to_upload_queue("db", "table", row1)
        queue.add_to_upload_queue("db", "table", row2)

        client.raw.rows.insert.assert_not_called()

        queue.upload()

        client.raw.rows.insert.assert_called_with(
            db_name="db", table_name="table", row=[row1, row2], ensure_parent=True
        )

        queue.upload()
        client.raw.rows.insert.assert_called_once()

    @patch("cognite.client.CogniteClient")
    def test_raw_uploader2(self, MockCogniteClient):
        client: CogniteClient = MockCogniteClient()

        post_upload_test = {"value": False}

        def post(x):
            post_upload_test["value"] = True

        queue = RawUploadQueue(client, post_upload_function=post, max_queue_size=1)
        queue.add_to_upload_queue("db", "table", Row("key1", {"val": "a"}))

        client.raw.rows.insert.assert_not_called()

        queue.add_to_upload_queue("db", "table", Row("key2", {"val": "a" * 100}))

        client.raw.rows.insert.assert_called_once()
        self.assertTrue(post_upload_test["value"])

    @patch("cognite.client.CogniteClient")
    def test_ts_uploader1(self, MockCogniteClient):
        client: CogniteClient = MockCogniteClient()

        queue = TimeSeriesUploadQueue(client)

        with self.assertRaises(ValueError):
            queue.start()

        queue.add_to_upload_queue(id=1, datapoints=[(1, 1), (2, 2)])
        queue.add_to_upload_queue(id=2, datapoints=[(3, 3), (4, 4)])
        queue.add_to_upload_queue(id=1, datapoints=[(5, 5), (6, 6)])
        queue.add_to_upload_queue(id=3, datapoints=[(7, 7), (8, 8)])

        client.datapoints.insert_multiple.assert_not_called()
        queue.upload()
        client.datapoints.insert_multiple.assert_called_with(
            [
                {"id": 1, "datapoints": [(1, 1), (2, 2), (5, 5), (6, 6)]},
                {"id": 2, "datapoints": [(3, 3), (4, 4)]},
                {"id": 3, "datapoints": [(7, 7), (8, 8)]},
            ]
        )

    @patch("cognite.client.CogniteClient")
    def test_ts_uploader2(self, MockCogniteClient):
        client: CogniteClient = MockCogniteClient()

        post_upload_test = {"value": False}

        def post(x):
            post_upload_test["value"] = True

        queue = TimeSeriesUploadQueue(client, max_upload_interval=2, post_upload_function=post)
        queue.start()

        queue.add_to_upload_queue(id=1, datapoints=[(1, 1), (2, 2)])
        queue.add_to_upload_queue(id=2, datapoints=[(3, 3), (4, 4)])
        queue.add_to_upload_queue(id=1, datapoints=[(5, 5), (6, 6)])
        queue.add_to_upload_queue(id=3, datapoints=[(7, 7), (8, 8)])

        time.sleep(2.1)

        client.datapoints.insert_multiple.assert_called_with(
            [
                {"id": 1, "datapoints": [(1, 1), (2, 2), (5, 5), (6, 6)]},
                {"id": 2, "datapoints": [(3, 3), (4, 4)]},
                {"id": 3, "datapoints": [(7, 7), (8, 8)]},
            ]
        )
        self.assertTrue(post_upload_test["value"])

        queue.stop()

    @patch("cognite.client.CogniteClient")
    def test_event_uploader1(self, MockCogniteClient):
        client: CogniteClient = MockCogniteClient()

        queue = EventUploadQueue(client)

        with self.assertRaises(ValueError):
            queue.start()

        event1 = Event(start_time=1, end_time=2, description="test event")
        event2 = Event(start_time=3, end_time=4, description="another test event")

        queue.add_to_upload_queue(event1)
        queue.add_to_upload_queue(event2)

        client.events.create.assert_not_called()
        queue.upload()
        client.events.create.ssert_called_with([event1, event2])

    @patch("cognite.client.CogniteClient")
    def test_event_uploader2(self, MockCogniteClient):
        client: CogniteClient = MockCogniteClient()

        post_upload_test = {"value": False}

        def post(x):
            post_upload_test["value"] = True

        queue = EventUploadQueue(client, max_upload_interval=2, post_upload_function=post)
        queue.start()

        event1 = Event(start_time=1, end_time=2, description="test event")
        event2 = Event(start_time=3, end_time=4, description="another test event")

        queue.add_to_upload_queue(event1)
        queue.add_to_upload_queue(event2)

        time.sleep(2.1)

        client.events.create.ssert_called_with([event1, event2])
        self.assertTrue(post_upload_test["value"])

        queue.stop()
