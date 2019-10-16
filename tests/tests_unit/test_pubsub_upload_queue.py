import unittest
from json import dumps
from unittest.mock import Mock

from google.cloud.pubsub_v1 import PublisherClient

from cognite.client.data_classes import Row


class FakeFuture:
    @staticmethod
    def running(*args):
        return False

    def add_done_callback(self, *args):
        pass


class TestPubSubUploadQueue(unittest.TestCase):
    def setUp(self):
        from cognite.extractorutils import uploader

        self.modified_uploader = uploader
        self.modified_uploader.pubsub_v1 = Mock()
        self.fake_publisher: PublisherClient = Mock()
        self.fake_publisher.publish.return_value = FakeFuture
        self.modified_uploader.pubsub_v1.PublisherClient.return_value = self.fake_publisher

    def test_pubsub_upload_queue(self):
        queue = self.modified_uploader.PubSubUploadQueue("project", "topic")

        row1 = Row("key1", {"col1": "val1", "col2": "val2"})
        row2 = Row("key2", {"col1": "val1", "col2": "val2"})

        queue.add_to_upload_queue("db", "table", row1)
        queue.add_to_upload_queue("db", "table", row2)

        self.fake_publisher.publish.assert_not_called()

        queue.upload()

        self.fake_publisher.publish.assert_called_with(
            queue.topic_path,
            str.encode(
                dumps(
                    {
                        "database": "db",
                        "table": "table",
                        "rows": [{"key": r.key, "columns": r.columns} for r in [row1, row2]],
                    }
                )
            ),
        )

        queue.upload()
        self.fake_publisher.publish.assert_called_once()
