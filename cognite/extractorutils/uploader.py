"""
Module containing upload queue classes. The UploadQueue classes chunks together items and uploads them together to CDF,
both to minimize the load on the API, and also to speed up uploading as requests can be slow.

Each upload queue comes with some configurable conditions that, when met, automatically triggers an upload.

**Note:** You cannot assume that an element is uploaded when it is added to the queue, since the upload may be
delayed. To ensure that everything is uploaded you should set the `post_upload_function` callback to verify. For
example, for a time series queue you might want to check the latest time stamp, as such (assuming incremental time
stamps and using timestamp-value tuples as data point format): ::

    latest_point = {"timestamp": 0}

    def store_latest(time_series):
        # time_series is a list of dicts where each dict represents a time series
        latest_point["timestamp"] = max(
            latest_point["timestamp"],
            *[ts["datapoints"][-1][0] for ts in time_series]
        )

        # here you can also update metrics etc

    queue = TimeSeriesUploadQueue(
        cdf_client=self.client,
        post_upload_function=store_latest,
        max_upload_interval=1
    )
"""
import logging
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime
from json import dumps
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from google.cloud import pubsub_v1

from cognite.client import CogniteClient
from cognite.client._api.raw import RawAPI  # Private access, but we need it for typing
from cognite.client.data_classes import Event
from cognite.client.data_classes.raw import Row
from cognite.client.exceptions import CogniteNotFoundError
from cognite.extractorutils._inner_util import _EitherId

DataPointList = Union[
    List[Dict[Union[int, float, datetime], Union[int, float, str]]],
    List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
]


class UploadQueue(ABC):
    """
    Abstract uploader class.

    Args:
        post_upload_function (Callable[[int]): (Optional). A function that will be called after each upload. The
            function will be given one argument: A list of the elements that were uploaded
        queue_threshold (int): (Optional). Maximum byte size of upload queue. Defaults to no queue (ie upload after each
            add_to_upload_queue).
    """

    def __init__(
        self, post_upload_function: Optional[Callable[[int], None]] = None, queue_threshold: Optional[int] = None
    ):
        """
        Called by subclasses. Saves info and inits upload queue.
        """
        self.threshold = queue_threshold if queue_threshold is not None else -1
        self.upload_queue_byte_size = 0

        self.post_upload_function = post_upload_function

    def _check_triggers(self, item: Any) -> Any:
        """
        Check if upload triggers are met, call upload if they are. Called by subclasses.

        Args:
            item (dict): Item added to upload queue

        Returns:
            Any: Result of upload, if upload happened.
        """
        self.upload_queue_byte_size += len(repr(item))

        # Check upload threshold
        if self.upload_queue_byte_size > self.threshold and self.threshold >= 0:
            return self.upload()

        return None

    def _post_upload(self, uploaded: List[Any]):
        if self.post_upload_function is not None:
            try:
                self.post_upload_function(uploaded)
            except Exception as e:
                logging.getLogger(__name__).exception("Error during upload callback")

    @abstractmethod
    def add_to_upload_queue(self, *args) -> Any:
        """
        Adds an element to the upload queue. The queue will be uploaded if the queue byte size is larger than the
        threshold specified in the config.

        Returns:
            Any: Result of upload, if applicable and if upload happened.
        """

    @abstractmethod
    def upload(self) -> Any:
        """
        Uploads the queue.

        Returns:
            Any: Result status of upload.
        """


class RawUploadQueue(UploadQueue):
    """
    Upload queue for RAW

    Args:
        cdf_client (CogniteClient): Cognite Data Fusion client
        post_upload_function (Callable[[int]): (Optional). A function that will be called after each upload. The
            function will be given one argument: An a list of the rows that were uploaded.
        queue_threshold (int): (Optional). Maximum size of upload queue. Defaults to no queue (ie upload after each
            add_to_upload_queue).
    """

    raw: RawAPI

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Any]], None]] = None,
        queue_threshold: Optional[int] = None,
    ):
        # Super sets post_upload and threshold
        super().__init__(post_upload_function, queue_threshold)

        self.upload_queue: Dict[str, Dict[str, List[Row]]] = dict()

        self.raw = cdf_client.raw

    def add_to_upload_queue(self, database: str, table: str, raw_row: Row) -> None:
        """
        Adds a row to the upload queue. The queue will be uploaded if the queue byte size is larger than the threshold
        specified in the __init__.

        Args:
            database (str): The database to upload the Raw object to
            table (str): The table to upload the Raw object to
            raw_row (Row): The row object

        Returns:
            None.
        """
        # Ensure that the dicts has correct keys
        if database not in self.upload_queue:
            self.upload_queue[database] = dict()
        if table not in self.upload_queue[database]:
            self.upload_queue[database][table] = []

        # Append row to queue
        self.upload_queue[database][table].append(raw_row)

        self._check_triggers(raw_row)

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        for database, tables in self.upload_queue.items():
            for table, rows in tables.items():
                # Upload
                self.raw.rows.insert(db_name=database, table_name=table, row=rows, ensure_parent=True)

                # Perform post-upload logic if applicable
                self._post_upload(rows)

        self.upload_queue.clear()
        self.upload_queue_byte_size = 0


class PubSubUploadQueue(UploadQueue):
    """
    Upload queue towards Google's Pub/Sub API. This queue will batch up RAW rows and send them as JSON encoded PubSub
    messages

    JSON format:
        The following example illustrates the message format: ::

            {
                "database": db-name,
                "table": table-name,
                "rows": [
                    {
                        "key": key,
                        "columns": {
                            ...
                        }
                    },
                    ...
                ]
            }

    Args:
        project_id (str): ID of Google Cloud Platform project to use.
        topic_name (str): Name of Pub/Sub topic to publish to
        post_upload_function (Callable[[int]): (Optional). A function that will be called after each upload. The
            function will be given one argument: An a list of the rows that were uploaded.
        queue_threshold (int): (Optional). Maximum size of upload queue. Defaults to no queue (ie upload after each
            add_to_upload_queue).
        always_ensure_topic (bool): (Optional). Call ensure_topic before each upload
    """

    def __init__(
        self,
        project_id: str,
        topic_name: str,
        post_upload_function: Optional[Callable[[List[Row]], None]] = None,
        queue_threshold: Optional[int] = None,
        always_ensure_topic: Optional[bool] = False,
    ):
        # Super sets post_upload and threshold
        super().__init__(post_upload_function, queue_threshold)

        self.upload_queue: Dict[str, Dict[str, List[Row]]] = dict()

        # Init pub/sub
        self.pubsub_client = pubsub_v1.PublisherClient()

        # Save Google Pub/Sub info, construct topic path
        self.project_id = project_id
        self.project_path = self.pubsub_client.project_path(project_id)
        self.topic_name = topic_name
        self.topic_path = self.pubsub_client.topic_path(project_id, topic_name)

        self.always_ensure_topic = always_ensure_topic

    def ensure_topic(self):
        """
        Checks that the given topic name exists in project, and creates it if it doesn't.

        Note that this action requires extended previlegies on the cloud platform. The normal 'publisher' role is not
        allowed to view or manage topics.

        Raises:
            PermissionDenied: If API key stored in GOOGLE_APPLICATION_CREDENTIALS are not allowed to manipulate topics.
        """
        topic_paths = [topic.name for topic in self.pubsub_client.list_topics(self.project_path)]

        if self.topic_path not in topic_paths:
            self.pubsub_client.create_topic(self.topic_path)

    def add_to_upload_queue(self, database: str, table: str, raw_row: Row) -> List[Any]:
        """
        Adds a row to the upload queue. The queue will be uploaded if the queue
        byte size is larger than the threshold specified in the config.

        Args:
            database (str): The database to upload the Raw object to
            table (str): The table to upload the Raw object to
            raw_row (Row): The row object

        Returns:
            List[Any]: Pub/Sub message IDs, if an upload was made. None otherwise.
        """
        # Ensure that the dicts has correct keys
        if database not in self.upload_queue:
            self.upload_queue[database] = dict()
        if table not in self.upload_queue[database]:
            self.upload_queue[database][table] = []

        # Append row to queue
        self.upload_queue[database][table].append(raw_row)

        return self._check_triggers(raw_row)

    def upload(self) -> List[Any]:
        """
        Publishes the queue to Google's Pub/Sub.

        Returns:
            List[Any]: Pub/Sub message IDs.
        """
        if self.always_ensure_topic:
            self.ensure_topic()

        futures = []
        ids = []

        # Upload each table as a separate Pub/Sub message
        for database, tables in self.upload_queue.items():
            for table, rows in tables.items():
                # Convert list of row dicts to one single bytestring
                bytestring = str.encode(
                    dumps(
                        {
                            "database": database,
                            "table": table,
                            "rows": [{"key": row.key, "columns": row.columns} for row in rows],
                        }
                    )
                )

                # Publish
                pubsub_future = self.pubsub_client.publish(self.topic_path, bytestring)
                futures.append(pubsub_future)

                # Add message ID to ID list when message is sent
                pubsub_future.add_done_callback(lambda f: ids.append(f.result()))

                # Update upload stats
                self._post_upload(rows)

        self.upload_queue.clear()
        self.upload_queue_byte_size = 0

        # Wait until all the messages are sent before returning. We can't use concurrent.futures.wait here since these
        # objects are instances of Google's own Future class and not the stdlib one.
        for future in futures:
            while future.running():
                time.sleep(0.1)

        return ids


class TimeSeriesUploadQueue(UploadQueue):
    """
    Upload queue for time series

    Args:
        cdf_client (CogniteClient): Cognite Data Fusion client
        post_upload_function (Callable[[int]): (Optional). A function that will be called after each upload. The
            function will be given one argument: A dict from time series ID to a list of the data points that were
            uploaded
        queue_threshold (int): (Optional). Maximum size of upload queue. Defaults to no queue (ie upload after each
            add_to_upload_queue).
        max_upload_interval (int): (Optional). Automatically trigger an upload each m seconds when run as a thread (use
            start/stop methods).
    """

    cdf_client: CogniteClient

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[Dict[_EitherId, DataPointList]], None]] = None,
        queue_threshold: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
    ):
        # Super sets post_upload and threshold
        super().__init__(post_upload_function, queue_threshold)

        self.upload_queue: Dict[_EitherId, DataPointList] = dict()

        self.max_upload_interval = max_upload_interval
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.stopping = threading.Event()

        self.cdf_client = cdf_client

        self.logger = logging.getLogger(__name__)

    def add_to_upload_queue(self, *, id: int = None, external_id: str = None, datapoints: DataPointList = []) -> None:
        """
        Add data points to upload queue. The queue will be uploaded if the queue byte size is larger than the threshold
        specified in the __init__.

        Args:
            id (int): Internal ID of time series. Either this or external_id must be set.
            external_id (str): External ID of time series. Either this or external_id must be set.
            datapoints (list): List of data points to add
        """
        either_id = _EitherId(id, external_id)

        if either_id not in self.upload_queue:
            self.upload_queue[either_id] = []

        self.upload_queue[either_id].extend(datapoints)
        self._check_triggers(datapoints)

    def _run(self):
        """
        Internal run method for upload thread
        """
        while not self.stopping.is_set():
            self.upload()
            time.sleep(self.max_upload_interval)

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        self.logger.debug("Triggering upload")

        upload_this = [
            {either_id.type(): either_id.content(), "datapoints": datapoints}
            for either_id, datapoints in self.upload_queue.items()
        ]
        self.upload_queue.clear()

        try:
            self.cdf_client.datapoints.insert_multiple(upload_this)

        except CogniteNotFoundError as ex:
            self.logger.error("Could not upload data points to %s: %s", str(ex.not_found), str(ex))

            # Get IDs of time series that exists, but failed because of the non-existing time series
            retry_these = [_EitherId(**id_dict) for id_dict in ex.failed if id_dict not in ex.not_found]

            # Remove entries with non-existing time series from upload queue
            upload_this = [
                entry for entry in upload_this if _EitherId(entry.get("id"), entry.get("externalId")) in retry_these
            ]

            # Upload remaining
            self.cdf_client.datapoints.insert_multiple(upload_this)

        self._post_upload(upload_this)
        self.upload_queue_byte_size = 0

    def start(self):
        """
        Start upload thread, this called the upload method every max_upload_interval seconds
        """
        if self.max_upload_interval is None:
            raise ValueError("No max_upload_interval given, can't start uploader thread")

        self.stopping.clear()
        self.thread.start()

    def stop(self, ensure_upload: bool = True):
        """
        Stop upload thread

        Args:
            ensure_upload (bool): (Optional). Call upload one last time after shutting down thread to ensure empty
                upload queue.
        """
        self.stopping.set()
        if ensure_upload:
            self.upload()


class EventUploadQueue(UploadQueue):
    """
    Upload queue for events

    Args:
        cdf_client (CogniteClient): Cognite Data Fusion client
        post_upload_function (Callable[[int]): (Optional). A function that will be called after each upload. The
            function will be given one argument: A list of the events created
        queue_threshold (int): (Optional). Maximum size of upload queue. Defaults to no queue (ie upload after each
            add_to_upload_queue).
        max_upload_interval (int): (Optional). Automatically trigger an upload each m seconds when run as a thread (use
            start/stop methods).
    """

    cdf_client: CogniteClient

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Event]], None]] = None,
        queue_threshold: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
    ):
        # Super sets post_upload and threshold
        super().__init__(post_upload_function, queue_threshold)

        self.upload_queue: List[Event] = []

        self.max_upload_interval = max_upload_interval
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.stopping = threading.Event()

        self.cdf_client = cdf_client

        self.logger = logging.getLogger(__name__)

    def add_to_upload_queue(self, event: Event) -> None:
        """
        Add event to upload queue. The queue will be uploaded if the queue byte size is larger than the threshold
        specified in the __init__.

        Args:
            event (Event): Event to add
        """
        self.upload_queue.append(event)
        self._check_triggers(event)

    def _run(self):
        """
        Internal run method for upload thread
        """
        while not self.stopping.is_set():
            self.upload()
            time.sleep(self.max_upload_interval)

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        self.logger.debug("Triggering upload")

        self.cdf_client.events.create(self.upload_queue)
        self._post_upload(self.upload_queue)
        self.upload_queue.clear()
        self.upload_queue_byte_size = 0

    def start(self):
        """
        Start upload thread, this called the upload method every max_upload_interval seconds
        """
        if self.max_upload_interval is None:
            raise ValueError("No max_upload_interval given, can't start uploader thread")

        self.stopping.clear()
        self.thread.start()

    def stop(self, ensure_upload: bool = True):
        """
        Stop upload thread

        Args:
            ensure_upload (bool): (Optional). Call upload one last time after shutting down thread to ensure empty
                upload queue.
        """
        self.stopping.set()
        if ensure_upload:
            self.upload()
