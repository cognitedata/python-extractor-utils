"""
Module containing upload queue classes. The UploadQueue classes chunks together items and uploads them together to CDF,
both to minimize the load on the API, and also to speed up uploading as requests can be slow.

Each upload queue comes with some configurable conditions that, when met, automatically triggers an upload.

**Note:** You cannot assume that an element is uploaded when it is added to the queue, since the upload may be
delayed. To ensure that everything is uploaded you should set the `post_upload_function` callback to verify. For
example, for a time series queue you might want to check the latest time stamp, as such (assuming incremental time
stamps and using timestamp-value tuples as data point format):


.. code-block:: python

    latest_point = {"timestamp": 0}

    def store_latest(time_series):
        # time_series is a list of dicts where each dict represents a time series
        latest_point["timestamp"] = max(
            latest_point["timestamp"],
            *[ts["datapoints"][-1][0] for ts in time_series]
        )

        # here you can also update metrics etc

    queue = TimeSeriesUploadQueue(
        cdf_client=my_cognite_client,
        post_upload_function=store_latest,
        max_upload_interval=1
    )

"""
import logging
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Event
from cognite.client.data_classes.raw import Row
from cognite.client.exceptions import CogniteNotFoundError

from ._inner_util import _resolve_log_level
from .util import EitherId

DataPointList = Union[
    List[Dict[Union[int, float, datetime], Union[int, float, str]]],
    List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
]


class AbstractUploadQueue(ABC):
    """
    Abstract uploader class.

    Args:
        post_upload_function: A function that will be called after each upload. The
            function will be given one argument: A list of the elements that were uploaded
        max_queue_size: Maximum byte size of upload queue. Defaults to no automatic uploading.
    """

    def __init__(
        self,
        post_upload_function: Optional[Callable[[List[Any]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
    ):
        """
        Called by subclasses. Saves info and inits upload queue.
        """
        self.threshold = max_queue_size if max_queue_size is not None else -1
        self.upload_queue_size = 0

        self.trigger_log_level = _resolve_log_level(trigger_log_level)
        self.logger = logging.getLogger(__name__)

        self.thread = threading.Thread(target=self._run, daemon=True, name=thread_name)
        self.lock = threading.RLock()
        self.stopping = threading.Event()

        self.max_upload_interval = max_upload_interval

        self.post_upload_function = post_upload_function

    def _check_triggers(self) -> None:
        """
        Check if upload triggers are met, call upload if they are. Called by subclasses.
        """
        # Check upload threshold
        if self.upload_queue_size > self.threshold >= 0:
            self.logger.log(self.trigger_log_level, "Upload queue reached threshold size, triggering upload")
            return self.upload()

        return None

    def _post_upload(self, uploaded: List[Any]) -> None:
        if self.post_upload_function is not None:
            try:
                self.post_upload_function(uploaded)
            except Exception as e:
                logging.getLogger(__name__).exception("Error during upload callback")

    @abstractmethod
    def add_to_upload_queue(self, *args) -> None:
        """
        Adds an element to the upload queue. The queue will be uploaded if the queue byte size is larger than the
        threshold specified in the config.
        """

    @abstractmethod
    def upload(self) -> None:
        """
        Uploads the queue.
        """

    def _run(self) -> None:
        """
        Internal run method for upload thread
        """
        while not self.stopping.is_set():
            try:
                self.upload()
            except Exception as e:
                self.logger.error("Unexpected error while uploading: %s. Skipping this upload.", str(e))
            time.sleep(self.max_upload_interval)

    def start(self) -> None:
        """
        Start upload thread, this called the upload method every max_upload_interval seconds
        """
        if self.max_upload_interval is None:
            raise ValueError("No max_upload_interval given, can't start uploader thread")

        self.stopping.clear()
        self.thread.start()

    def stop(self, ensure_upload: bool = True) -> None:
        """
        Stop upload thread

        Args:
            ensure_upload (bool): (Optional). Call upload one last time after shutting down thread to ensure empty
                upload queue.
        """
        self.stopping.set()
        if ensure_upload:
            self.upload()

    def __enter__(self) -> "AbstractUploadQueue":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Wraps around stop method, for use as context manager

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()

    def __len__(self) -> int:
        """
        The size of the upload queue

        Returns:
            Number of elements in queue
        """
        return self.upload_queue_size


class RawUploadQueue(AbstractUploadQueue):
    """
    Upload queue for RAW

    Args:
        cdf_client: Cognite Data Fusion client
        post_upload_function: A function that will be called after each upload. The
            function will be given one argument: An a list of the rows that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no automatic uploading.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use
            start/stop methods).
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Any]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
    ):
        # Super sets post_upload and threshold
        super().__init__(post_upload_function, max_queue_size, max_upload_interval, trigger_log_level, thread_name)

        self.upload_queue: Dict[str, Dict[str, List[Row]]] = dict()

        self.lock = threading.RLock()

        self.cdf_client = cdf_client

    def add_to_upload_queue(self, database: str, table: str, raw_row: Row) -> None:
        """
        Adds a row to the upload queue. The queue will be uploaded if the queue byte size is larger than the threshold
        specified in the __init__.

        Args:
            database: The database to upload the Raw object to
            table: The table to upload the Raw object to
            raw_row: The row object

        Returns:
            None.
        """
        with self.lock:
            # Ensure that the dicts has correct keys
            if database not in self.upload_queue:
                self.upload_queue[database] = dict()
            if table not in self.upload_queue[database]:
                self.upload_queue[database][table] = []

            # Append row to queue
            self.upload_queue[database][table].append(raw_row)
            self.upload_queue_size += 1

            self._check_triggers()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        with self.lock:
            for database, tables in self.upload_queue.items():
                for table, rows in tables.items():
                    # Upload
                    self.cdf_client.raw.rows.insert(db_name=database, table_name=table, row=rows, ensure_parent=True)

                    # Perform post-upload logic if applicable
                    try:
                        self._post_upload(rows)
                    except Exception as e:
                        self.logger.error("Error in upload callback: %s", str(e))

            self.upload_queue.clear()
            self.upload_queue_size = 0


class TimeSeriesUploadQueue(AbstractUploadQueue):
    """
    Upload queue for time series

    Args:
        cdf_client: Cognite Data Fusion client
        post_upload_function: A function that will be called after each upload. The
            function will be given one argument: A dict from time series ID to a list of the data points that were
            uploaded
        max_queue_size: Maximum size of upload queue. Defaults to no automatic uploading.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use
            start/stop methods).
    """

    cdf_client: CogniteClient

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Dict[str, Union[str, DataPointList]]]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
    ):
        # Super sets post_upload and threshold
        super().__init__(post_upload_function, max_queue_size, max_upload_interval, trigger_log_level, thread_name)

        self.upload_queue: Dict[EitherId, DataPointList] = dict()

        self.cdf_client = cdf_client

    def add_to_upload_queue(self, *, id: int = None, external_id: str = None, datapoints: DataPointList = []) -> None:
        """
        Add data points to upload queue. The queue will be uploaded if the queue byte size is larger than the threshold
        specified in the __init__.

        Args:
            id: Internal ID of time series. Either this or external_id must be set.
            external_id: External ID of time series. Either this or external_id must be set.
            datapoints: List of data points to add
        """
        either_id = EitherId(id=id, external_id=external_id)

        self.lock.acquire()
        try:
            if either_id not in self.upload_queue:
                self.upload_queue[either_id] = []

            self.upload_queue[either_id].extend(datapoints)
            self.upload_queue_size += len(datapoints)

            self._check_triggers()

        finally:
            self.lock.release()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        upload_this = [
            {either_id.type(): either_id.content(), "datapoints": datapoints}
            for either_id, datapoints in self.upload_queue.items()
            if len(datapoints) > 0
        ]

        self.lock.acquire()

        try:
            if len(upload_this) == 0:
                return

            self.logger.log(self.trigger_log_level, "Triggering scheduled upload")

            try:
                self.cdf_client.datapoints.insert_multiple(upload_this)

            except CogniteNotFoundError as ex:
                self.logger.error("Could not upload data points to %s: %s", str(ex.not_found), str(ex))

                # Get IDs of time series that exists, but failed because of the non-existing time series
                retry_these = [EitherId(**id_dict) for id_dict in ex.failed if id_dict not in ex.not_found]

                # Remove entries with non-existing time series from upload queue
                upload_this = [
                    entry
                    for entry in upload_this
                    if EitherId(id=entry.get("id"), external_id=entry.get("externalId")) in retry_these
                ]

                # Upload remaining
                self.cdf_client.datapoints.insert_multiple(upload_this)

            try:
                self._post_upload(upload_this)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))

            self.upload_queue.clear()
            self.upload_queue_size = 0

        finally:
            self.lock.release()


class EventUploadQueue(AbstractUploadQueue):
    """
    Upload queue for events

    Args:
        cdf_client: Cognite Data Fusion client
        post_upload_function: A function that will be called after each upload. The
            function will be given one argument: A list of the events created
        max_queue_size: Maximum size of upload queue. Defaults to no automatic uploading.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use
            start/stop methods).
    """

    cdf_client: CogniteClient

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Event]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
    ):
        # Super sets post_upload and threshold
        super().__init__(post_upload_function, max_queue_size, max_upload_interval, trigger_log_level, thread_name)

        self.upload_queue: List[Event] = []

        self.cdf_client = cdf_client

    def add_to_upload_queue(self, event: Event) -> None:
        """
        Add event to upload queue. The queue will be uploaded if the queue byte size is larger than the threshold
        specified in the __init__.

        Args:
            event: Event to add
        """
        self.lock.acquire()

        try:
            self.upload_queue.append(event)
            self.upload_queue_size += 1

            self._check_triggers()

        finally:
            self.lock.release()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        self.logger.log(self.trigger_log_level, "Triggering scheduled upload")

        self.lock.acquire()

        try:
            self.cdf_client.events.create(self.upload_queue)
            try:
                self._post_upload(self.upload_queue)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))
            self.upload_queue.clear()
            self.upload_queue_size = 0

        finally:
            self.lock.release()
