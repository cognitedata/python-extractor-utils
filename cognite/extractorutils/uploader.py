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

"""
Module containing upload queue classes. The UploadQueue classes chunks together items and uploads them together to CDF,
both to minimize the load on the API, and also to speed up uploading as requests can be slow.

Each upload queue comes with some configurable conditions that, when met, automatically triggers an upload.

**Note:** You cannot assume that an element is uploaded when it is added to the queue, since the upload may be
delayed. To ensure that everything is uploaded you should set the `post_upload_function` callback to verify. For
example, for a time series queue you might want to check the latest time stamp, as such (assuming incremental time
stamps and using timestamp-value tuples as data point format):


.. code-block:: python

    state_store = LocalStateStore("states.json")

    queue = TimeSeriesUploadQueue(
        cdf_client=my_cognite_client,
        post_upload_function=state_store.post_upload_handler(),
        max_upload_interval=1
    )

"""

import logging
import threading
import time
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from os import PathLike
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import arrow
from arrow import Arrow
from prometheus_client import Counter, Gauge, Histogram
from requests.exceptions import ConnectionError
from retry import retry

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, FileMetadata, Sequence, SequenceData, TimeSeries
from cognite.client.data_classes.raw import Row
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError, CogniteReadTimeout

from ._inner_util import _resolve_log_level
from .util import EitherId

RETRY_BACKOFF_FACTOR = 1.5
RETRY_MAX_DELAY = 15
RETRY_DELAY = 5
RETRIES = 10

DataPointList = Union[
    List[Dict[Union[int, float, datetime], Union[int, float, str]]],
    List[Tuple[Union[int, float, datetime], Union[int, float, str]]],
]


class AbstractUploadQueue(ABC):
    """
    Abstract uploader class.

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the elements that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Any]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancelation_token: threading.Event = threading.Event(),
    ):
        self.cdf_client = cdf_client

        self.threshold = max_queue_size if max_queue_size is not None else -1
        self.upload_queue_size = 0

        self.trigger_log_level = _resolve_log_level(trigger_log_level)
        self.logger = logging.getLogger(__name__)

        self.thread = threading.Thread(target=self._run, daemon=True, name=thread_name)
        self.lock = threading.RLock()
        self.cancelation_token: threading.Event = cancelation_token

        self.max_upload_interval = max_upload_interval

        self.post_upload_function = post_upload_function

    def _check_triggers(self) -> None:
        """
        Check if upload triggers are met, call upload if they are. Called by subclasses.
        """
        if self.upload_queue_size > self.threshold >= 0:
            self.logger.log(
                self.trigger_log_level,
                f"Upload queue reached threshold size {self.upload_queue_size}/{self.threshold}, triggering upload",
            )
            return self.upload()

        return None

    def _post_upload(self, uploaded: List[Any]) -> None:
        """
        Perform post_upload_function to uploaded data, if applicable

        Args:
            uploaded: List of uploaded data
        """
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
        while not self.cancelation_token.wait(timeout=self.max_upload_interval):
            try:
                self.logger.log(self.trigger_log_level, "Triggering scheduled upload")
                self.upload()
            except Exception as e:
                self.logger.error("Unexpected error while uploading: %s. Skipping this upload.", str(e))

        # trigger stop event explicitly to drain the queue
        self.stop(ensure_upload=True)

    def start(self) -> None:
        """
        Start upload thread if max_upload_interval is set, this called the upload method every max_upload_interval
        seconds.
        """
        if self.max_upload_interval is not None:
            self.cancelation_token.clear()
            self.thread.start()

    def stop(self, ensure_upload: bool = True) -> None:
        """
        Stop upload thread if running, and ensures that the upload queue is empty if ensure_upload is True.

        Args:
            ensure_upload (bool): (Optional). Call upload one last time after shutting down thread to ensure empty
                upload queue.
        """
        self.cancelation_token.set()
        if ensure_upload:
            self.upload()


@dataclass(frozen=True)
class TimestampedObject:
    payload: Any
    created: Arrow


RAW_UPLOADER_ROWS_QUEUED = Counter(
    "cognite_raw_uploader_rows_queued", "Total number of records queued", labelnames=["destination"]
)
RAW_UPLOADER_ROWS_WRITTEN = Counter(
    "cognite_raw_uploader_rows_written", "Total number of records written", labelnames=["destination"]
)
RAW_UPLOADER_ROWS_DUPLICATES = Counter(
    "cognite_raw_uploader_rows_duplicates", "Total number of duplicates found", labelnames=["destination"]
)
RAW_UPLOADER_QUEUE_SIZE = Gauge("cognite_raw_uploader_queue_size", "Internal queue size")
RAW_UPLOADER_LATENCY = Histogram(
    "cognite_raw_uploader_latency",
    "Distribution of times in seconds records spend in the queue",
    labelnames=["destination"],
)

TIMESERIES_UPLOADER_POINTS_QUEUED = Counter(
    "cognite_timeseries_uploader_points_queued", "Total number of datapoints queued", labelnames=["destination"]
)

TIMESERIES_UPLOADER_POINTS_WRITTEN = Counter(
    "cognite_timeseries_uploader_points_written", "Total number of datapoints written", labelnames=["destination"],
)

TIMESERIES_UPLOADER_QUEUE_SIZE = Gauge("cognite_timeseries_uploader_queue_size", "Internal queue size")

TIMESERIES_UPLOADER_LATENCY = Histogram(
    "cognite_timeseries_uploader_latency",
    "Distribution of times in seconds records spend in the queue",
    labelnames=["destination"],
)

SEQUENCES_UPLOADER_POINTS_QUEUED = Counter(
    "cognite_sequences_uploader_points_queued", "Total number of sequences queued", labelnames=["destination"]
)

SEQUENCES_UPLOADER_POINTS_WRITTEN = Counter(
    "cognite_sequences_uploader_points_written", "Total number of sequences written", labelnames=["destination"],
)

SEQUENCES_UPLOADER_QUEUE_SIZE = Gauge("cognite_sequences_uploader_queue_size", "Internal queue size")

SEQUENCES_UPLOADER_LATENCY = Histogram(
    "cognite_sequences_uploader_latency",
    "Distribution of times in seconds records spend in the queue",
    labelnames=["destination"],
)

EVENTS_UPLOADER_QUEUED = Counter("cognite_events_uploader_queued", "Total number of events queued")

EVENTS_UPLOADER_WRITTEN = Counter("cognite_events_uploader_written", "Total number of events written")

EVENTS_UPLOADER_QUEUE_SIZE = Gauge("cognite_events_uploader_queue_size", "Internal queue size")

EVENTS_UPLOADER_LATENCY = Histogram(
    "cognite_events_uploader_latency", "Distribution of times in seconds records spend in the queue"
)

FILES_UPLOADER_QUEUED = Counter("cognite_files_uploader_queued", "Total number of files queued")

FILES_UPLOADER_WRITTEN = Counter("cognite_files_uploader_written", "Total number of files written")

FILES_UPLOADER_QUEUE_SIZE = Gauge("cognite_files_uploader_queue_size", "Internal queue size")

FILES_UPLOADER_LATENCY = Histogram(
    "cognite_files_uploader_latency", "Distribution of times in seconds records spend in the queue"
)


class RawUploadQueue(AbstractUploadQueue):
    """
    Upload queue for RAW

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the rows that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Any]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancelation_token: threading.Event = threading.Event(),
    ):
        # Super sets post_upload and thresholds
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancelation_token,
        )
        self.upload_queue: Dict[str, Dict[str, List[TimestampedObject]]] = dict()

        # It is a hack since Prometheus client registers metrics on object creation, so object has to be created once
        self.rows_queued = RAW_UPLOADER_ROWS_QUEUED
        self.rows_written = RAW_UPLOADER_ROWS_WRITTEN
        self.rows_duplicates = RAW_UPLOADER_ROWS_DUPLICATES
        self.queue_size = RAW_UPLOADER_QUEUE_SIZE
        self.latency = RAW_UPLOADER_LATENCY

    def add_to_upload_queue(self, database: str, table: str, raw_row: Row) -> None:
        """
        Adds a row to the upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.

        Args:
            database: The database to upload the Raw object to
            table: The table to upload the Raw object to
            raw_row: The row object
        """
        with self.lock:
            # Ensure that the dicts has correct keys
            if database not in self.upload_queue:
                self.upload_queue[database] = dict()
            if table not in self.upload_queue[database]:
                self.upload_queue[database][table] = []

            # Append row to queue
            self.upload_queue[database][table].append(TimestampedObject(payload=raw_row, created=Arrow.utcnow()))
            self.upload_queue_size += 1
            self.rows_queued.labels(f"{database}:{table}").inc()
            self.queue_size.set(self.upload_queue_size)

            self._check_triggers()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        with self.lock:
            for database, tables in self.upload_queue.items():
                for table, rows in tables.items():
                    _labels = [f"{database}:{table}"]

                    # Deduplicate
                    # In case of duplicate keys, the first key is preserved, and the last value is preserved.
                    patch: Dict[str, Row] = {r.payload.key: r.payload for r in rows}
                    self.rows_duplicates.labels(_labels).inc(len(rows) - len(patch))

                    self._upload_batch(database=database, table=table, patch=list(patch.values()))
                    self.rows_written.labels(_labels).inc(len(patch))
                    _written: Arrow = arrow.utcnow()
                    for r in rows:
                        self.latency.labels(_labels).observe((_written - r.created).total_seconds())

                    # Perform post-upload logic if applicable
                    try:
                        self._post_upload(rows)
                    except Exception as e:
                        self.logger.error("Error in upload callback: %s", str(e))

            self.upload_queue.clear()
            self.logger.info(f"Uploaded {self.upload_queue_size} rows in total")
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    @retry(
        exceptions=(CogniteAPIError, ConnectionError, CogniteReadTimeout),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _upload_batch(self, database: str, table: str, patch: List[Row]):
        # Upload
        self.cdf_client.raw.rows.insert(db_name=database, table_name=table, row=patch, ensure_parent=True)

    def __enter__(self) -> "RawUploadQueue":
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


def default_time_series_factory(external_id: str, datapoints: DataPointList) -> TimeSeries:
    """
    Default time series factory used when create_missing in a TimeSeriesUploadQueue is given as a boolean.

    Args:
        external_id: External ID of time series to create
        datapoints: The list of datapoints that were tried to be inserted

    Returns:
        A TimeSeries object with external_id set, and the is_string automatically detected
    """
    is_string = (
        isinstance(datapoints[0].get("value"), str)
        if isinstance(datapoints[0], dict)
        else isinstance(datapoints[0][1], str)
    )
    return TimeSeries(external_id=external_id, is_string=is_string)


class TimeSeriesUploadQueue(AbstractUploadQueue):
    """
    Upload queue for time series

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of dicts containing the datapoints that were uploaded (on the same format as the kwargs in
            datapoints upload in the Cognite SDK).
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
        create_missing: Create missing time series if possible (ie, if external id is used). Either given as a boolean
            (True would auto-create a time series with nothing but an external ID), or as a factory function taking an
            external ID and a list of datapoints about to be inserted and returning a TimeSeries object.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Dict[str, Union[str, DataPointList]]]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        create_missing: Union[Callable[[str, DataPointList], TimeSeries], bool] = False,
        cancelation_token: threading.Event = threading.Event(),
    ):
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancelation_token,
        )

        if isinstance(create_missing, bool):
            self.create_missing = create_missing
            self.missing_factory = default_time_series_factory
        else:
            self.create_missing = True
            self.missing_factory = create_missing

        self.upload_queue: Dict[EitherId, DataPointList] = dict()

        self.points_queued = TIMESERIES_UPLOADER_POINTS_QUEUED
        self.points_written = TIMESERIES_UPLOADER_POINTS_WRITTEN
        self.queue_size = TIMESERIES_UPLOADER_QUEUE_SIZE
        self.latency = TIMESERIES_UPLOADER_LATENCY
        self.latency_zero_point = arrow.utcnow()

    def add_to_upload_queue(self, *, id: int = None, external_id: str = None, datapoints: DataPointList = []) -> None:
        """
        Add data points to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.

        Args:
            id: Internal ID of time series. Either this or external_id must be set.
            external_id: External ID of time series. Either this or external_id must be set.
            datapoints: List of data points to add
        """
        either_id = EitherId(id=id, external_id=external_id)
        _labels = [str(either_id.content())]

        with self.lock:
            if either_id not in self.upload_queue:
                self.upload_queue[either_id] = []

            if self.upload_queue_size == 0:
                self.latency_zero_point = arrow.utcnow()

            self.upload_queue[either_id].extend(datapoints)
            self.points_queued.labels(_labels).inc(len(datapoints))
            self.latency.labels(_labels).observe((arrow.utcnow() - self.latency_zero_point).total_seconds())
            self.upload_queue_size += len(datapoints)
            self.queue_size.set(self.upload_queue_size)

            self._check_triggers()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        with self.lock:
            upload_this = self._upload_batch(
                [
                    {either_id.type(): either_id.content(), "datapoints": datapoints}
                    for either_id, datapoints in self.upload_queue.items()
                    if len(datapoints) > 0
                ]
            )

            for either_id, datapoints in self.upload_queue.items():
                self.points_written.labels([str(either_id.content())]).inc(len(datapoints))

            try:
                self._post_upload(upload_this)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))

            self.upload_queue.clear()
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    @retry(
        exceptions=(CogniteAPIError, ConnectionError),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _upload_batch(self, upload_this: List[Dict], retries=5) -> List[Dict]:
        if len(upload_this) == 0:
            return upload_this

        try:
            self.cdf_client.datapoints.insert_multiple(upload_this)

        except CogniteNotFoundError as ex:
            if not retries:
                raise ex

            if not self.create_missing:
                self.logger.error("Could not upload data points to %s: %s", str(ex.not_found), str(ex))

            # Get IDs of time series that exists, but failed because of the non-existing time series
            retry_these = [EitherId(**id_dict) for id_dict in ex.failed if id_dict not in ex.not_found]

            if self.create_missing:
                # Get the time series that can be created
                create_these_ids = [id_dict["externalId"] for id_dict in ex.not_found if "externalId" in id_dict]
                datapoints_lists: Dict[str, DataPointList] = {
                    ts_dict["externalId"]: ts_dict["datapoints"]
                    for ts_dict in upload_this
                    if ts_dict["externalId"] in create_these_ids
                }

                self.logger.info(f"Creating {len(create_these_ids)} time series")
                self.cdf_client.time_series.create(
                    [
                        self.missing_factory(external_id, datapoints_lists[external_id])
                        for external_id in create_these_ids
                    ]
                )

                retry_these.extend([EitherId(external_id=i) for i in create_these_ids])

                if len(ex.not_found) != len(create_these_ids):
                    missing = [id_dict for id_dict in ex.not_found if id_dict.get("externalId") not in retry_these]
                    self.logger.error(
                        f"{len(ex.not_found) - len(create_these_ids)} time series not found, and could not be created automatically:\n"
                        + str(missing)
                        + "\nData will be dropped"
                    )

            # Remove entries with non-existing time series from upload queue
            upload_this = [
                entry
                for entry in upload_this
                if EitherId(id=entry.get("id"), external_id=entry.get("externalId")) in retry_these
            ]

            # Upload remaining
            self._upload_batch(upload_this, retries - 1)

        return upload_this

    def __enter__(self) -> "TimeSeriesUploadQueue":
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
            Number of data points in queue
        """
        return self.upload_queue_size


class EventUploadQueue(AbstractUploadQueue):
    """
    Upload queue for events

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the events that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Event]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancelation_token: threading.Event = threading.Event(),
    ):
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancelation_token,
        )

        self.upload_queue: List[Event] = []

        self.events_queued = EVENTS_UPLOADER_QUEUED
        self.events_written = EVENTS_UPLOADER_WRITTEN
        self.queue_size = EVENTS_UPLOADER_QUEUE_SIZE
        self.latency = EVENTS_UPLOADER_LATENCY
        self.latency_zero_point = arrow.utcnow()

    def add_to_upload_queue(self, event: Event) -> None:
        """
        Add event to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.

        Args:
            event: Event to add
        """
        with self.lock:
            if self.upload_queue_size == 0:
                self.latency_zero_point = arrow.utcnow()

            self.upload_queue.append(event)
            self.events_queued.inc()
            self.upload_queue_size += 1
            self.queue_size.set(self.upload_queue_size)

            self._check_triggers()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        with self.lock:
            self._upload_batch()

            self.latency.observe((arrow.utcnow() - self.latency_zero_point).total_seconds())
            self.events_written.inc(self.upload_queue_size)

            try:
                self._post_upload(self.upload_queue)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))
            self.upload_queue.clear()
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    @retry(
        exceptions=(CogniteAPIError, ConnectionError),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _upload_batch(self):
        self.cdf_client.events.create([e for e in self.upload_queue])

    def __enter__(self) -> "EventUploadQueue":
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
            Number of events in queue
        """
        return self.upload_queue_size


class SequenceUploadQueue(AbstractUploadQueue):
    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Any]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        create_missing=False,
        cancelation_token: threading.Event = threading.Event(),
    ):
        """
        Args:
            cdf_client: Cognite Data Fusion client to use
            post_upload_function: A function that will be called after each upload. The function will be given one argument:
                A list of the events that were uploaded.
            max_queue_size: Maximum size of upload queue. Defaults to no max size.
            max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
                methods).
            trigger_log_level: Log level to log upload triggers to.
            thread_name: Thread name of uploader thread.
            create_missing: Create missing time series if possible (ie, if external id is used)
        """

        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancelation_token,
        )
        self.upload_queue: Dict[EitherId, SequenceData] = dict()
        self.sequence_metadata: Dict[EitherId, Dict[str, Union[str, int, float]]] = dict()
        self.sequence_asset_ids: Dict[EitherId, str] = dict()
        self.sequence_dataset_ids: Dict[EitherId, str] = dict()
        self.column_definitions: Dict[EitherId, List[Dict[str, str]]] = dict()
        self.create_missing = create_missing

        self.points_queued = SEQUENCES_UPLOADER_POINTS_QUEUED
        self.points_written = SEQUENCES_UPLOADER_POINTS_WRITTEN
        self.queue_size = SEQUENCES_UPLOADER_QUEUE_SIZE
        self.latency = SEQUENCES_UPLOADER_LATENCY
        self.latency_zero_point = arrow.utcnow()

    def set_sequence_metadata(
        self,
        metadata: Dict[str, Union[str, int, float]],
        id: int = None,
        external_id: str = None,
        asset_external_id: str = None,
        dataset_external_id: str = None,
    ) -> None:
        """
        Set sequence metadata. Metadata will be cached until the sequence is created. The metadata will be updated
        if the sequence already exists

        Args:
            metadata: Sequence metadata
            id: Sequence internal ID
                Use if external_id is None
            external_id: Sequence external ID
                Us if id is None
            asset_external_id: Sequence asset ID
            dataset_external_id: Sequence dataset id
        """
        either_id = EitherId(id=id, external_id=external_id)
        self.sequence_metadata[either_id] = metadata
        self.sequence_asset_ids[either_id] = asset_external_id
        self.sequence_dataset_ids[either_id] = dataset_external_id

    def set_sequence_column_definition(
        self, col_def: List[Dict[str, str]], id: int = None, external_id: str = None
    ) -> None:
        """
        Set sequence column definition

        Args:
            col_def: Sequence column definition
            id: Sequence internal ID
                Use if external_id is None
            external_id: Sequence external ID
                Us if id is None
        """
        either_id = EitherId(id=id, external_id=external_id)
        self.column_definitions[either_id] = col_def

    def add_to_upload_queue(
        self,
        rows: Union[
            Dict[int, List[Union[int, float, str]]],
            List[Tuple[int, Union[int, float, str]]],
            List[Dict[str, Any]],
            SequenceData,
        ],
        column_external_ids: Optional[List[str]] = None,
        id: int = None,
        external_id: str = None,
    ) -> None:
        """
        Add sequence rows to upload queue. Mirrors implementation of SequenceApi.insert. Inserted rows will be
        cached until uploaded

        Args:
            rows: The rows to be inserted. Can either be a list of tuples, a list of ["rownumber": ..., "values": ...]
                objects, a dictionary of rowNumber: data, or a SequenceData object.
            column_external_ids: List of external id for the columns of the sequence
            id: Sequence internal ID
                Use if external_id is None
            external_id: Sequence external ID
                Us if id is None
        """

        if len(rows) == 0:
            pass

        either_id = EitherId(id=id, external_id=external_id)

        if isinstance(rows, SequenceData):
            # Already in desired format
            pass
        elif isinstance(rows, dict):
            rows = [{"rowNumber": row_number, "values": values} for row_number, values in rows.items()]

            rows = SequenceData(id=id, external_id=id, rows=rows, columns=column_external_ids)

        elif isinstance(rows, list):
            if isinstance(rows[0], tuple) or isinstance(rows[0], list):
                rows = [{"rowNumber": row_number, "values": values} for row_number, values in rows]

            rows = SequenceData(id=id, external_id=id, rows=rows, columns=column_external_ids)
        else:
            raise TypeError("Unsupported type for sequence rows: {}".format(type(rows)))

        with self.lock:
            seq = self.upload_queue.get(either_id)
            if seq is not None:
                # Update sequence
                seq.values = [*seq.values, *rows.values]
                seq.row_numbers = [*seq.values, *rows.row_numbers]

                self.upload_queue[either_id] = seq
            else:
                self.upload_queue[either_id] = rows
            self.upload_queue_size = len(self.upload_queue)
            self.queue_size.set(self.upload_queue_size)
            self.points_queued.labels([str(either_id.content())]).inc()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        with self.lock:
            for either_id, upload_this in self.upload_queue.items():
                _labels = [str(either_id.content())]
                self._upload_single(either_id, upload_this)
                self.latency.labels(_labels).observe((arrow.utcnow() - self.latency_zero_point).total_seconds())
                self.points_written.labels(_labels).inc()

            try:
                self._post_upload([seqdata for _, seqdata in self.upload_queue.items()])
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))

            self.upload_queue.clear()
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    @retry(
        exceptions=CogniteAPIError,
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _upload_single(self, either_id: EitherId, upload_this: SequenceData) -> SequenceData:

        self.logger.debug("Writing {} rows to sequence {}".format(len(upload_this.values), either_id))

        try:
            self.cdf_client.sequences.data.insert(
                id=either_id.internal_id, external_id=either_id.external_id, rows=upload_this, column_external_ids=None
            )
        except CogniteNotFoundError as ex:
            if self.create_missing:

                # Create missing sequence
                self._create_or_update(either_id)

                # Retry
                self.cdf_client.sequences.data.insert(
                    id=either_id.internal_id,
                    external_id=either_id.external_id,
                    rows=upload_this,
                    column_external_ids=None,
                )
            else:
                raise ex

        return upload_this

    def _create_or_update(self, either_id: EitherId) -> None:
        """
        Create or update sequence, based on provided metadata and column definitions

        Args:
            either_id: Id/External Id of sequence to be updated
        """

        column_def = self.column_definitions.get(either_id)

        try:
            seq = self.cdf_client.sequences.create(
                Sequence(
                    id=either_id.internal_id,
                    external_id=either_id.external_id,
                    metadata=self.sequence_metadata.get(either_id, None),
                    asset_id=self.sequence_asset_ids.get(either_id, None),
                    data_set_id=self.sequence_asset_ids.get(either_id, None),
                    columns=column_def,
                )
            )

        except CogniteDuplicatedError:

            self.logger.info("Sequnce already exist: {}".format(either_id))
            seq = self.cdf_client.sequences.retrieve(id=either_id.internal_id, external_id=either_id.external_id)

        # Update definition of cached sequence
        cseq = self.upload_queue[either_id]
        cseq.columns = seq.columns

    def __enter__(self, exc_type, exc_val, exc_tb) -> "SequenceUploadQueue":
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
            Number of data points in queue
        """
        return self.upload_queue_size


class FileUploadQueue(AbstractUploadQueue):
    """
    Upload queue for files

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the events that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Event]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        overwrite_existing: bool = False,
        cancelation_token: threading.Event = threading.Event(),
    ):
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancelation_token,
        )

        self.upload_queue: List[Tuple[FileMetadata, Union[str, PathLike]]] = []
        self.overwrite_existing = overwrite_existing

        self.files_queued = FILES_UPLOADER_QUEUED
        self.files_written = FILES_UPLOADER_WRITTEN
        self.queue_size = FILES_UPLOADER_QUEUE_SIZE
        self.latency = FILES_UPLOADER_LATENCY
        self.latency_zero_point = arrow.utcnow()

    def add_to_upload_queue(self, file_meta: FileMetadata, file_name: Union[str, PathLike] = None) -> None:
        """
        Add file to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.

        Args:
            file_meta: File metadata-object
            file_name: Path to file to be uploaded.
                If none, the file object will still be created, but no data is uploaded
        """
        with self.lock:
            if self.upload_queue_size == 0:
                self.latency_zero_point = arrow.utcnow()

            self.upload_queue.append((file_meta, file_name))
            self.upload_queue_size += 1
            self.files_queued.inc()
            self.queue_size.set(self.upload_queue_size)

            self._check_triggers()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        with self.lock:
            self._upload_batch()

            self.latency.observe((arrow.utcnow() - self.latency_zero_point).total_seconds())
            self.files_written.inc(self.upload_queue_size)

            try:
                self._post_upload(self.upload_queue)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))
            self.upload_queue.clear()
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    @retry(
        exceptions=(CogniteAPIError, ConnectionError),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _upload_single(self, index, file_name, file_meta):
        # Upload file
        file_meta = self.cdf_client.files.upload(file_name, overwrite=self.overwrite_existing, **file_meta.dump())

        # Update meta-object in queue
        self.upload_queue[index] = (file_meta, file_name)

    def _upload_batch(self):
        # Concurrently execute file-uploads

        with ThreadPoolExecutor(self.cdf_client.config.max_workers) as pool:
            for i, (file_meta, file_name) in enumerate(self.upload_queue):
                pool.submit(self._upload_single, i, file_name, file_meta)

    def __enter__(self) -> "FileUploadQueue":
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
            Number of events in queue
        """
        return self.upload_queue_size
