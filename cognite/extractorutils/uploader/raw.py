#  Copyright 2023 Cognite AS
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

from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Type

import arrow
from arrow import Arrow

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.uploader._base import (
    RETRIES,
    RETRY_BACKOFF_FACTOR,
    RETRY_DELAY,
    RETRY_MAX_DELAY,
    AbstractUploadQueue,
    TimestampedObject,
)
from cognite.extractorutils.uploader._metrics import (
    RAW_UPLOADER_QUEUE_SIZE,
    RAW_UPLOADER_ROWS_DUPLICATES,
    RAW_UPLOADER_ROWS_QUEUED,
    RAW_UPLOADER_ROWS_WRITTEN,
)
from cognite.extractorutils.util import cognite_exceptions, retry


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
        cancellation_token: Optional[CancellationToken] = None,
    ):
        # Super sets post_upload and thresholds
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )
        self.upload_queue: Dict[str, Dict[str, List[TimestampedObject]]] = {}

        # It is a hack since Prometheus client registers metrics on object creation, so object has to be created once
        self.rows_queued = RAW_UPLOADER_ROWS_QUEUED
        self.rows_written = RAW_UPLOADER_ROWS_WRITTEN
        self.rows_duplicates = RAW_UPLOADER_ROWS_DUPLICATES
        self.queue_size = RAW_UPLOADER_QUEUE_SIZE

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
                self.upload_queue[database] = {}
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

        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def _upload_batch(database: str, table: str, patch: List[Row]) -> None:
            # Upload
            self.cdf_client.raw.rows.insert(db_name=database, table_name=table, row=patch, ensure_parent=True)

        if len(self.upload_queue) == 0:
            return

        with self.lock:
            for database, tables in self.upload_queue.items():
                for table, rows in tables.items():
                    _labels = f"{database}:{table}"

                    # Deduplicate
                    # In case of duplicate keys, the first key is preserved, and the last value is preserved.
                    patch: Dict[str, Row] = {r.payload.key: r.payload for r in rows}
                    self.rows_duplicates.labels(_labels).inc(len(rows) - len(patch))

                    _upload_batch(database=database, table=table, patch=list(patch.values()))
                    self.rows_written.labels(_labels).inc(len(patch))
                    _written: Arrow = arrow.utcnow()

                    # Perform post-upload logic if applicable
                    try:
                        self._post_upload(rows)
                    except Exception as e:
                        self.logger.error("Error in upload callback: %s", str(e))

            self.upload_queue.clear()
            self.logger.info(f"Uploaded {self.upload_queue_size} raw rows")
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    def __enter__(self) -> "RawUploadQueue":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
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
