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

import threading
from concurrent.futures import ThreadPoolExecutor
from os import PathLike
from types import TracebackType
from typing import Any, Callable, List, Optional, Tuple, Type, Union

from requests import ConnectionError

from cognite.client import CogniteClient
from cognite.client.data_classes import Event, FileMetadata
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.uploader._base import (
    RETRIES,
    RETRY_BACKOFF_FACTOR,
    RETRY_DELAY,
    RETRY_MAX_DELAY,
    AbstractUploadQueue,
)
from cognite.extractorutils.uploader._metrics import (
    BYTES_UPLOADER_QUEUE_SIZE,
    BYTES_UPLOADER_QUEUED,
    BYTES_UPLOADER_WRITTEN,
    FILES_UPLOADER_QUEUE_SIZE,
    FILES_UPLOADER_QUEUED,
    FILES_UPLOADER_WRITTEN,
)
from cognite.extractorutils.util import retry


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
        cancellation_token: threading.Event = threading.Event(),
    ):
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )

        self.upload_queue: List[Tuple[FileMetadata, Union[str, PathLike]]] = []
        self.overwrite_existing = overwrite_existing

        self.files_queued = FILES_UPLOADER_QUEUED
        self.files_written = FILES_UPLOADER_WRITTEN
        self.queue_size = FILES_UPLOADER_QUEUE_SIZE

    def add_to_upload_queue(self, file_meta: FileMetadata, file_name: Union[str, PathLike]) -> None:
        """
        Add file to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.

        Args:
            file_meta: File metadata-object
            file_name: Path to file to be uploaded.
                If none, the file object will still be created, but no data is uploaded
        """
        with self.lock:
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

            self.files_written.inc(self.upload_queue_size)

            try:
                self._post_upload(self.upload_queue)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))
            self.upload_queue.clear()
            self.logger.info(f"Uploaded {self.upload_queue_size} files")
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    @retry(
        exceptions=(CogniteAPIError, ConnectionError),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _upload_single(self, index: int, file_name: Union[str, PathLike], file_meta: FileMetadata) -> None:
        # Upload file
        file_meta = self.cdf_client.files.upload(str(file_name), overwrite=self.overwrite_existing, **file_meta.dump())  # type: ignore

        # Update meta-object in queue
        self.upload_queue[index] = (file_meta, file_name)

    def _upload_batch(self) -> None:
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
            Number of events in queue
        """
        return self.upload_queue_size


class BytesUploadQueue(AbstractUploadQueue):
    """
    Upload queue for bytes

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the events that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
        overwrite_existing: If 'overwrite' is set to true, fields for the files found for externalIds can be overwritten
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Any]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        overwrite_existing: bool = False,
        cancellation_token: threading.Event = threading.Event(),
    ) -> None:
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )
        self.upload_queue: List[Tuple[bytes, FileMetadata]] = []
        self.overwrite_existing = overwrite_existing
        self.upload_queue_size = 0

        self.bytes_queued = BYTES_UPLOADER_QUEUED
        self.queue_size = BYTES_UPLOADER_QUEUE_SIZE
        self.bytes_written = BYTES_UPLOADER_WRITTEN

    def add_to_upload_queue(self, content: bytes, metadata: FileMetadata) -> None:
        """
        Add object to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.
        Args:
            content: bytes object to upload
            metadata: metadata for the given bytes object
        """
        with self.lock:
            self.upload_queue.append((content, metadata))
            self.upload_queue_size += 1
            self.bytes_queued.inc()
            self.queue_size.set(self.upload_queue_size)

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """
        if len(self.upload_queue) == 0:
            return

        with self.lock:
            # Upload frames in batches
            self._upload_batch()

            # Log stats
            self.bytes_written.inc(self.upload_queue_size)

            try:
                self._post_upload(self.upload_queue)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))

            # Clear queue
            self.upload_queue.clear()
            self.upload_queue_size = 0
            self.logger.info(f"Uploaded {self.upload_queue_size} files")
            self.queue_size.set(self.upload_queue_size)

    def _upload_batch(self) -> None:
        # Concurrently execute bytes-uploads
        with ThreadPoolExecutor(self.cdf_client.config.max_workers) as pool:
            for i, (frame, metadata) in enumerate(self.upload_queue):
                pool.submit(self._upload_single, i, frame, metadata)

    @retry(
        exceptions=(CogniteAPIError, ConnectionError),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _upload_single(self, index: int, content: bytes, metadata: FileMetadata) -> None:
        # Upload object
        file_meta_data: FileMetadata = self.cdf_client.files.upload_bytes(
            content, overwrite=self.overwrite_existing, **metadata.dump()
        )

        # Update meta-object in queue
        self.upload_queue[index] = (content, file_meta_data)

    def __enter__(self) -> "BytesUploadQueue":
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
            Number of events in queue
        """
        return self.upload_queue_size
