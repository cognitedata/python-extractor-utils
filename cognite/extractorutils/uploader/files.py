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
from concurrent.futures import Future, ThreadPoolExecutor
from io import BytesIO
from os import PathLike
from types import TracebackType
from typing import BinaryIO, Callable, List, Optional, Tuple, Type, Union

from requests import ConnectionError

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.uploader._base import (
    RETRIES,
    RETRY_BACKOFF_FACTOR,
    RETRY_DELAY,
    RETRY_MAX_DELAY,
    AbstractUploadQueue,
)
from cognite.extractorutils.uploader._metrics import (
    FILES_UPLOADER_QUEUE_SIZE,
    FILES_UPLOADER_QUEUED,
    FILES_UPLOADER_WRITTEN,
)
from cognite.extractorutils.util import retry


class IOFileUploadQueue(AbstractUploadQueue):
    """
    Upload queue for files using BinaryIO

    Note that if the upload fails, the stream needs to be restarted, so
    the enqueued callback needs to produce a new IO object for each call.

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the events that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
        max_parallelism: Maximum number of parallel uploads. If this is greater than 0,
            the largest of this and client.config.max_workers is used to limit the number
            of parallel uploads. This may be important if the IO objects being processed
            also load data from an external system.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[FileMetadata]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        overwrite_existing: bool = False,
        cancellation_token: threading.Event = threading.Event(),
        max_parallelism: int = 0,
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

        self.upload_queue: List[Tuple[FileMetadata, Union[str, Callable[[], BinaryIO]]]] = []
        self.overwrite_existing = overwrite_existing

        self.parallelism = self.cdf_client.config.max_workers
        if max_parallelism > 0 and max_parallelism < self.parallelism:
            self.parallelism = max_parallelism
        if self.parallelism <= 0:
            self.parallelism = 4

        self.files_queued = FILES_UPLOADER_QUEUED
        self.files_written = FILES_UPLOADER_WRITTEN
        self.queue_size = FILES_UPLOADER_QUEUE_SIZE

    def add_io_to_upload_queue(self, file_meta: FileMetadata, read_file: Callable[[], BinaryIO]) -> None:
        """
        Add file to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.

        Args:
            file_meta: File metadata-object
            file_name: Path to file to be uploaded.
                If none, the file object will still be created, but no data is uploaded
        """
        with self.lock:
            self.upload_queue.append((file_meta, read_file))
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
                self._post_upload([el[0] for el in self.upload_queue])
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
    def _upload_single(self, index: int, read_file: Callable[[], BinaryIO], file_meta: FileMetadata) -> None:
        # Upload file
        with read_file() as file:
            file_meta = self.cdf_client.files.upload_bytes(
                file,
                file_meta.name if file_meta.name is not None else "",
                overwrite=self.overwrite_existing,
                external_id=file_meta.external_id,
                source=file_meta.source,
                mime_type=file_meta.mime_type,
                metadata=file_meta.metadata,
                directory=file_meta.directory,
                asset_ids=file_meta.asset_ids,
                data_set_id=file_meta.data_set_id,
                labels=file_meta.labels,
                geo_location=file_meta.geo_location,
                source_created_time=file_meta.source_created_time,
                source_modified_time=file_meta.source_modified_time,
                security_categories=file_meta.security_categories,
            )

        # Update meta-object in queue
        self.upload_queue[index] = (file_meta, read_file)

    def _upload_batch(self) -> None:
        # Concurrently execute file-uploads

        futures: List[Future] = []
        with ThreadPoolExecutor(self.parallelism) as pool:
            for i, (file_meta, file_name) in enumerate(self.upload_queue):
                futures.append(pool.submit(self._upload_single, i, file_name, file_meta))
        for fut in futures:
            fut.result(0.0)

    def __enter__(self) -> "IOFileUploadQueue":
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


class FileUploadQueue(IOFileUploadQueue):
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
        post_upload_function: Optional[Callable[[List[FileMetadata]], None]] = None,
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
            overwrite_existing,
            cancellation_token,
        )

    def add_to_upload_queue(self, file_meta: FileMetadata, file_name: Union[str, PathLike]) -> None:
        """
        Add file to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.

        Args:
            file_meta: File metadata-object
            file_name: Path to file to be uploaded.
                If none, the file object will still be created, but no data is uploaded
        """

        def load_file_from_path() -> BinaryIO:
            return open(file_name, "rb")

        self.add_io_to_upload_queue(file_meta, load_file_from_path)


class BytesUploadQueue(IOFileUploadQueue):
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
        post_upload_function: Optional[Callable[[List[FileMetadata]], None]] = None,
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
            overwrite_existing,
            cancellation_token,
        )

    def add_to_upload_queue(self, content: bytes, metadata: FileMetadata) -> None:
        """
        Add object to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.
        Args:
            content: bytes object to upload
            metadata: metadata for the given bytes object
        """

        def get_byte_io() -> BinaryIO:
            return BytesIO(content)

        self.add_io_to_upload_queue(metadata, get_byte_io)
