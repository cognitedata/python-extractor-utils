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
from io import BytesIO, RawIOBase
from math import ceil
from os import PathLike
from types import TracebackType
from typing import Any, BinaryIO, Callable, Dict, List, Optional, Tuple, Type, Union

from requests.utils import super_len

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.extractorutils.threading import CancellationToken
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
from cognite.extractorutils.util import cognite_exceptions, retry

_QUEUES: int = 0
_QUEUES_LOCK: threading.RLock = threading.RLock()

# 5 GiB
_MAX_SINGLE_CHUNK_FILE_SIZE = 5 * 1024 * 1024 * 1024
# 4000 MiB
_MAX_FILE_CHUNK_SIZE = 4 * 1024 * 1024 * 1000


class ChunkedStream(RawIOBase, BinaryIO):
    """
    Wrapper around a read-only stream to allow treating it as a sequence of smaller streams.

    `next_chunk` will return `true` if there is one more chunk, it must be called
    before this is treated as a stream the first time, typically in a `while` loop.

    Args:
        inner: Stream to wrap.
        max_chunk_size: Maximum size per stream chunk.
        stream_length: Total (remaining) length of the inner stream. This must be accurate.
    """

    def __init__(self, inner: BinaryIO, max_chunk_size: int, stream_length: int) -> None:
        self._inner = inner
        self._pos = -1
        self._max_chunk_size = max_chunk_size
        self._stream_length = stream_length
        self._chunk_index = -1
        self._current_chunk_size = -1

    def tell(self) -> int:
        return self._pos

    # RawIOBase is (stupidly) incompatible with BinaryIO
    # Implementing a correct type that inherits from both is impossible,
    # but python does so anyway, (ab)using the property that if bytes() and if None
    # resolve the same way. These four useless methods with liberal use of Any are
    # required to satisfy mypy.
    # This may be solvable by changing the typing in the python SDK to use typing.Protocol.
    def writelines(self, __lines: Any) -> None:
        raise NotImplementedError()

    def write(self, __b: Any) -> int:
        raise NotImplementedError()

    def __enter__(self) -> "ChunkedStream":
        return super().__enter__()

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        return super().__exit__(exc_type, exc_val, exc_tb)

    @property
    def chunk_count(self) -> int:
        return ceil(self._stream_length / self._max_chunk_size)

    @property
    def len(self) -> int:
        return len(self)

    @property
    def current_chunk(self) -> int:
        """
        Current chunk number.
        """
        return self._chunk_index

    def __len__(self) -> int:
        return self._current_chunk_size

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = self._current_chunk_size - self._pos

        size = min(size, self._current_chunk_size - self._pos)
        if size > 0:
            self._pos += size
            return self._inner.read(size)
        return bytes()

    def next_chunk(self) -> bool:
        """
        Step into the next chunk, letting this be read as a stream again.

        Returns `False` if the stream is exhausted.
        """
        if self._chunk_index >= self.chunk_count - 1:
            return False

        self._chunk_index += 1
        inner_pos = self._inner.tell()
        self._current_chunk_size = min(self._max_chunk_size, self._stream_length - inner_pos)
        self._pos = 0

        return True


class IOFileUploadQueue(AbstractUploadQueue):
    """
    Upload queue for files using BinaryIO

    Note that if the upload fails, the stream needs to be restarted, so
    the enqueued callback needs to produce a new IO object for each call.

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the events that were uploaded.
        max_queue_size: Maximum size of upload queue.
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
        max_parallelism: Maximum number of parallel uploads. If nothing is given, the parallelism will be capped by the
            max_workers of the cognite client.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[FileMetadata]], None]] = None,
        max_queue_size: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        overwrite_existing: bool = False,
        cancellation_token: Optional[CancellationToken] = None,
        max_parallelism: Optional[int] = None,
    ):
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            None,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )

        if self.threshold <= 0:
            raise ValueError("Max queue size must be positive for file upload queues")

        self.upload_queue: List[Future] = []
        self.errors: List[Exception] = []

        self.overwrite_existing = overwrite_existing

        self.parallelism = self.cdf_client.config.max_workers
        if max_parallelism:
            self.parallelism = max_parallelism
        if self.parallelism <= 0:
            self.parallelism = 4

        self.files_queued = FILES_UPLOADER_QUEUED
        self.files_written = FILES_UPLOADER_WRITTEN
        self.queue_size = FILES_UPLOADER_QUEUE_SIZE

        self.max_single_chunk_file_size = _MAX_SINGLE_CHUNK_FILE_SIZE
        self.max_file_chunk_size = _MAX_FILE_CHUNK_SIZE

        self._update_queue_thread = threading.Thread(target=self._remove_done_from_queue, daemon=True)

        self._full_queue = threading.Condition()

        global _QUEUES, _QUEUES_LOCK
        with _QUEUES_LOCK:
            self._pool = ThreadPoolExecutor(
                max_workers=self.parallelism, thread_name_prefix=f"FileUploadQueue-{_QUEUES}"
            )
            _QUEUES += 1

    def _remove_done_from_queue(self) -> None:
        while not self.cancellation_token.is_cancelled:
            with self.lock:
                self.upload_queue = list(filter(lambda f: f.running(), self.upload_queue))

            self.cancellation_token.wait(5)

    def add_io_to_upload_queue(
        self,
        file_meta: FileMetadata,
        read_file: Callable[[], BinaryIO],
        extra_retries: Optional[
            Union[Tuple[Type[Exception], ...], Dict[Type[Exception], Callable[[Any], bool]]]
        ] = None,
    ) -> None:
        """
        Add file to upload queue. The file will start uploading immedeately. If the size of the queue is larger than
        the specified max size, this call will block until it's

        Args:
            file_meta: File metadata-object
            file_name: Path to file to be uploaded.
                If none, the file object will still be created, but no data is uploaded
            extra_retries: Exception types that might be raised by ``read_file`` that should be retried
        """
        retries = cognite_exceptions()
        if isinstance(extra_retries, tuple):
            retries.update({exc: lambda _e: True for exc in extra_retries or []})
        elif isinstance(extra_retries, dict):
            retries.update(extra_retries)

        @retry(
            exceptions=retries,
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def _upload_single(read_file: Callable[[], BinaryIO], file_meta: FileMetadata) -> None:
            try:
                # Upload file
                with read_file() as file:
                    size = super_len(file)
                    if size == 0:
                        # upload just the file metadata witout data
                        file_meta, _url = self.cdf_client.files.create(
                            file_metadata=file_meta, overwrite=self.overwrite_existing
                        )
                    elif size >= self.max_single_chunk_file_size:
                        # The minimum chunk size is 4000MiB.
                        chunks = ChunkedStream(file, self.max_file_chunk_size, size)
                        self.logger.debug(
                            f"File {file_meta.external_id} is larger than 5GiB ({size})"
                            f", uploading in {chunks.chunk_count} chunks"
                        )
                        with self.cdf_client.files.multipart_upload_session(
                            file_meta.name if file_meta.name is not None else "",
                            parts=chunks.chunk_count,
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
                        ) as session:
                            while chunks.next_chunk():
                                session.upload_part(chunks.current_chunk, chunks)
                            file_meta = session.file_metadata
                    else:
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

                if self.post_upload_function:
                    try:
                        self.post_upload_function([file_meta])
                    except Exception as e:
                        self.logger.error("Error in upload callback: %s", str(e))

            except Exception as e:
                self.logger.exception("Unexpected error while uploading file")
                self.errors.append(e)

            finally:
                with self.lock:
                    self.files_written.inc()
                    self.upload_queue_size -= 1
                    self.queue_size.set(self.upload_queue_size)
                with self._full_queue:
                    self._full_queue.notify()

        if self.upload_queue_size >= self.threshold:
            with self._full_queue:
                while not self._full_queue.wait(timeout=2) and not self.cancellation_token.is_cancelled:
                    pass

        with self.lock:
            self.upload_queue.append(self._pool.submit(_upload_single, read_file, file_meta))
            self.upload_queue_size += 1
            self.files_queued.inc()
            self.queue_size.set(self.upload_queue_size)

    def upload(self, fail_on_errors: bool = True, timeout: Optional[float] = None) -> None:
        """
        Wait for all uploads to finish
        """
        for future in self.upload_queue:
            future.result(timeout=timeout)
        with self.lock:
            self.queue_size.set(self.upload_queue_size)
        if fail_on_errors and self.errors:
            # There might be more errors, but we can only have one as the cause, so pick the first
            raise RuntimeError(f"{len(self.errors)} upload(s) finished with errors") from self.errors[0]

    def __enter__(self) -> "IOFileUploadQueue":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        self._pool.__enter__()
        self._update_queue_thread.start()
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
        self._pool.__exit__(exc_type, exc_val, exc_tb)
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
        max_queue_size: Maximum size of upload queue.
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
        cancellation_token: Optional[CancellationToken] = None,
    ):
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
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
        max_queue_size: Maximum size of upload queue.
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
        overwrite_existing: If 'overwrite' is set to true, fields for the files found for externalIds can be overwritten
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[FileMetadata]], None]] = None,
        max_queue_size: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        overwrite_existing: bool = False,
        cancellation_token: Optional[CancellationToken] = None,
    ) -> None:
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
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
