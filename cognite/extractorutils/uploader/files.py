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
from typing import Any, BinaryIO, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union
from urllib.parse import ParseResult, urlparse

from httpx import URL, Client, Headers, Request, StreamConsumed, SyncByteStream
from requests.utils import super_len

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata, FileMetadataUpdate
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import CogniteExtractorFileApply
from cognite.client.utils._identifier import IdentifierSequence
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

_CDF_ALPHA_VERSION_HEADER = {"cdf-version": "alpha"}

FileMetadataOrCogniteExtractorFile = Union[FileMetadata, CogniteExtractorFileApply]


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


class IOByteStream(SyncByteStream):
    CHUNK_SIZE = 65_536

    def __init__(self, stream: BinaryIO) -> None:
        self._stream = stream
        self._is_stream_consumed = False

    def __iter__(self) -> Iterator[bytes]:
        if self._is_stream_consumed:
            raise StreamConsumed()
        chunk = self._stream.read(self.CHUNK_SIZE)
        while chunk:
            yield chunk
            chunk = self._stream.read(self.CHUNK_SIZE)


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
        post_upload_function: Optional[Callable[[List[FileMetadataOrCogniteExtractorFile]], None]] = None,
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

        self._httpx_client = Client(follow_redirects=True, timeout=cdf_client.config.file_transfer_timeout)

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

    def _apply_cognite_file(self, file_apply: CogniteExtractorFileApply) -> NodeId:
        instance_result = self.cdf_client.data_modeling.instances.apply(file_apply)
        node = instance_result.nodes[0]
        return node.as_id()

    def _upload_empty(
        self, file_meta: FileMetadataOrCogniteExtractorFile
    ) -> tuple[FileMetadataOrCogniteExtractorFile, str]:
        if isinstance(file_meta, CogniteExtractorFileApply):
            node_id = self._apply_cognite_file(file_meta)
            file_meta_response, url = self._create_cdm(instance_id=node_id)
        else:
            file_meta_response, url = self.cdf_client.files.create(
                file_metadata=file_meta, overwrite=self.overwrite_existing
            )

            # The files API for whatever reason doesn't update directory or source when you overwrite,
            # so we need to update those later.
            any_unchaged = (
                file_meta_response.directory != file_meta.directory or file_meta_response.source != file_meta.source
            )
            if any_unchaged:
                update = FileMetadataUpdate(external_id=file_meta.external_id)
                any = False
                if file_meta.source:
                    any = True
                    update.source.set(file_meta.source)
                if file_meta.directory:
                    any = True
                    update.directory.set(file_meta.directory)
                if any:
                    self.cdf_client.files.update(update)

        return file_meta_response, url

    def _upload_bytes(self, size: int, file: BinaryIO, file_meta: FileMetadataOrCogniteExtractorFile) -> None:
        file_meta, url = self._upload_empty(file_meta)
        resp = self._httpx_client.send(self._get_file_upload_request(url, file, size, file_meta.mime_type))
        resp.raise_for_status()

    def _upload_multipart(self, size: int, file: BinaryIO, file_meta: FileMetadataOrCogniteExtractorFile) -> None:
        chunks = ChunkedStream(file, self.max_file_chunk_size, size)
        self.logger.debug(
            f"File {file_meta.external_id} is larger than 5GiB ({size})" f", uploading in {chunks.chunk_count} chunks"
        )

        returned_file_metadata = self._create_multi_part(file_meta, chunks)
        upload_urls = returned_file_metadata["uploadUrls"]
        upload_id = returned_file_metadata["uploadId"]
        file_meta = FileMetadata.load(returned_file_metadata)

        for url in upload_urls:
            chunks.next_chunk()
            resp = self._httpx_client.send(self._get_file_upload_request(url, chunks, len(chunks), file_meta.mime_type))
            resp.raise_for_status()

        completed_headers = (
            _CDF_ALPHA_VERSION_HEADER if isinstance(file_meta, CogniteExtractorFileApply) is not None else None
        )

        res = self.cdf_client.files._post(
            url_path="/files/completemultipartupload",
            json={"id": file_meta.id, "uploadId": upload_id},
            headers=completed_headers,
        )
        res.raise_for_status()

    def _create_multi_part(self, file_meta: FileMetadataOrCogniteExtractorFile, chunks: ChunkedStream) -> dict:
        if isinstance(file_meta, CogniteExtractorFileApply):
            node_id = self._apply_cognite_file(file_meta)
            identifiers = IdentifierSequence.load(instance_ids=node_id).as_singleton()
            res = self.cdf_client.files._post(
                url_path="/files/multiuploadlink",
                json={"items": identifiers.as_dicts()},
                params={"parts": chunks.chunk_count},
                headers=_CDF_ALPHA_VERSION_HEADER,
            )
            res.raise_for_status()
            return res.json()["items"][0]
        else:
            res = self.cdf_client.files._post(
                url_path="/files/initmultipartupload",
                json=file_meta.dump(camel_case=True),
                params={"overwrite": self.overwrite_existing, "parts": chunks.chunk_count},
            )
            res.raise_for_status()
            return res.json()

    def add_io_to_upload_queue(
        self,
        file_meta: FileMetadataOrCogniteExtractorFile,
        read_file: Callable[[], BinaryIO],
        extra_retries: Optional[
            Union[Tuple[Type[Exception], ...], Dict[Type[Exception], Callable[[Any], bool]]]
        ] = None,
    ) -> None:
        """
        Add file to upload queue. The file will start uploading immedeately. If the size of the queue is larger than
        the specified max size, this call will block until it's completed the upload.

        Args:
            file_meta: File metadata-object
            file_name: Path to file to be uploaded.
                If none, the file object will still be created, but no data is uploaded
            extra_retries: Exception types that might be raised by ``read_file`` that should be retried
        """
        retries = cognite_exceptions()
        if isinstance(extra_retries, tuple):
            retries.update({exc: lambda _: True for exc in extra_retries or []})
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
        def upload_file(read_file: Callable[[], BinaryIO], file_meta: FileMetadataOrCogniteExtractorFile) -> None:
            with read_file() as file:
                size = super_len(file)
                if size == 0:
                    # upload just the file metadata witout data
                    file_meta, _ = self._upload_empty(file_meta)
                elif size >= self.max_single_chunk_file_size:
                    # The minimum chunk size is 4000MiB.
                    self._upload_multipart(size, file, file_meta)

                else:
                    self._upload_bytes(size, file, file_meta)

                if isinstance(file_meta, CogniteExtractorFileApply):
                    file_meta.is_uploaded = True

            if self.post_upload_function:
                try:
                    self.post_upload_function([file_meta])
                except Exception as e:
                    self.logger.error("Error in upload callback: %s", str(e))

        def wrapped_upload(read_file: Callable[[], BinaryIO], file_meta: FileMetadataOrCogniteExtractorFile) -> None:
            try:
                upload_file(read_file, file_meta)

            except Exception as e:
                self.logger.exception(f"Unexpected error while uploading file: {file_meta.external_id}")
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
            self.upload_queue.append(self._pool.submit(wrapped_upload, read_file, file_meta))
            self.upload_queue_size += 1
            self.files_queued.inc()
            self.queue_size.set(self.upload_queue_size)

    def _get_file_upload_request(
        self, url_str: str, stream: BinaryIO, size: int, mime_type: Optional[str] = None
    ) -> Request:
        url = URL(url_str)
        base_url = URL(self.cdf_client.config.base_url)

        if url.host == base_url.host:
            upload_url = url
        else:
            parsed_url: ParseResult = urlparse(url_str)
            parsed_base_url: ParseResult = urlparse(self.cdf_client.config.base_url)
            replaced_upload_url = parsed_url._replace(netloc=parsed_base_url.netloc).geturl()
            upload_url = URL(replaced_upload_url)

        headers = Headers(self._httpx_client.headers)
        headers.update(
            {
                "Accept": "*/*",
                "Content-Length": str(size),
                "Host": upload_url.netloc.decode("ascii"),
                "x-cdp-app": self.cdf_client._config.client_name,
            }
        )

        if mime_type is not None:
            headers.update({"Content-Type": mime_type})

        return Request(
            method="PUT",
            url=upload_url,
            stream=IOByteStream(stream),
            headers=headers,
        )

    def _create_cdm(self, instance_id: NodeId) -> tuple[FileMetadata, str]:
        identifiers = IdentifierSequence.load(instance_ids=instance_id).as_singleton()
        res = self.cdf_client.files._post(
            url_path="/files/uploadlink",
            json={"items": identifiers.as_dicts()},
            headers=_CDF_ALPHA_VERSION_HEADER,
        )
        res.raise_for_status()
        resp_json = res.json()["items"][0]
        return FileMetadata.load(resp_json), resp_json["uploadUrl"]

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
        post_upload_function: Optional[Callable[[List[FileMetadataOrCogniteExtractorFile]], None]] = None,
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

    def add_to_upload_queue(
        self, file_meta: FileMetadataOrCogniteExtractorFile, file_name: Union[str, PathLike]
    ) -> None:
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
        post_upload_function: Optional[Callable[[List[FileMetadataOrCogniteExtractorFile]], None]] = None,
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

    def add_to_upload_queue(self, content: bytes, file_meta: FileMetadataOrCogniteExtractorFile) -> None:
        """
        Add object to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.
        Args:
            content: bytes object to upload
            metadata: metadata for the given bytes object
        """

        def get_byte_io() -> BinaryIO:
            return BytesIO(content)

        self.add_io_to_upload_queue(file_meta, get_byte_io)
