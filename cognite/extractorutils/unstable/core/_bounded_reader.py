"""Bounded binary reader for point-in-time log file uploads."""

from typing import BinaryIO


class BoundedReader:
    """
    Wraps a binary file handle and limits reads to a byte count captured at snapshot time.

    Implements ``__len__`` so ``requests.utils.super_len()`` bypasses ``os.fstat()``
    and declares the correct HTTP ``Content-Length``. This is critical when uploading
    the active log file (``file.log``) while the logger is still appending to it:
    without the cap, the upload reads past the declared length and httpx raises
    ``LocalProtocolError: "Too much data for declared Content-Length"``.

    Usage::

        snapshot_size = os.path.getsize(log_path)
        with BoundedReader(open(log_path, "rb"), snapshot_size) as reader:
            upload_queue.add_io_to_upload_queue(file_meta, lambda: reader, ...)
    """

    def __init__(self, stream: BinaryIO, max_bytes: int) -> None:
        self._stream = stream
        self._size = max_bytes
        self._remaining = max_bytes

    def __len__(self) -> int:
        # super_len() checks __len__ before os.fstat — returning the fixed snapshot
        # size here ensures Content-Length is declared at the moment of the snapshot,
        # not at the moment the upload resolves the inode size (which may have grown).
        return self._size

    def tell(self) -> int:
        return self._size - self._remaining

    @property
    def closed(self) -> bool:
        return self._stream.closed

    def close(self) -> None:
        self._stream.close()

    def read(self, size: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        to_read = self._remaining if size < 0 else min(size, self._remaining)
        data = self._stream.read(to_read)
        self._remaining -= len(data)
        return data

    def __enter__(self) -> "BoundedReader":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
