"""Bounded binary reader for point-in-time log file uploads."""

import io
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

        with open(log_path, "rb") as f:
            size = f.seek(0, 2); f.seek(0)
            cdf_client.files.upload_bytes(content=BoundedReader(f, size), ...)
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

    def seek(self, offset: int, whence: int = 0) -> int:
        if whence == 2:
            raise io.UnsupportedOperation("BoundedReader does not support seek from end (whence=2)")
        pos = self._stream.seek(offset, whence)
        self._remaining = max(0, self._size - pos)
        return pos

    def seekable(self) -> bool:
        return self._stream.seekable()

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
