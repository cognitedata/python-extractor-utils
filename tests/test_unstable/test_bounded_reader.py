from pathlib import Path

import pytest
from requests.utils import super_len

from cognite.extractorutils.unstable.core._bounded_reader import BoundedReader


def _make_file(tmp_path: Path, content: bytes) -> Path:
    p = tmp_path / "f.log"
    p.write_bytes(content)
    return p


def test_len_returns_snapshot_size_and_is_stable_after_reads(tmp_path: Path) -> None:
    path = _make_file(tmp_path, b"hello world")
    with open(path, "rb") as f:
        reader = BoundedReader(f, 5)
        assert len(reader) == 5
        reader.read(3)
        assert len(reader) == 5  # must not decrement with reads


@pytest.mark.parametrize(
    "snapshot,read_arg,expected",
    [
        (5, -1, b"hello"),  # read() with no arg reads up to snapshot
        (5, 3, b"hel"),  # read(n) where n < remaining
        (5, 10, b"hello"),  # read(n) where n > snapshot — capped at snapshot
    ],
    ids=["read_all", "read_partial", "read_exceeds_snapshot"],
)
def test_read_respects_snapshot_bound(tmp_path: Path, snapshot: int, read_arg: int, expected: bytes) -> None:
    path = _make_file(tmp_path, b"hello world")  # 11 bytes — always larger than snapshot
    with open(path, "rb") as f:
        assert BoundedReader(f, snapshot).read(read_arg) == expected


def test_read_returns_empty_after_exhaustion(tmp_path: Path) -> None:
    path = _make_file(tmp_path, b"hello world")
    with open(path, "rb") as f:
        reader = BoundedReader(f, 5)
        reader.read()
        assert reader.read() == b""
        assert reader.read(100) == b""


def test_zero_snapshot_returns_empty_immediately(tmp_path: Path) -> None:
    path = _make_file(tmp_path, b"data")
    with open(path, "rb") as f:
        reader = BoundedReader(f, 0)
        assert len(reader) == 0
        assert reader.read() == b""


def test_context_manager_closes_underlying_file(tmp_path: Path) -> None:
    path = _make_file(tmp_path, b"data")
    f = open(path, "rb")  # noqa: SIM115
    with BoundedReader(f, 4):
        pass
    assert f.closed


def test_super_len_reads_len_not_fstat(tmp_path: Path) -> None:
    # Verify that requests.utils.super_len() uses __len__ (snapshot) rather than
    # os.fstat().st_size (live inode size). The file is intentionally larger than
    # the declared snapshot to confirm fstat is not consulted.
    path = _make_file(tmp_path, b"hello world extended")  # 20 bytes on disk
    with open(path, "rb") as f:
        reader = BoundedReader(f, 5)
        assert super_len(reader) == 5


def test_midnight_rotation_race_file_shorter_than_snapshot(tmp_path: Path) -> None:
    # If TimedRotatingFileHandler rotates between snapshot and file open, the new
    # file.log is empty. BoundedReader returns b"" — the upload layer sees "too little
    # data" against the declared Content-Length. This is acceptable; the caller retries.
    path = _make_file(tmp_path, b"")
    with open(path, "rb") as f:
        reader = BoundedReader(f, 1_000)
        assert len(reader) == 1_000  # snapshot still declared
        assert reader.read() == b""  # but file is empty


def test_tell_tracks_bytes_consumed(tmp_path: Path) -> None:
    path = _make_file(tmp_path, b"hello world")
    with open(path, "rb") as f:
        reader = BoundedReader(f, 8)
        assert reader.tell() == 0
        reader.read(3)
        assert reader.tell() == 3
        reader.read(5)
        assert reader.tell() == 8


def test_stream_shorter_than_snapshot_partial(tmp_path: Path) -> None:
    # File has 5 bytes but snapshot declares 10 (e.g. file was truncated after snapshot).
    # _remaining decrements by len(data) not to_read, so tell() reflects actual bytes read.
    path = _make_file(tmp_path, b"hello")
    with open(path, "rb") as f:
        reader = BoundedReader(f, 10)
        assert len(reader) == 10  # snapshot still declared
        data = reader.read(10)
        assert data == b"hello"  # only 5 bytes available
        assert reader.tell() == 5  # tracks actual bytes, not requested
        assert reader.read() == b""


def test_read_multiple_calls_clamps_final_chunk(tmp_path: Path) -> None:
    path = _make_file(tmp_path, b"hello world")
    with open(path, "rb") as f:
        reader = BoundedReader(f, 7)
        assert reader.read(3) == b"hel"
        assert reader.read(3) == b"lo "
        assert reader.read(3) == b"w"  # only 1 byte left — clamped by min(size, remaining)
        assert reader.read(3) == b""


def test_read_all_after_partial_consumption(tmp_path: Path) -> None:
    path = _make_file(tmp_path, b"hello")
    with open(path, "rb") as f:
        reader = BoundedReader(f, 5)
        assert reader.read(3) == b"hel"
        assert reader.read(-1) == b"lo"  # read() after partial — returns only remaining 2 bytes


def test_close_and_closed_property(tmp_path: Path) -> None:
    path = _make_file(tmp_path, b"data")
    f = open(path, "rb")  # noqa: SIM115
    reader = BoundedReader(f, 4)
    assert not reader.closed
    reader.close()
    assert reader.closed
    assert f.closed
