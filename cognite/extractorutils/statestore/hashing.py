import hashlib
import json
from abc import ABC
from types import TracebackType
from typing import Any, Dict, Iterable, Iterator, Optional, Set, Type

import orjson

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils._inner_util import _DecimalDecoder, _DecimalEncoder
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.util import cognite_exceptions, retry

from ._base import RETRIES, RETRY_BACKOFF_FACTOR, RETRY_DELAY, RETRY_MAX_DELAY, _BaseStateStore


class AbstractHashStateStore(_BaseStateStore, ABC):
    def __init__(
        self,
        save_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ) -> None:
        super().__init__(
            save_interval=save_interval,
            trigger_log_level=trigger_log_level,
            thread_name=thread_name,
            cancellation_token=cancellation_token,
        )

        self._local_state: Dict[str, Dict[str, str]] = {}
        self._seen: Set[str] = set()

    def get_state(self, external_id: str) -> Optional[str]:
        with self.lock:
            return self._local_state.get(external_id, {}).get("digest")

    def _hash_row(self, data: Dict[str, Any]) -> str:
        return hashlib.sha256(orjson.dumps(data, option=orjson.OPT_SORT_KEYS)).hexdigest()

    def set_state(self, external_id: str, data: Dict[str, Any]) -> None:
        with self.lock:
            self._local_state[external_id] = {"digest": self._hash_row(data)}

    def has_changed(self, external_id: str, data: Dict[str, Any]) -> bool:
        with self.lock:
            if external_id not in self._local_state:
                return True

            return self._hash_row(data) != self._local_state[external_id]["digest"]

    def __getitem__(self, external_id: str) -> Optional[str]:
        return self.get_state(external_id)

    def __setitem__(self, key: str, value: Dict[str, Any]) -> None:
        self.set_state(external_id=key, data=value)

    def __contains__(self, external_id: str) -> bool:
        return external_id in self._local_state

    def __len__(self) -> int:
        return len(self._local_state)

    def __iter__(self) -> Iterator[str]:
        with self.lock:
            for key in self._local_state:
                yield key


class RawHashStateStore(AbstractHashStateStore):
    def __init__(
        self,
        cdf_client: CogniteClient,
        database: str,
        table: str,
        save_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ) -> None:
        super().__init__(
            save_interval=save_interval,
            trigger_log_level=trigger_log_level,
            thread_name=thread_name,
            cancellation_token=cancellation_token,
        )
        self._cdf_client = cdf_client
        self.database = database
        self.table = table

    def synchronize(self) -> None:
        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def impl() -> None:
            """
            Upload local state store to CDF
            """
            with self.lock:
                self._cdf_client.raw.rows.insert(
                    db_name=self.database,
                    table_name=self.table,
                    row=self._local_state,
                    ensure_parent=True,
                )

        impl()

    def initialize(self, force: bool = False) -> None:
        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def impl() -> None:
            """
            Get all known states.

            Args:
                force: Enable re-initialization, ie overwrite when called multiple times
            """
            if self._initialized and not force:
                return

            rows: Iterable[Row]
            try:
                rows = self._cdf_client.raw.rows.list(db_name=self.database, table_name=self.table, limit=None)
            except CogniteAPIError as e:
                if e.code == 404:
                    rows = []
                else:
                    raise e

            with self.lock:
                self._local_state.clear()
                for row in rows:
                    if row.key is None or row.columns is None:
                        self.logger.warning(f"None encountered in row: {str(row)}")
                        # should never happen, but type from sdk is optional
                        continue
                    state = row.columns.get("digest")
                    if state:
                        self._local_state[row.key] = {"digest": state}

            self._initialized = True

        impl()

    def __enter__(self) -> "RawHashStateStore":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """
        Wraps around stop method, for use as context manager

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()


class LocalHashStateStore(AbstractHashStateStore):
    def __init__(
        self,
        file_path: str,
        save_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ) -> None:
        super().__init__(
            save_interval=save_interval,
            trigger_log_level=trigger_log_level,
            thread_name=thread_name,
            cancellation_token=cancellation_token,
        )

        self._file_path = file_path

    def initialize(self, force: bool = False) -> None:
        """
        Load states from specified JSON file

        Args:
            force: Enable re-initialization, ie overwrite when called multiple times
        """
        if self._initialized and not force:
            return

        with self.lock:
            try:
                with open(self._file_path, "r") as f:
                    self._local_state = json.load(f, cls=_DecimalDecoder)
            except FileNotFoundError:
                pass
            except json.decoder.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in state store file: {str(e)}") from e

        self._initialized = True

    def synchronize(self) -> None:
        """
        Save states to specified JSON file
        """
        with self.lock:
            with open(self._file_path, "w") as f:
                json.dump(self._local_state, f, cls=_DecimalEncoder)

    def __enter__(self) -> "LocalHashStateStore":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """
        Wraps around stop method, for use as context manager

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()
