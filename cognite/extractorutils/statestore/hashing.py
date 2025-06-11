"""
State store implementations that use hashing to track changes.

This module provides two main classes for state management:
- ``RawHashStateStore``: A state store that uses CDF RAW to store and persist states based on a hash of the data.
- ``LocalHashStateStore``: A state store that uses a local JSON file to store and persist states based on a hash of the
  data.
"""

import hashlib
import json
from abc import ABC
from collections.abc import Iterable, Iterator
from types import TracebackType
from typing import Any

import orjson

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils._inner_util import _DecimalDecoder, _DecimalEncoder
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.util import cognite_exceptions, retry

from ._base import RETRIES, RETRY_BACKOFF_FACTOR, RETRY_DELAY, RETRY_MAX_DELAY, _BaseStateStore


class AbstractHashStateStore(_BaseStateStore, ABC):
    """
    Base class for state stores that use hashing to track changes.

    This class is thread-safe.
    """

    def __init__(
        self,
        save_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        cancellation_token: CancellationToken | None = None,
    ) -> None:
        super().__init__(
            save_interval=save_interval,
            trigger_log_level=trigger_log_level,
            thread_name=thread_name,
            cancellation_token=cancellation_token,
        )

        self._local_state: dict[str, dict[str, str]] = {}
        self._seen: set[str] = set()

    def get_state(self, external_id: str) -> str | None:
        """
        Get the state for a given external ID as a hash digest.

        Args:
            external_id: The external ID for which to retrieve the state.

        Returns:
            The hash digest of the state if it exists, otherwise None.
        """
        with self.lock:
            return self._local_state.get(external_id, {}).get("digest")

    def _hash_row(self, data: dict[str, Any]) -> str:
        return hashlib.sha256(orjson.dumps(data, option=orjson.OPT_SORT_KEYS)).hexdigest()

    def set_state(self, external_id: str, data: dict[str, Any]) -> None:
        """
        Set the state for a given external ID based on a hash of the provided data.

        Args:
            external_id: The external ID for which to set the state.
            data: The data to hash and store as the state.
        """
        with self.lock:
            self._local_state[external_id] = {"digest": self._hash_row(data)}

    def has_changed(self, external_id: str, data: dict[str, Any]) -> bool:
        """
        Check if the provided data is different from the stored state for the given external ID.

        This is done by comparing the hash of the provided data with the stored hash.

        Args:
            external_id: The external ID for which to check the state.
            data: The data to hash and compare against the stored state.

        Returns:
            True if the data has changed (i.e., the hash is different or not present), otherwise False.
        """
        with self.lock:
            if external_id not in self._local_state:
                return True

            return self._hash_row(data) != self._local_state[external_id]["digest"]

    def __getitem__(self, external_id: str) -> str | None:
        """
        Get the state for a given external ID as a hash digest.

        Args:
            external_id: The external ID for which to retrieve the state.

        Returns:
            The hash digest of the state if it exists, otherwise None.
        """
        return self.get_state(external_id)

    def __setitem__(self, key: str, value: dict[str, Any]) -> None:
        """
        Set the state for a given external ID based on a hash of the provided data.

        Args:
            key: The external ID for which to set the state.
            value: The data to hash and store as the state.
        """
        self.set_state(external_id=key, data=value)

    def __contains__(self, external_id: str) -> bool:
        """
        Check if the given external ID exists in the state store.
        """
        return external_id in self._local_state

    def __len__(self) -> int:
        """
        Get the number of external IDs stored in the state store.
        """
        return len(self._local_state)

    def __iter__(self) -> Iterator[str]:
        """
        Iterate over the external IDs stored in the state store.
        """
        with self.lock:
            yield from self._local_state


class RawHashStateStore(AbstractHashStateStore):
    """
    A version of AbstractHashStateStore that uses CDF RAW to store and persist states.

    All states are stored in a CDF RAW table, where each row is identified by an external ID.

    This class is thread-safe.

    Args:
        cdf_client: The CogniteClient instance to use for ingesting to/reading from RAW.
        database: The name of the CDF RAW database.
        table: The name of the CDF RAW table.
        save_interval: If set, the state store will periodically synchronize with CDF RAW.
        trigger_log_level: The logging level to use for synchronization triggers.
        thread_name: Name of the thread used for synchronization.
        cancellation_token: A CancellationToken to control the lifecycle of the state store.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        database: str,
        table: str,
        save_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        cancellation_token: CancellationToken | None = None,
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
        """
        Upload local state store to CDF.
        """

        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def impl() -> None:
            with self.lock:
                self._cdf_client.raw.rows.insert(
                    db_name=self.database,
                    table_name=self.table,
                    row=self._local_state,
                    ensure_parent=True,
                )

        impl()

    def initialize(self, force: bool = False) -> None:
        """
        Initialize the state store by loading all known states from CDF RAW.

        Unless ``force`` is set to True, this will not re-initialize the state store if it has already been initialized.
        Subsequent calls to this method will be noop unless ``force`` is set to True.

        Args:
            force: Enable re-initialization, ie overwrite when called multiple times
        """

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
                        self.logger.warning(f"None encountered in row: {row!s}")
                        # should never happen, but type from sdk is optional
                        continue
                    state = row.columns.get("digest")
                    if state:
                        self._local_state[row.key] = {"digest": state}

            self._initialized = True

        impl()

    def __enter__(self) -> "RawHashStateStore":
        """
        Wraps around start method, for use as context manager.

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Wraps around stop method, for use as context manager.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()


class LocalHashStateStore(AbstractHashStateStore):
    """
    A version of AbstractHashStateStore that uses a local JSON file to store and persist states.

    All states are stored in a JSON file, where each key is an external ID and the value is a dictionary containing
    the hash digest of the data.

    This class is thread-safe.

    Args:
        file_path: The path to the JSON file where states will be stored.
        save_interval: If set, the state store will periodically synchronize with the JSON file.
        trigger_log_level: The logging level to use for synchronization triggers.
        thread_name: Name of the thread used for synchronization.
        cancellation_token: A CancellationToken to control the lifecycle of the state store.
    """

    def __init__(
        self,
        file_path: str,
        save_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        cancellation_token: CancellationToken | None = None,
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
        Load states from specified JSON file.

        Unless ``force`` is set to True, this will not re-initialize the state store if it has already been initialized.
        Subsequent calls to this method will be noop unless ``force`` is set to True.

        Args:
            force: Enable re-initialization, i.e. overwrite when called multiple times
        """
        if self._initialized and not force:
            return

        with self.lock:
            try:
                with open(self._file_path) as f:
                    self._local_state = json.load(f, cls=_DecimalDecoder)
            except FileNotFoundError:
                pass
            except json.decoder.JSONDecodeError as e:
                raise ValueError(f"Invalid JSON in state store file: {e!s}") from e

        self._initialized = True

    def synchronize(self) -> None:
        """
        Save states to specified JSON file.
        """
        with self.lock, open(self._file_path, "w") as f:
            json.dump(self._local_state, f, cls=_DecimalEncoder)

    def __enter__(self) -> "LocalHashStateStore":
        """
        Wraps around start method, for use as context manager.

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Wraps around stop method, for use as context manager.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()
