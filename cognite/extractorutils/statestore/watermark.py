# ruff: noqa: ANN401
# TODO: the state stores should be generic over the type of state, not just Any.

#  Copyright 2020 Cognite AS
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
"""
State store implementation that uses watermarks to track changes.

Watermarks are either low and high values, or just high values, that represent the known range of data that has been
processed for a given external ID. This allows for incremental processing of data, where only new or changed data
is processed in subsequent runs.

For example, if a time series has a low watermark of 100 and a high watermark of 200, the extractor can start processing
new data from 201 onwards when starting up, and can begin backfilling historical data from 100 and backwards.

Or if a file has a high watermark of 1000, and the extractor receives a new file with a high watermark of 1500, the
extractor will know that this the file has indeed changed.

This module provides the following state store implementations:
- `RawStateStore`: A state store that uses a CDF RAW table to store states.
- `LocalStateStore`: A state store that uses a local JSON file to store states.
- `NoStateStore`: A state store that does not persist states between runs, but keeps the state in memory only.
"""

import json
from abc import ABC
from collections.abc import Callable, Iterator
from types import TracebackType
from typing import Any

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils._inner_util import _DecimalDecoder, _DecimalEncoder
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.uploader import DataPointList
from cognite.extractorutils.util import cognite_exceptions, retry

from ._base import RETRIES, RETRY_BACKOFF_FACTOR, RETRY_DELAY, RETRY_MAX_DELAY, _BaseStateStore


class AbstractStateStore(_BaseStateStore, ABC):
    """
    Base class for a state store.

    This class is thread-safe.

    Args:
        save_interval: Automatically trigger synchronize each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log synchronize triggers to.
        thread_name: Thread name of synchronize thread.
        cancellation_token: Token to cancel event from elsewhere. Cancelled when stop is called.
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

        self._local_state: dict[str, dict[str, Any]] = {}
        self._deleted: list[str] = []

    def get_state(self, external_id: str | list[str]) -> tuple[Any, Any] | list[tuple[Any, Any]]:
        """
        Get state(s) for external ID(s).

        Args:
            external_id: An external ID or list of external IDs to get states for

        Returns:
            A tuple with (low, high) watermarks, or a list of tuples
        """
        with self.lock:
            if isinstance(external_id, list):
                states = []
                for e in external_id:
                    state = self._local_state.get(e, {})
                    states.append((state.get("low"), state.get("high")))

                return states

            else:
                state = self._local_state.get(external_id, {})
                return state.get("low"), state.get("high")

    def set_state(self, external_id: str, low: Any | None = None, high: Any | None = None) -> None:
        """
        Set/update state of a singe external ID.

        Consider using `expand_state` instead, since this method will overwrite the current state no matter if it is
        actually outside the current state.

        Args:
            external_id: External ID of e.g. time series to store state of
            low: Low watermark
            high: High watermark
        """
        with self.lock:
            state = self._local_state.setdefault(external_id, {})
            state["low"] = low if low is not None else state.get("low")
            state["high"] = high if high is not None else state.get("high")

    def expand_state(self, external_id: str, low: Any | None = None, high: Any | None = None) -> None:
        """
        Only set/update state if the proposed state is outside the stored state.

        Only updates the low watermark if the proposed low is lower than the stored low, and only updates the high
        watermark if the proposed high is higher than the stored high.

        Args:
            external_id: External ID of e.g. time series to store state of
            low: Low watermark
            high: High watermark
        """
        with self.lock:
            state = self._local_state.setdefault(external_id, {})
            state["low"] = min(state.get("low", low), low) if low is not None else state.get("low")
            state["high"] = max(state.get("high", high), high) if high is not None else state.get("high")

    def delete_state(self, external_id: str) -> None:
        """
        Delete an external ID from the state store.

        Args:
            external_id: External ID to remove
        """
        with self.lock:
            self._local_state.pop(external_id, None)
            self._deleted.append(external_id)

    def post_upload_handler(self) -> Callable[[list[dict[str, str | DataPointList]]], None]:
        """
        Get a callback function to handle post-upload events.

        This callable is suitable for passing to a time series upload queue as ``post_upload_function``, that will
        automatically update the states in this state store when that upload queue is uploading.

        Returns:
            A function that expands the current states with the values given
        """

        def callback(uploaded_points: list[dict[str, str | DataPointList]]) -> None:
            for time_series in uploaded_points:
                # Use CDF timestamps
                data_points = time_series["datapoints"]
                if data_points:
                    high = max(data_points)[0]
                    low = min(data_points)[0]
                    external_id: str = time_series["externalId"]  # type: ignore  # known to be str from where we set it
                    self.expand_state(external_id, low, high)

        return callback

    def outside_state(self, external_id: str, new_state: Any) -> bool:
        """
        Check if a new proposed state is outside state interval (ie, if a new datapoint should be processed).

        Returns true if new_state is outside of stored state or if external_id is previously unseen.

        Args:
            external_id: External ID to test
            new_state: Proposed new state to test

        Returns:
            True if new_state is higher than the stored high watermark or lower than the low watermark.
        """
        if external_id not in self._local_state:
            return True

        low, high = self.get_state(external_id)

        if high is not None and new_state > high:
            return True
        return bool(low is not None and new_state < low)

    def __getitem__(self, external_id: str) -> tuple[Any, Any]:
        """
        Get state for a single external ID.
        """
        return self.get_state(external_id)  # type: ignore  # will not be list if input is single str

    def __setitem__(self, key: str, value: tuple[Any, Any]) -> None:
        """
        Set state for a single external ID.

        This will always overwrite the current state, so use with care.
        """
        self.set_state(external_id=key, low=value[0], high=value[1])

    def __contains__(self, external_id: str) -> bool:
        """
        Check if an external ID is in the state store.
        """
        return external_id in self._local_state

    def __len__(self) -> int:
        """
        Get the number of external IDs in the state store.
        """
        return len(self._local_state)

    def __iter__(self) -> Iterator[str]:
        """
        Iterate over external IDs in the state store.
        """
        yield from self._local_state


class RawStateStore(AbstractStateStore):
    """
    An extractor state store based on CDF RAW.

    This class is thread-safe.

    Args:
        cdf_client: Cognite client to use
        database: Name of CDF database
        table: Name of CDF table
        save_interval: Automatically trigger synchronize each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log synchronize triggers to.
        thread_name: Thread name of synchronize thread.
        cancellation_token: Token to cancel event from elsewhere. Cancelled when stop is called.
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
        super().__init__(save_interval, trigger_log_level, thread_name, cancellation_token)

        self._cdf_client = cdf_client
        self.database = database
        self.table = table

        self._ensure_table()

    def _ensure_table(self) -> None:
        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def impl() -> None:
            try:
                self._cdf_client.raw.databases.create(self.database)
            except CogniteAPIError as e:
                if not e.code == 400:
                    raise e
            try:
                self._cdf_client.raw.tables.create(self.database, self.table)
            except CogniteAPIError as e:
                if not e.code == 400:
                    raise e

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

            rows = self._cdf_client.raw.rows.list(db_name=self.database, table_name=self.table, limit=None)

            with self.lock:
                self._local_state.clear()
                for row in rows:
                    if row.key is None or row.columns is None:
                        self.logger.warning(f"None encountered in row: {row!s}")
                        # should never happen, but type from sdk is optional
                        continue
                    self._local_state[row.key] = row.columns

            self._initialized = True

        impl()

    def synchronize(self) -> None:
        """
        Upload the contents of the state store to CDF RAW.
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
            Upload local state store to CDF.
            """
            with self.lock:
                self._cdf_client.raw.rows.insert(db_name=self.database, table_name=self.table, row=self._local_state)
                # Create a copy of deleted to facilitate testing (mock library stores list, and as it changes, the
                # assertions fail)
                self._cdf_client.raw.rows.delete(db_name=self.database, table_name=self.table, key=list(self._deleted))
                self._deleted.clear()

        impl()

    def __enter__(self) -> "RawStateStore":
        """
        Wraps around start method, for use as context manager.

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """
        Wraps around stop method, for use as context manager.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()


class LocalStateStore(AbstractStateStore):
    """
    An extractor state store using a local JSON file as backend.

    Args:
        file_path: File path to JSON file to use
        save_interval: Automatically trigger synchronize each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log synchronize triggers to.
        thread_name: Thread name of synchronize thread.
        cancellation_token: Token to cancel event from elsewhere. Cancelled when stop is called.
    """

    def __init__(
        self,
        file_path: str,
        save_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        cancellation_token: CancellationToken | None = None,
    ) -> None:
        super().__init__(save_interval, trigger_log_level, thread_name, cancellation_token)

        self._file_path = file_path

    def initialize(self, force: bool = False) -> None:
        """
        Load states from specified JSON file.

        Args:
            force: Enable re-initialization, ie overwrite when called multiple times
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
        with self.lock:
            with open(self._file_path, "w") as f:
                json.dump(self._local_state, f, cls=_DecimalEncoder)
            self._deleted.clear()

    def __enter__(self) -> "LocalStateStore":
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


class NoStateStore(AbstractStateStore):
    """
    A state store that only keeps states in memory and never stores or initializes from external sources.

    This class is thread-safe.
    """

    def __init__(self) -> None:
        super().__init__()

    def initialize(self, force: bool = False) -> None:
        """
        Does nothing.
        """
        pass

    def synchronize(self) -> None:
        """
        Does nothing.
        """
        pass
