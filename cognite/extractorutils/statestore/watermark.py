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
The ``statestore`` module contains classes for keeping track of the extraction state of individual items, facilitating
incremental load and speeding up startup times.

At the beginning of a run the extractor typically calls the ``initialize`` method, which loads the states from the
remote store (which can either be a local JSON file or a table in CDF RAW), and during and/or at the end of a run, the
``synchronize`` method is called, which saves the current states to the remote store.

You can choose the back-end for your state store with which class you're instantiating:

.. code-block:: python

    # A state store using a JSON file as remote storage:
    states = LocalStateStore("state.json")
    states.initialize()

    # A state store using a RAW table as remote storage:
    states = RawStateStore(
        cdf_client = CogniteClient(),
        database = "extractor_states",
        table = "my_extractor_deployment"
    )
    states.initialize()

You can now use this state store to get states:

.. code-block:: python

    low, high = states.get_state(external_id = "my-id")

You can set states:

.. code-block:: python

    states.set_state(external_id = "another-id", high=100)

and similar for ``low``. The ``set_state(...)`` method will always overwrite the current state. Some times you might
want to only set state *if larger* than the previous state, in that case consider ``expand_state(...)``:

.. code-block:: python

    # High watermark of another-id is already 100, nothing happens in this call:
    states.expand_state(external_id = "another-id", high=50)

    # This will set high to 150 as it is larger than the previous state
    states.expand_state(external_id = "another-id", high=150)

To store the state to the remote store, use the ``synchronize()`` method:

.. code-block:: python

    states.synchronize()

You can set a state store to automatically update on upload triggers from an upload queue by using the
``post_upload_function`` in the upload queue:

.. code-block:: python

    states = LocalStateStore("state.json")
    states.initialize()

    uploader = TimeSeriesUploadQueue(
        cdf_client = CogniteClient(),
        max_upload_interval = 10
        post_upload_function = states.post_upload_handler()
    )

    # The state store is now updated automatically!

    states.synchronize()

"""

import json
from abc import ABC
from types import TracebackType
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple, Type, Union

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

    Args:
        save_interval: Automatically trigger synchronize each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log synchronize triggers to.
        thread_name: Thread name of synchronize thread.
        cancellation_token: Token to cancel event from elsewhere. Cancelled when stop is called.
    """

    def __init__(
        self,
        save_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ):
        super().__init__(
            save_interval=save_interval,
            trigger_log_level=trigger_log_level,
            thread_name=thread_name,
            cancellation_token=cancellation_token,
        )

        self._local_state: Dict[str, Dict[str, Any]] = {}
        self._deleted: List[str] = []

    def get_state(self, external_id: Union[str, List[str]]) -> Union[Tuple[Any, Any], List[Tuple[Any, Any]]]:
        """
        Get state(s) for external ID(s)

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

    def set_state(self, external_id: str, low: Optional[Any] = None, high: Optional[Any] = None) -> None:
        """
        Set/update state of a singe external ID.

        Args:
            external_id: External ID of e.g. time series to store state of
            low: Low watermark
            high: High watermark
        """
        with self.lock:
            state = self._local_state.setdefault(external_id, {})
            state["low"] = low if low is not None else state.get("low")
            state["high"] = high if high is not None else state.get("high")

    def expand_state(self, external_id: str, low: Optional[Any] = None, high: Optional[Any] = None) -> None:
        """
        Like set_state, but only sets state if the proposed state is outside the stored state. That is if e.g. low is
        lower than the stored low.

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

    def post_upload_handler(self) -> Callable[[List[Dict[str, Union[str, DataPointList]]]], None]:
        """
        Get a callable suitable for passing to a time series upload queue as post_upload_function, that will
        automatically update the states in this state store when that upload queue is uploading.

        Returns:
            A function that expands the current states with the values given
        """

        def callback(uploaded_points: List[Dict[str, Union[str, DataPointList]]]) -> None:
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
        if low is not None and new_state < low:
            return True

        return False

    def __getitem__(self, external_id: str) -> Tuple[Any, Any]:
        return self.get_state(external_id)  # type: ignore  # will not be list if input is single str

    def __setitem__(self, key: str, value: Tuple[Any, Any]) -> None:
        self.set_state(external_id=key, low=value[0], high=value[1])

    def __contains__(self, external_id: str) -> bool:
        return external_id in self._local_state

    def __len__(self) -> int:
        return len(self._local_state)

    def __iter__(self) -> Iterator[str]:
        for key in self._local_state:
            yield key


class RawStateStore(AbstractStateStore):
    """
    An extractor state store based on CDF RAW.

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
        save_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ):
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
                        self.logger.warning(f"None encountered in row: {str(row)}")
                        # should never happen, but type from sdk is optional
                        continue
                    self._local_state[row.key] = row.columns

            self._initialized = True

        impl()

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
                self._cdf_client.raw.rows.insert(db_name=self.database, table_name=self.table, row=self._local_state)
                # Create a copy of deleted to facilitate testing (mock library stores list, and as it changes, the
                # assertions fail)
                self._cdf_client.raw.rows.delete(
                    db_name=self.database, table_name=self.table, key=[k for k in self._deleted]
                )
                self._deleted.clear()

        impl()

    def __enter__(self) -> "RawStateStore":
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
        save_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ):
        super().__init__(save_interval, trigger_log_level, thread_name, cancellation_token)

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
            self._deleted.clear()

    def __enter__(self) -> "LocalStateStore":
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


class NoStateStore(AbstractStateStore):
    """
    A state store that only keeps states in memory and never stores or initializes from external sources.
    """

    def __init__(self) -> None:
        super().__init__()

    def initialize(self, force: bool = False) -> None:
        pass

    def synchronize(self) -> None:
        pass
