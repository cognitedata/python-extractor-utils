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
"""

import json
from abc import ABC, abstractmethod
from threading import Lock
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from requests.exceptions import ConnectionError
from retry import retry

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.uploader import DataPointList

RETRY_BACKOFF_FACTOR = 1.5
RETRY_MAX_DELAY = 15
RETRY_DELAY = 5
RETRIES = 10


class AbstractStateStore(ABC):
    """
    Base class for a state store.
    """

    def __init__(self):
        self._initialized = False
        self._local_state: Dict[str, Dict[str, Any]] = {}

        self._deleted: List[str] = []

        self.lock = Lock()

    @abstractmethod
    def initialize(self, force: bool = False) -> None:
        """
        Get states from remote store
        """
        pass

    @abstractmethod
    def synchronize(self) -> None:
        """
        Upload states to remote store
        """
        pass

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
                l = []
                for e in external_id:
                    state = self._local_state.get(e, {})
                    l.append((state.get("low"), state.get("high")))

                return l

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

        def callback(uploaded_points: List[Dict[str, Union[str, DataPointList]]]):
            for time_series in uploaded_points:
                # Use CDF timestamps
                data_points = time_series["datapoints"]
                if data_points:
                    high = max(data_points)[0]
                    low = min(data_points)[0]
                    external_id = time_series["externalId"]
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


class RawStateStore(AbstractStateStore):
    """
    An extractor state store based on CDF RAW.

    Args:
        cdf_client: Cognite client to use
        database: Name of CDF database
        table: Name of CDF table
    """

    def __init__(self, cdf_client: CogniteClient, database: str, table: str):
        super().__init__()

        self._cdf_client = cdf_client
        self.database = database
        self.table = table

        self._ensure_table()

    @retry(
        exceptions=(CogniteAPIError, ConnectionError),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _ensure_table(self) -> None:
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

    def initialize(self, force: bool = False) -> None:
        self._initialize_implementation(force)

    @retry(
        exceptions=(CogniteAPIError, ConnectionError),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _initialize_implementation(self, force: bool = False) -> None:
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
                self._local_state[row.key] = row.columns

        self._initialized = True

    def synchronize(self) -> None:
        self._synchronize_implementation()

    @retry(
        exceptions=(CogniteAPIError, ConnectionError),
        tries=RETRIES,
        delay=RETRY_DELAY,
        max_delay=RETRY_MAX_DELAY,
        backoff=RETRY_BACKOFF_FACTOR,
    )
    def _synchronize_implementation(self) -> None:
        """
        Upload local state store to CDF
        """
        self._cdf_client.raw.rows.insert(db_name=self.database, table_name=self.table, row=self._local_state)
        # Create a copy of deleted to facilitate testing (mock library stores list, and as it changes, the assertions
        # fail)
        self._cdf_client.raw.rows.delete(db_name=self.database, table_name=self.table, key=[k for k in self._deleted])
        with self.lock:
            self._deleted.clear()


class LocalStateStore(AbstractStateStore):
    """
    An extractor state store using a local JSON file as backend.

    Args:
        file_path: File path to JSON file to use
    """

    def __init__(self, file_path: str):
        super().__init__()

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
                    self._local_state = json.load(f)
            except FileNotFoundError:
                pass

        self._initialized = True

    def synchronize(self) -> None:
        """
        Save states to specified JSON file
        """
        with open(self._file_path, "w") as f:
            json.dump(self._local_state, f)

        with self.lock:
            self._deleted.clear()


class NoStateStore(AbstractStateStore):
    """
    A state store that only keeps states in memory and never stores or initializes from external sources.
    """

    def initialize(self, force: bool = False) -> None:
        pass

    def synchronize(self) -> None:
        pass
