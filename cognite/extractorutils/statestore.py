import json
from abc import ABC, abstractmethod
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple, Union

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError


class StateStore(ABC):
    """
    Base class for a state store.

    An extractor state store based is storing the progress of an extractor between runs, facilitating incremental load
    and speeding up startup times.
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
            external_id (str): External ID of e.g. time series to store state of
            low (Any): Low watermark
            high (Any): High watermark
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
            external_id (str): External ID of e.g. time series to store state of
            low (Any): Low watermark
            high (Any): High watermark
        """
        with self.lock:
            state = self._local_state.setdefault(external_id, {})
            state["low"] = min(state.get("low", low), low) if low is not None else state.get("low")
            state["high"] = max(state.get("high", high), high) if high is not None else state.get("high")

    def delete_state(self, external_id: str) -> None:
        """
        Delete an external ID from the state store.

        Args:
            external_id (str): External ID to remove
        """
        with self.lock:
            self._local_state.pop(external_id, None)
            self._deleted.append(external_id)


class RawStateStore(StateStore):
    """
    An extractor state store based on CDF RAW.

    Args:
        client (CogniteClient): Cognite client to use
        database (str): Name of CDF database
        table (str): Name of CDF table
    """

    def __init__(self, client: CogniteClient, database: str, table: str):
        super().__init__()

        self._client = client
        self.database = database
        self.table = table

        self._ensure_table()

    def _ensure_table(self):
        try:
            self._client.raw.databases.create(self.database)
        except CogniteAPIError as e:
            if not e.code == 400:
                raise e
        try:
            self._client.raw.tables.create(self.database, self.table)
        except CogniteAPIError as e:
            if not e.code == 400:
                raise e

    def initialize(self, force: bool = False) -> None:
        """
        Get all known states.

        Args:
            force (bool): Enable re-initialization, ie overwrite when called multiple times
        """
        if self._initialized and not force:
            return

        rows = self._client.raw.rows.list(db_name=self.database, table_name=self.table, limit=None)

        with self.lock:
            self._local_state.clear()
            for row in rows:
                self._local_state[row.key] = row.columns

        self._initialized = True

    def synchronize(self) -> None:
        """
        Upload local state store to CDF
        """
        self._client.raw.rows.insert(db_name=self.database, table_name=self.table, row=self._local_state)

        # Create a copy of deleted to facilitate testing (mock library stores list, and as it changes, the assertions
        # fail)
        self._client.raw.rows.delete(db_name=self.database, table_name=self.table, key=[k for k in self._deleted])

        with self.lock:
            self._deleted.clear()


class LocalStateStore(StateStore):
    """
    An extractor state store using a local JSON file as backend.

    Args:
        file_path (str): File path to JSON file to use
    """

    def __init__(self, file_path: str):
        super().__init__()

        self._file_path = file_path

    def initialize(self, force: bool = False) -> None:
        """
        Load states from specified JSON file

        Args:
            force (bool): Enable re-initialization, ie overwrite when called multiple times
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
