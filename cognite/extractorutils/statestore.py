from threading import Lock
from typing import Any, Dict, List, Optional

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError


class RawStateStore:
    """
    An extractor state store based on CDF RAW, storing the progress of an extractor.

    Args:
        client (CogniteClient): Cognite client to use
        database (str): Name of CDF database
        table (str): Name of CDF table
    """

    def __init__(self, client: CogniteClient, database: str, table: str):
        self._client = client
        self.database = database
        self.table = table

        self._initialized = False
        self._local_state: Dict[str, Dict[str, Any]] = {}

        self._deleted: List[str] = []

        self._ensure_table()

        self.lock = Lock()

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

    def get_states(self, use_cache: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        Get all known states.

        WARNING: Calling get_states with use_cache = False will OVERWRITE the local state, if you have written changes
        to the local states, these will be lost

        Args:
            use_cache (bool): Use locally cached states, if exist

        Returns:
            dict: A mapping of external ID -> state
        """
        if use_cache and self._initialized:
            return self._local_state

        rows = self._client.raw.rows.list(db_name=self.database, table_name=self.table, limit=None)

        with self.lock.acquire():
            self._local_state.clear()
            for row in rows:
                self._local_state[row.key] = row.columns

        self._initialized = True
        return self._local_state

    def set_state(self, external_id: str, low: Optional[Any] = None, high: Optional[Any] = None) -> None:
        """
        Set/update state of a singe external ID.

        Args:
            external_id (str): External ID of e.g. time series to store state of
            low (Any): Low watermark
            high (Any): High watermark
        """
        with self.lock.acquire():
            if external_id not in self._local_state:
                self._local_state[external_id] = {}
            self._local_state[external_id]["high"] = (
                high if high is not None else self._local_state[external_id].get("high")
            )
            self._local_state[external_id]["low"] = (
                low if low is not None else self._local_state[external_id].get("low")
            )

    def expand_state(self, external_id: str, low: Optional[Any] = None, high: Optional[Any] = None) -> None:
        """
        Like set_state, but only sets state if the proposed state is outside the stored state. That is if e.g. low is
        lower than the stored low.

        Args:
            external_id (str): External ID of e.g. time series to store state of
            low (Any): Low watermark
            high (Any): High watermark
        """
        if low is not None and external_id in self._local_state and "low" in self._local_state[external_id]:
            low = low if low < self._local_state[external_id]["low"] else None

        if high is not None and external_id in self._local_state and "high" in self._local_state[external_id]:
            high = high if high > self._local_state[external_id]["high"] else None

        self.set_state(external_id, low, high)

    def delete_state(self, external_id: str) -> None:
        """
        Delete an external ID from the state store.

        Args:
            external_id (str): External ID to remove
        """
        with self.lock.acquire():
            self._local_state.pop(external_id, None)
            self._deleted.append(external_id)

    def synchronize(self) -> None:
        """
        Upload local state store to CDF
        """
        self._client.raw.rows.insert(db_name=self.database, table_name=self.table, row=self._local_state)
        self._client.raw.rows.delete(db_name=self.database, table_name=self.table, key=self._deleted)

        with self.lock.acquire():
            self._deleted.clear()
