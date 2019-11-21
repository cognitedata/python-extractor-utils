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
        self.client = client
        self.database = database
        self.table = table

        self.initialized = False
        self.local_state: Dict[str, Dict[str, Any]] = {}

        self.deleted: List[str] = []

        self._ensure_table()

    def _ensure_table(self):
        try:
            self.client.raw.databases.create(self.database)
        except CogniteAPIError as e:
            if not e.code == 400:
                raise e
        try:
            self.client.raw.tables.create(self.database, self.table)
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
        if use_cache and self.initialized:
            return self.local_state

        rows = self.client.raw.rows.list(db_name=self.database, table_name=self.table, limit=None)

        self.local_state.clear()
        for row in rows:
            self.local_state[row.key] = row.columns

        self.initialized = True
        return self.local_state

    def set_state(self, external_id: str, low: Optional[Any] = None, high: Optional[Any] = None) -> None:
        """
        Set/update state of a singe external ID.

        Args:
            external_id (str): External ID of e.g. time series to store state of
            low (Any): Low watermark
            high (Any): High watermark
        """
        if external_id not in self.local_state:
            self.local_state[external_id] = {}
        self.local_state[external_id]["high"] = high if high is not None else self.local_state[external_id].get("high")
        self.local_state[external_id]["low"] = low if low is not None else self.local_state[external_id].get("low")

    def delete_state(self, external_id: str) -> None:
        """
        Delete an external ID from the state store.

        Args:
            external_id (str): External ID to remove
        """
        self.local_state.pop(external_id, None)
        self.deleted.append(external_id)

    def synchronize(self) -> None:
        """
        Upload local state store to CDF
        """
        self.client.raw.rows.insert(db_name=self.database, table_name=self.table, row=self.local_state)
        self.client.raw.rows.delete(db_name=self.database, table_name=self.table, key=self.deleted)

        self.deleted.clear()
