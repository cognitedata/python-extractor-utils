from typing import Iterable, List, Optional, Union

from cognite.client.data_classes import Event as _Event
from cognite.client.data_classes import Row as _Row

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    # Backport for python < 3.10
    from typing_extensions import TypeAlias


from cognite.extractorutils.uploader.time_series import DataPoint


class InsertDatapoints:
    def __init__(self, *, id: Optional[int] = None, external_id: Optional[str] = None, datapoints: List[DataPoint]):
        self.id = id
        self.external_id = external_id
        self.datapoints = datapoints


class RawRow:
    def __init__(self, db_name: str, table_name: str, row: Union[_Row, Iterable[_Row]]):
        self.db_name = db_name
        self.table_name = table_name
        if isinstance(row, Iterable):
            self.rows = row
        else:
            self.rows = [row]


Event: TypeAlias = _Event

CdfTypes = Union[Event, Iterable[Event], RawRow, Iterable[RawRow], InsertDatapoints, Iterable[InsertDatapoints]]
