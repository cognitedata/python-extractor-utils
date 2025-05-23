from collections.abc import Iterable
from typing import TypeAlias

from cognite.client.data_classes import Event as _Event
from cognite.client.data_classes import Row as _Row
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import CogniteExtractorTimeSeriesApply
from cognite.extractorutils.uploader.time_series import DataPoint


class InsertDatapoints:
    def __init__(self, *, id: int | None = None, external_id: str | None = None, datapoints: list[DataPoint]):
        self.id = id
        self.external_id = external_id
        self.datapoints = datapoints


class InsertCDMDatapoints:
    def __init__(self, *, timeseries_apply: CogniteExtractorTimeSeriesApply, datapoints: list[DataPoint]):
        self.timeseries_apply = timeseries_apply
        self.datapoints = datapoints


class RawRow:
    def __init__(self, db_name: str, table_name: str, row: _Row | Iterable[_Row]):
        self.db_name = db_name
        self.table_name = table_name
        if isinstance(row, Iterable):
            self.rows = row
        else:
            self.rows = [row]


Event: TypeAlias = _Event

CdfTypes = Event | Iterable[Event] | RawRow | Iterable[RawRow] | InsertDatapoints | Iterable[InsertDatapoints]
