"""
DEPRECATED: This module is deprecated and will be removed in a future release.

These types are used in the UploaderExtractor, as well as the REST and MQTT extensions for the extractorutils library.
"""

from collections.abc import Iterable
from typing import TypeAlias

from cognite.client.data_classes import Event as _Event
from cognite.client.data_classes import Row as _Row
from cognite.client.data_classes.data_modeling import NodeId
from cognite.extractorutils.uploader.time_series import DataPoint


class InsertDatapoints:
    """
    A class representing a batch of datapoints to be inserted into a time series.
    """

    def __init__(self, *, id: int | None = None, external_id: str | None = None, datapoints: list[DataPoint]) -> None:  # noqa: A002
        self.id = id
        self.external_id = external_id
        self.datapoints = datapoints


class InsertCDMDatapoints:
    """
    A class representing a batch of datapoints to be inserted into a cdm time series.
    """

    def __init__(self, *, instance_id: NodeId, datapoints: list[DataPoint]) -> None:
        self.instance_id = instance_id
        self.datapoints = datapoints


class RawRow:
    """
    A class representing a row of data to be inserted into a RAW table.
    """

    def __init__(self, db_name: str, table_name: str, row: _Row | Iterable[_Row]) -> None:
        self.db_name = db_name
        self.table_name = table_name
        if isinstance(row, Iterable):
            self.rows = row
        else:
            self.rows = [row]


Event: TypeAlias = _Event

CdfTypes = Event | Iterable[Event] | RawRow | Iterable[RawRow] | InsertDatapoints | Iterable[InsertDatapoints]
