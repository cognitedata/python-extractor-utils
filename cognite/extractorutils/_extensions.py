#  Copyright 2022 Cognite AS
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
A module containing utilities for developing extensions for generic source systems.
"""

from datetime import datetime
from types import TracebackType
from typing import Callable, Iterable, List, Optional, Tuple, Type, Union

from more_itertools import peekable

from cognite.client import CogniteClient
from cognite.client.data_classes import Event as _Event
from cognite.client.data_classes import Row as _Row
from cognite.extractorutils.base import Extractor
from cognite.extractorutils.configtools import CustomConfigClass
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.uploader import EventUploadQueue, RawUploadQueue, TimeSeriesUploadQueue

try:
    from typing import TypeAlias  # type: ignore
except ImportError:
    # Backport for python < 3.10
    from typing_extensions import TypeAlias


class RawRow:
    def __init__(self, db_name: str, table_name: str, row: Union[_Row, Iterable[_Row]]):
        self.db_name = db_name
        self.table_name = table_name
        if isinstance(row, Iterable):
            self.rows = row
        else:
            self.rows = [row]


TimeStamp = Union[int, datetime]


class InsertDatapoints:
    def __init__(
        self,
        *,
        id: Optional[int] = None,
        external_id: Optional[str] = None,
        datapoints: Union[List[Tuple[TimeStamp, float]], List[Tuple[TimeStamp, str]]],
    ):
        self.id = id
        self.external_id = external_id
        self.datapoints = datapoints


Event: TypeAlias = _Event

CdfTypes = Union[Event, Iterable[Event], RawRow, Iterable[RawRow], InsertDatapoints, Iterable[InsertDatapoints]]


class BaseExtensionExtractor(Extractor[CustomConfigClass]):
    """
    Base class for extension extractors.

    Intended for use with an extractor that is capable of producing CdfTypes from some underlying source.

    Args:
        name: Name of the extractor, how it's invoked from the command line.
        description: A short 1-2 sentence description of the extractor.
        version: Version number, following semantic versioning.
        run_handle: A function to call when setup is done that runs the extractor, taking a cognite client, state store
            config object and a shutdown event as arguments.
        config_class: A class (based on the BaseConfig class) that defines the configuration schema for the extractor
        metrics: Metrics collection, a default one with be created if omitted.
        use_default_state_store: Create a simple instance of the LocalStateStore to provide to the run handle. If false
            a NoStateStore will be created in its place.
        cancelation_token: An event that will be set when the extractor should shut down, an empty one will be created
            if omitted.
        config_file_path: If supplied, the extractor will not use command line arguments to get a config file, but
            rather use the supplied path.
        continuous_extractor: If True, extractor will both successful start and end time. Else, only show run on exit.
        heartbeat_waiting_time: Time interval between each heartbeat to the extraction pipeline in seconds.
    """

    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        run_handle: Optional[Callable[[CogniteClient, AbstractStateStore, CustomConfigClass, Event], None]] = None,
        config_class: Type[CustomConfigClass],
        metrics: Optional[BaseMetrics] = None,
        use_default_state_store: bool = True,
        cancelation_token: Event = Event(),
        config_file_path: Optional[str] = None,
        continuous_extractor: bool = False,
        heartbeat_waiting_time: int = 600,
        handle_interrupts: bool = True,
    ):
        super(BaseExtensionExtractor, self).__init__(
            name=name,
            description=description,
            version=version,
            run_handle=run_handle,
            config_class=config_class,
            metrics=metrics,
            use_default_state_store=use_default_state_store,
            cancelation_token=cancelation_token,
            config_file_path=config_file_path,
            continuous_extractor=continuous_extractor,
            heartbeat_waiting_time=heartbeat_waiting_time,
            handle_interrupts=handle_interrupts,
        )

        self._event_queue_size = 10_000
        self._raw_queue_size = 100_000
        self._timeseries_queue_size = 1_000_000
        self._upload_interval = 60

    def handle_output(self, output: CdfTypes) -> None:
        if not isinstance(output, Iterable):
            output = [output]

        peekable_output = peekable(output)
        peek = peekable_output.peek(None)

        if peek is None:
            return

        if isinstance(peek, Event):
            for event in peekable_output:
                self.event_queue.add_to_upload_queue(event)
        elif isinstance(peek, RawRow):
            for raw_row in peekable_output:
                for row in raw_row.rows:
                    self.raw_queue.add_to_upload_queue(database=raw_row.db_name, table=raw_row.table_name, raw_row=row)
        elif isinstance(peek, InsertDatapoints):
            for datapoints in peekable_output:
                self.time_series_queue.add_to_upload_queue(
                    id=datapoints.id, external_id=datapoints.external_id, datapoints=datapoints.datapoints
                )
        else:
            raise ValueError(f"Unexpected type: {type(peek)}")

    def __enter__(self) -> "BaseExtensionExtractor":
        super(BaseExtensionExtractor, self).__enter__()
        self.event_queue = EventUploadQueue(
            self.cognite_client,
            max_queue_size=self._event_queue_size,
            max_upload_interval=self._upload_interval,
            trigger_log_level="INFO",
        ).__enter__()
        self.raw_queue = RawUploadQueue(
            self.cognite_client,
            max_queue_size=self._raw_queue_size,
            max_upload_interval=self._upload_interval,
            trigger_log_level="INFO",
        ).__enter__()
        self.time_series_queue = TimeSeriesUploadQueue(
            self.cognite_client,
            max_queue_size=self._timeseries_queue_size,
            max_upload_interval=self._upload_interval,
            trigger_log_level="INFO",
            create_missing=True,
        ).__enter__()

        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> bool:
        self.event_queue.__exit__(exc_type, exc_val, exc_tb)
        self.raw_queue.__exit__(exc_type, exc_val, exc_tb)
        self.time_series_queue.__exit__(exc_type, exc_val, exc_tb)
        return super(BaseExtensionExtractor, self).__exit__(exc_type, exc_val, exc_tb)
