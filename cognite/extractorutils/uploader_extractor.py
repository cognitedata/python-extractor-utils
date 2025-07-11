"""
DEPRECATED. Use the normal base class and instantiate the upload queues manually.

A module containing a version of the Extractor class with pre-defined upload queues.
"""

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

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, TypeVar

from more_itertools import peekable

from cognite.client import CogniteClient
from cognite.extractorutils.base import Extractor
from cognite.extractorutils.configtools import BaseConfig, TimeIntervalConfig
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.statestore import AbstractStateStore
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.uploader import (
    CDMTimeSeriesUploadQueue,
    EventUploadQueue,
    RawUploadQueue,
    TimeSeriesUploadQueue,
)
from cognite.extractorutils.uploader_types import CdfTypes, Event, InsertCDMDatapoints, InsertDatapoints, RawRow


@dataclass
class QueueConfigClass:
    """
    Configuration for several upload queues.
    """

    event_size: int = 10_000
    raw_size: int = 50_000
    timeseries_size: int = 1_000_000
    upload_interval: TimeIntervalConfig = field(default_factory=lambda: TimeIntervalConfig("1m"))


@dataclass
class UploaderExtractorConfig(BaseConfig):
    """
    Base configuration for the UploaderExtractor.
    """

    queues: QueueConfigClass | None


UploaderExtractorConfigClass = TypeVar("UploaderExtractorConfigClass", bound=UploaderExtractorConfig)
RunHandle = Callable[[CogniteClient, AbstractStateStore, UploaderExtractorConfigClass, CancellationToken], None]


class UploaderExtractor(Extractor[UploaderExtractorConfigClass]):
    """
    Base class for simple extractors producing an output that can be described by the CdfTypes type alias.

    Feed output to the handle_output method and it will sort them into appropriate upload queues.

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
        cancellation_token: An event that will be set when the extractor should shut down, an empty one will be created
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
        version: str | None = None,
        run_handle: RunHandle | None = None,
        config_class: type[UploaderExtractorConfigClass],
        metrics: BaseMetrics | None = None,
        use_default_state_store: bool = True,
        cancellation_token: CancellationToken | None = None,
        config_file_path: str | None = None,
        continuous_extractor: bool = False,
        heartbeat_waiting_time: int = 600,
        handle_interrupts: bool = True,
        middleware: list[Callable[[dict], dict]] | None = None,
    ) -> None:
        super().__init__(
            name=name,
            description=description,
            version=version,
            run_handle=run_handle,
            config_class=config_class,
            metrics=metrics,
            use_default_state_store=use_default_state_store,
            cancellation_token=cancellation_token,
            config_file_path=config_file_path,
            continuous_extractor=continuous_extractor,
            heartbeat_waiting_time=heartbeat_waiting_time,
            handle_interrupts=handle_interrupts,
        )
        self.middleware = middleware if isinstance(middleware, list) else []

    def handle_output(self, output: CdfTypes) -> None:
        """
        Handle the output of the extractor and sort it into appropriate upload queues.

        Args:
            output: The output from the extractor, which can be an Event, RawRow, InsertDatapoints, or an iterable of
                these types.
        """
        list_output = [output] if not isinstance(output, Iterable) else output
        peekable_output = peekable(list_output)

        peek = peekable_output.peek(None)

        if peek is None:
            return

        if isinstance(peek, Event):
            for event in peekable_output:
                event = self._apply_middleware(event)
                if isinstance(event, Event):
                    self.event_queue.add_to_upload_queue(event)

        elif isinstance(peek, RawRow):
            for raw_row in peekable_output:
                if isinstance(raw_row, RawRow):
                    for row in raw_row.rows:
                        row = self._apply_middleware(row)
                        self.raw_queue.add_to_upload_queue(
                            database=raw_row.db_name, table=raw_row.table_name, raw_row=row
                        )
        elif isinstance(peek, InsertDatapoints):
            for dp in peekable_output:
                if isinstance(dp, InsertDatapoints):
                    self.time_series_queue.add_to_upload_queue(
                        id=dp.id, external_id=dp.external_id, datapoints=dp.datapoints
                    )
        elif isinstance(peek, InsertCDMDatapoints):
            for dp in peekable_output:
                if isinstance(dp, InsertCDMDatapoints):
                    self.cdm_time_series_queue.add_to_upload_queue(instance_id=dp.instance_id, datapoints=dp.datapoints)
        else:
            raise ValueError(f"Unexpected type: {type(peek)}")

    def _apply_middleware(self, item: Any) -> Any:  # noqa: ANN401
        for mw in self.middleware:
            item = mw(item)
        return item

    def __enter__(self) -> "UploaderExtractor":
        """
        Initializes the upload queues and returns the extractor instance.
        """
        super().__enter__()

        queue_config = self.config.queues if self.config.queues else QueueConfigClass()
        self.event_queue = EventUploadQueue(
            self.cognite_client,
            max_queue_size=queue_config.event_size,
            max_upload_interval=queue_config.upload_interval.seconds,
            trigger_log_level="INFO",
        ).__enter__()
        self.raw_queue = RawUploadQueue(
            self.cognite_client,
            max_queue_size=queue_config.raw_size,
            max_upload_interval=queue_config.upload_interval.seconds,
            trigger_log_level="INFO",
        ).__enter__()
        self.time_series_queue = TimeSeriesUploadQueue(
            self.cognite_client,
            max_queue_size=queue_config.timeseries_size,
            max_upload_interval=queue_config.upload_interval.seconds,
            trigger_log_level="INFO",
            create_missing=True,
        ).__enter__()
        self.cdm_time_series_queue = CDMTimeSeriesUploadQueue(
            self.cognite_client,
            max_queue_size=queue_config.timeseries_size,
            max_upload_interval=queue_config.upload_interval.seconds,
            trigger_log_level="INFO",
        ).__enter__()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool:
        """
        Waits for the upload queues and exits the extractor context.
        """
        self.event_queue.__exit__(exc_type, exc_val, exc_tb)
        self.raw_queue.__exit__(exc_type, exc_val, exc_tb)
        self.time_series_queue.__exit__(exc_type, exc_val, exc_tb)
        self.cdm_time_series_queue.__exit__(exc_type, exc_val, exc_tb)
        return super().__exit__(exc_type, exc_val, exc_tb)
