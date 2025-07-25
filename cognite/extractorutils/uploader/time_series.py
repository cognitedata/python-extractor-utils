"""
Upload queue for time series and sequences.
"""
#  Copyright 2023 Cognite AS
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

import math
from collections.abc import Callable
from datetime import datetime
from types import TracebackType
from typing import Any, Generic, Literal, TypedDict, TypeVar

from cognite.client import CogniteClient
from cognite.client.data_classes import (
    Sequence,
    SequenceData,
    SequenceRows,
    StatusCode,
    TimeSeries,
)
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import CogniteExtractorTimeSeriesApply
from cognite.client.data_classes.data_modeling.instances import DirectRelationReference
from cognite.client.exceptions import CogniteDuplicatedError, CogniteNotFoundError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.uploader._base import (
    RETRIES,
    RETRY_BACKOFF_FACTOR,
    RETRY_DELAY,
    RETRY_MAX_DELAY,
    AbstractUploadQueue,
)
from cognite.extractorutils.uploader._metrics import (
    SEQUENCES_UPLOADER_POINTS_QUEUED,
    SEQUENCES_UPLOADER_POINTS_WRITTEN,
    SEQUENCES_UPLOADER_QUEUE_SIZE,
    TIMESERIES_UPLOADER_POINTS_DISCARDED,
    TIMESERIES_UPLOADER_POINTS_QUEUED,
    TIMESERIES_UPLOADER_POINTS_WRITTEN,
    TIMESERIES_UPLOADER_QUEUE_SIZE,
)
from cognite.extractorutils.util import EitherId, cognite_exceptions, retry

MIN_DATAPOINT_TIMESTAMP = -2208988800000
MAX_DATAPOINT_STRING_LENGTH = 255
MAX_DATAPOINT_VALUE = 1e100
MIN_DATAPOINT_VALUE = -1e100

TimeStamp = int | datetime

DataPointWithoutStatus = tuple[TimeStamp, float] | tuple[TimeStamp, str] | tuple[TimeStamp, int]
FullStatusCode = StatusCode | int
DataPointWithStatus = tuple[TimeStamp, float, FullStatusCode] | tuple[TimeStamp, str, FullStatusCode]
DataPoint = DataPointWithoutStatus | DataPointWithStatus
DataPointList = list[DataPoint]

TQueue = TypeVar("TQueue", bound="BaseTimeSeriesUploadQueue")
IdType = TypeVar("IdType", EitherId, NodeId)


class CdmDatapointsPayload(TypedDict):
    """
    Represents a payload for CDF datapoints, linking them to a specific instance.
    """

    instanceId: NodeId
    datapoints: DataPointList


def default_time_series_factory(external_id: str, datapoints: DataPointList) -> TimeSeries:
    """
    Default time series factory used when create_missing in a TimeSeriesUploadQueue is given as a boolean.

    Args:
        external_id: External ID of time series to create
        datapoints: The list of datapoints that were tried to be inserted

    Returns:
        A TimeSeries object with external_id set, and the is_string automatically detected
    """
    is_string = (
        isinstance(datapoints[0].get("value"), str)
        if isinstance(datapoints[0], dict)
        else isinstance(datapoints[0][1], str)
    )
    return TimeSeries(external_id=external_id, is_string=is_string)


class BaseTimeSeriesUploadQueue(AbstractUploadQueue, Generic[IdType]):
    """
    Abstract base upload queue for time series.

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of dicts containing the datapoints that were uploaded (on the same format as the kwargs in
            datapoints upload in the Cognite SDK).
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Callable[[list[dict[str, str | DataPointList]]], None] | None = None,
        max_queue_size: int | None = None,
        max_upload_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        cancellation_token: CancellationToken | None = None,
    ) -> None:
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )

        self.upload_queue: dict[IdType, DataPointList] = {}

        self.points_queued = TIMESERIES_UPLOADER_POINTS_QUEUED
        self.points_written = TIMESERIES_UPLOADER_POINTS_WRITTEN
        self.queue_size = TIMESERIES_UPLOADER_QUEUE_SIZE

    def _verify_datapoint_time(self, time: int | float | datetime | str) -> bool:
        if isinstance(time, int | float):
            return not math.isnan(time) and time >= MIN_DATAPOINT_TIMESTAMP
        elif isinstance(time, str):
            return False
        else:
            return time.timestamp() * 1000.0 >= MIN_DATAPOINT_TIMESTAMP

    def _verify_datapoint_value(self, value: int | float | datetime | str) -> bool:
        if isinstance(value, float):
            return not (
                math.isnan(value) or math.isinf(value) or value > MAX_DATAPOINT_VALUE or value < MIN_DATAPOINT_VALUE
            )
        elif isinstance(value, str):
            return len(value) <= MAX_DATAPOINT_STRING_LENGTH
        return not isinstance(value, datetime)

    def _is_datapoint_valid(
        self,
        dp: DataPoint,
    ) -> bool:
        if isinstance(dp, dict):
            return self._verify_datapoint_time(dp["timestamp"]) and self._verify_datapoint_value(dp["value"])
        elif isinstance(dp, tuple):
            return self._verify_datapoint_time(dp[0]) and self._verify_datapoint_value(dp[1])
        else:
            return True

    def _sanitize_datapoints(self, datapoints: DataPointList | None) -> DataPointList:
        datapoints = datapoints or []
        old_len = len(datapoints)
        datapoints = list(filter(self._is_datapoint_valid, datapoints))

        new_len = len(datapoints)

        if old_len > new_len:
            diff = old_len - new_len
            self.logger.warning(f"Discarding {diff} datapoints due to bad timestamp or value")
            TIMESERIES_UPLOADER_POINTS_DISCARDED.inc(diff)

        return datapoints

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """
        Wraps around stop method, for use as context manager.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()

    def __len__(self) -> int:
        """
        The size of the upload queue.

        Returns:
            Number of data points in queue
        """
        return self.upload_queue_size

    def __enter__(self: TQueue) -> TQueue:
        """
        Wraps around start method, for use as context manager.

        Returns:
            self
        """
        self.start()
        return self


class TimeSeriesUploadQueue(BaseTimeSeriesUploadQueue[EitherId]):
    """
    Upload queue for time series.

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of dicts containing the datapoints that were uploaded (on the same format as the kwargs in
            datapoints upload in the Cognite SDK).
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
        create_missing: Create missing time series if possible (ie, if external id is used). Either given as a boolean
            (True would auto-create a time series with nothing but an external ID), or as a factory function taking an
            external ID and a list of datapoints about to be inserted and returning a TimeSeries object.
        data_set_id: Data set id passed to create_missing. Does nothing if create_missing is False.
            If a custom timeseries creation method is set in create_missing, this is used as fallback if
            that method does not set data set id on its own.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Callable[[list[dict[str, str | DataPointList]]], None] | None = None,
        max_queue_size: int | None = None,
        max_upload_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        create_missing: Callable[[str, DataPointList], TimeSeries] | bool = False,
        data_set_id: int | None = None,
        cancellation_token: CancellationToken | None = None,
    ) -> None:
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )

        self.missing_factory: Callable[[str, DataPointList], TimeSeries]

        if isinstance(create_missing, bool):
            self.create_missing = create_missing
            self.missing_factory = default_time_series_factory
        else:
            self.create_missing = True
            self.missing_factory = create_missing

        self.data_set_id = data_set_id

    def add_to_upload_queue(
        self,
        *,
        id: int | None = None,  # noqa: A002
        external_id: str | None = None,
        datapoints: DataPointList | None = None,
    ) -> None:
        """
        Add data points to upload queue.

        The queue will be uploaded if the queue size is larger than the threshold specified in the ``__init__``.

        Args:
            id: Internal ID of time series. Either this or external_id must be set.
            external_id: External ID of time series. Either this or external_id must be set.
            datapoints: list of data points to add
        """
        datapoints = self._sanitize_datapoints(datapoints)

        either_id = EitherId(id=id, external_id=external_id)

        with self.lock:
            if either_id not in self.upload_queue:
                self.upload_queue[either_id] = []

            self.upload_queue[either_id].extend(datapoints)
            self.points_queued.inc(len(datapoints))
            self.upload_queue_size += len(datapoints)
            self.queue_size.set(self.upload_queue_size)

            self._check_triggers()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards.
        """

        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def _upload_batch(upload_this: list[dict], retries: int = 5) -> list[dict]:
            if len(upload_this) == 0:
                return upload_this

            try:
                self.cdf_client.time_series.data.insert_multiple(upload_this)

            except CogniteNotFoundError as ex:
                if not retries:
                    raise ex

                if not self.create_missing:
                    self.logger.error("Could not upload data points to %s: %s", str(ex.not_found), str(ex))

                # Get IDs of time series that exists, but failed because of the non-existing time series
                retry_these = [EitherId(**id_dict) for id_dict in ex.failed if id_dict not in ex.not_found]

                if self.create_missing:
                    # Get the time series that can be created
                    create_these_ids = {id_dict["externalId"] for id_dict in ex.not_found if "externalId" in id_dict}
                    datapoints_lists: dict[str, DataPointList] = {
                        ts_dict["externalId"]: ts_dict["datapoints"]
                        for ts_dict in upload_this
                        if ts_dict["externalId"] in create_these_ids
                    }

                    self.logger.info(f"Creating {len(create_these_ids)} time series")
                    to_create: list[TimeSeries] = [
                        self.missing_factory(external_id, datapoints_lists[external_id])
                        for external_id in create_these_ids
                    ]
                    if self.data_set_id is not None:
                        for ts in to_create:
                            if ts.data_set_id is None:
                                ts.data_set_id = self.data_set_id
                    self.cdf_client.time_series.create(to_create)

                    retry_these.extend([EitherId(external_id=i) for i in create_these_ids])

                    if len(ex.not_found) != len(create_these_ids):
                        missing = [id_dict for id_dict in ex.not_found if id_dict.get("externalId") not in retry_these]
                        missing_num = len(ex.not_found) - len(create_these_ids)
                        self.logger.error(
                            f"{missing_num} time series not found, and could not be created automatically: "
                            + str(missing)
                            + " Data will be dropped"
                        )

                # Remove entries with non-existing time series from upload queue
                upload_this = [
                    entry
                    for entry in upload_this
                    if EitherId(id=entry.get("id"), external_id=entry.get("externalId")) in retry_these
                ]

                # Upload remaining
                _upload_batch(upload_this, retries - 1)

            return upload_this

        if len(self.upload_queue) == 0:
            return

        with self.lock:
            upload_this = _upload_batch(
                [
                    {either_id.type(): either_id.content(), "datapoints": list(datapoints)}
                    for either_id, datapoints in self.upload_queue.items()
                    if len(datapoints) > 0
                ]
            )

            for datapoints in self.upload_queue.values():
                self.points_written.inc(len(datapoints))

            try:
                self._post_upload(upload_this)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))

            self.upload_queue.clear()
            self.logger.info(f"Uploaded {self.upload_queue_size} datapoints")
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)


class CDMTimeSeriesUploadQueue(BaseTimeSeriesUploadQueue[NodeId]):
    """
    Upload queue for CDM time series.

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of dicts containing the datapoints that were uploaded (on the same format as the kwargs in
            datapoints upload in the Cognite SDK).
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Callable[[list[dict[str, str | DataPointList]]], None] | None = None,
        max_queue_size: int | None = None,
        max_upload_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        create_missing: Callable[[NodeId, DataPointList], CogniteExtractorTimeSeriesApply] | bool = False,
        cancellation_token: CancellationToken | None = None,
        source: DirectRelationReference | None = None,
    ) -> None:
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )

        self.missing_factory: Callable[[NodeId, DataPointList], CogniteExtractorTimeSeriesApply]
        self.source = source

        if isinstance(create_missing, bool):
            self.create_missing = create_missing
            self.missing_factory = self.default_cdm_time_series_factory
        else:
            self.create_missing = True
            self.missing_factory = create_missing

    def default_cdm_time_series_factory(
        self, instance_id: NodeId, datapoints: DataPointList
    ) -> CogniteExtractorTimeSeriesApply:
        """
        Default CDM time series factory used when create_missing in a CDMTimeSeriesUploadQueue is given as a boolean.

        Args:
            instance_id: Instance ID of time series to create
            datapoints: The list of datapoints that were tried to be inserted
            source: The source of the time series, used for creating the DirectRelationReference
        Returns:
            A CogniteExtractorTimeSeriesApply object with instance_id set, and the is_string automatically detected
        """
        is_string = (
            isinstance(datapoints[0].get("value"), str)
            if isinstance(datapoints[0], dict)
            else isinstance(datapoints[0][1], str)
        )

        time_series_type: Literal["numeric", "string"] = "string" if is_string else "numeric"

        return CogniteExtractorTimeSeriesApply(
            space=instance_id.space,
            external_id=instance_id.external_id,
            is_step=False,
            time_series_type=time_series_type,
            source=self.source,
        )

    def add_to_upload_queue(
        self,
        *,
        instance_id: NodeId,
        datapoints: DataPointList | None = None,
    ) -> None:
        """
        Add data points to upload queue.

        The queue will be uploaded if the queue size is larger than the threshold specified in the __init__.

        Args:
            instance_id: The identifier for the time series to which the datapoints belong.
            datapoints: list of data points to add
        """
        datapoints = self._sanitize_datapoints(datapoints)

        with self.lock:
            if instance_id not in self.upload_queue:
                self.upload_queue[instance_id] = []

            self.upload_queue[instance_id].extend(datapoints)
            self.points_queued.inc(len(datapoints))
            self.upload_queue_size += len(datapoints)
            self.queue_size.set(self.upload_queue_size)

            self._check_triggers()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards.
        """

        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def _upload_batch(upload_this: list[CdmDatapointsPayload], retries: int = 5) -> list[CdmDatapointsPayload]:
            if len(upload_this) == 0:
                return upload_this

            try:
                self.cdf_client.time_series.data.insert_multiple(upload_this)  # type: ignore[arg-type]
            except CogniteNotFoundError as ex:
                if not retries:
                    raise ex

                if not self.create_missing:
                    self.logger.error("Could not upload data points to %s: %s", str(ex.not_found), str(ex))

                # Get IDs of time series that exists, but failed because of the non-existing time series
                retry_these = [
                    NodeId(id_dict["instanceId"]["space"], id_dict["instanceId"]["externalId"])
                    for id_dict in ex.failed
                    if id_dict not in ex.not_found
                ]

                if self.create_missing:
                    # Get the time series that can be created
                    create_these_ids = {
                        NodeId(id_dict["instanceId"]["space"], id_dict["instanceId"]["externalId"])
                        for id_dict in ex.not_found
                    }
                    self.logger.info(f"Creating {len(create_these_ids)} time series")

                    datapoints_lists: dict[NodeId, DataPointList] = {
                        ts_dict["instanceId"]: ts_dict["datapoints"]
                        for ts_dict in upload_this
                        if ts_dict["instanceId"] in create_these_ids
                    }

                    to_create: list[CogniteExtractorTimeSeriesApply] = [
                        self.missing_factory(instance_id, datapoints_lists[instance_id])
                        for instance_id in create_these_ids
                    ]

                    instance_result = self.cdf_client.data_modeling.instances.apply(to_create)
                    retry_these.extend([node.as_id() for node in instance_result.nodes])

                    if len(ex.not_found) != len(create_these_ids):
                        missing = [
                            id_dict
                            for id_dict in ex.not_found
                            if NodeId(id_dict["instanceId"]["space"], id_dict["instanceId"]["externalId"])
                            not in retry_these
                        ]
                        missing_num = len(ex.not_found) - len(create_these_ids)
                        self.logger.error(
                            f"{missing_num} time series not found, and could not be created automatically: "
                            + str(missing)
                            + " Data will be dropped"
                        )

                # Remove entries with non-existing time series from upload queue
                upload_this = [entry for entry in upload_this if entry["instanceId"] in retry_these]

                # Upload remaining
                _upload_batch(upload_this, retries - 1)

            return upload_this

        if len(self.upload_queue) == 0:
            return

        with self.lock:
            upload_this = _upload_batch(
                [
                    {"instanceId": instance_id, "datapoints": list(datapoints)}
                    for instance_id, datapoints in self.upload_queue.items()
                    if len(datapoints) > 0
                ]
            )

            for datapoints in self.upload_queue.values():
                self.points_written.inc(len(datapoints))

            try:
                self._post_upload(upload_this)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))

            self.upload_queue.clear()
            self.logger.info(f"Uploaded {self.upload_queue_size} datapoints")
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)


class SequenceUploadQueue(AbstractUploadQueue):
    """
    Upload queue for sequences.

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one
            argument: A list of the events that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
        create_missing: Create missing sequences if possible (ie, if external id is used).
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Callable[[list[Any]], None] | None = None,
        max_queue_size: int | None = None,
        max_upload_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        create_missing: bool = False,
        cancellation_token: CancellationToken | None = None,
    ) -> None:
        # Super sets post_upload and threshold
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )
        self.upload_queue: dict[EitherId, SequenceRows] = {}
        self.sequence_metadata: dict[EitherId, dict[str, str | int | float]] = {}
        self.sequence_asset_external_ids: dict[EitherId, str] = {}
        self.sequence_dataset_external_ids: dict[EitherId, str] = {}
        self.sequence_names: dict[EitherId, str] = {}
        self.sequence_descriptions: dict[EitherId, str] = {}
        self.column_definitions: dict[EitherId, list[dict[str, str]]] = {}
        self.asset_ids: dict[str, int] = {}
        self.dataset_ids: dict[str, int] = {}
        self.create_missing = create_missing

        self.points_queued = SEQUENCES_UPLOADER_POINTS_QUEUED
        self.points_written = SEQUENCES_UPLOADER_POINTS_WRITTEN
        self.queue_size = SEQUENCES_UPLOADER_QUEUE_SIZE

    def set_sequence_metadata(
        self,
        metadata: dict[str, str | int | float],
        id: int | None = None,  # noqa: A002
        external_id: str | None = None,
        asset_external_id: str | None = None,
        dataset_external_id: str | None = None,
        name: str | None = None,
        description: str | None = None,
    ) -> None:
        """
        Set sequence metadata.

        Metadata will be cached until the sequence is created. The metadata will be updated if the sequence already
        exists.

        Args:
            metadata: Sequence metadata
            id: Sequence internal ID
                Use if external_id is None
            external_id: Sequence external ID
                Use if id is None
            asset_external_id: Sequence asset external ID
            dataset_external_id: Sequence dataset external ID
            name: Sequence name
            description: Sequence description
        """
        either_id = EitherId(id=id, external_id=external_id)
        self.sequence_metadata[either_id] = metadata
        if asset_external_id:
            self.sequence_asset_external_ids[either_id] = asset_external_id
        if dataset_external_id:
            self.sequence_dataset_external_ids[either_id] = dataset_external_id
        if name:
            self.sequence_names[either_id] = name
        if description:
            self.sequence_descriptions[either_id] = description

    def set_sequence_column_definition(
        self,
        col_def: list[dict[str, str]],
        id: int | None = None,  # noqa: A002
        external_id: str | None = None,
    ) -> None:
        """
        Set sequence column definition.

        Args:
            col_def: Sequence column definition
            id: Sequence internal ID
                Use if external_id is None
            external_id: Sequence external ID
                Us if id is None
        """
        either_id = EitherId(id=id, external_id=external_id)
        self.column_definitions[either_id] = col_def

    def add_to_upload_queue(
        self,
        rows: dict[int, list[int | float | str]]
        | list[tuple[int, int | float | str]]
        | list[dict[str, Any]]
        | SequenceData
        | SequenceRows,
        column_external_ids: list[dict] | None = None,
        id: int | None = None,  # noqa: A002
        external_id: str | None = None,
    ) -> None:
        """
        Add sequence rows to upload queue.

        Mirrors implementation of SequenceApi.insert. Inserted rows will be cached until uploaded.

        Args:
            rows: The rows to be inserted. Can either be a list of tuples, a list of ["rownumber": ..., "values": ...]
                objects, a dictionary of rowNumber: data, or a SequenceData object.
            column_external_ids: list of external id for the columns of the sequence
            id: Sequence internal ID
                Use if external_id is None
            external_id: Sequence external ID
                Us if id is None
        """
        if len(rows) == 0:
            pass

        either_id = EitherId(id=id, external_id=external_id)

        if isinstance(rows, SequenceRows):
            # Already in the desired format
            pass
        elif isinstance(rows, dict | list):
            rows_raw: list[dict[str, Any]]
            if isinstance(rows, dict):
                rows_raw = [{"rowNumber": row_number, "values": values} for row_number, values in rows.items()]
            elif isinstance(rows, list) and rows and isinstance(rows[0], tuple | list):
                rows_raw = [{"rowNumber": row_number, "values": values} for row_number, values in rows]
            else:
                rows_raw = rows  # type: ignore[assignment]
            rows = SequenceRows.load(
                {
                    "rows": rows_raw,
                    "columns": column_external_ids,
                    "id": id,
                    "externalId": external_id,
                }
            )
        else:
            raise TypeError(f"Unsupported type for sequence rows: {type(rows)}")

        with self.lock:
            seq = self.upload_queue.get(either_id)
            if seq is not None:
                # Update sequence
                seq.rows.extend(rows.rows)  # type: ignore[attr-defined]

                self.upload_queue[either_id] = seq
            else:
                self.upload_queue[either_id] = rows
            self.upload_queue_size = sum([len(rows) for rows in self.upload_queue.values()])
            self.queue_size.set(self.upload_queue_size)
            self.points_queued.inc()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards.
        """

        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def _upload_single(either_id: EitherId, upload_this: SequenceData) -> SequenceData:
            self.logger.debug(f"Writing {len(upload_this.values)} rows to sequence {either_id}")

            try:
                self.cdf_client.sequences.data.insert(
                    id=either_id.internal_id,
                    external_id=either_id.external_id,
                    rows=upload_this,
                    column_external_ids=None,
                )
            except CogniteNotFoundError as ex:
                if self.create_missing:
                    # Create missing sequence
                    self._create_or_update(either_id)

                    # Retry
                    self.cdf_client.sequences.data.insert(
                        id=either_id.internal_id,
                        external_id=either_id.external_id,
                        rows=upload_this,
                        column_external_ids=None,
                    )
                else:
                    raise ex

            return upload_this

        if len(self.upload_queue) == 0:
            return

        with self.lock:
            if self.create_missing:
                self._resolve_asset_ids()
                self._resolve_dataset_ids()

            for either_id, upload_this in self.upload_queue.items():
                _upload_single(either_id, upload_this)
                self.points_written.inc()

            try:
                self._post_upload([seqdata for _, seqdata in self.upload_queue.items()])
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))

            self.logger.info(f"Uploaded {self.upload_queue_size} sequence rows")
            self.upload_queue.clear()
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    def _create_or_update(self, either_id: EitherId) -> None:
        """
        Create or update sequence, based on provided metadata and column definitions.

        Args:
            either_id: Id/External Id of sequence to be updated
        """
        column_def = self.column_definitions.get(either_id)
        if column_def is None:
            self.logger.error(f"Can't create sequence {either_id!s}, no column definitions provided")

        try:
            seq = self.cdf_client.sequences.create(
                Sequence(
                    id=either_id.internal_id,
                    external_id=either_id.external_id,
                    name=self.sequence_names.get(either_id, None),
                    description=self.sequence_descriptions.get(either_id, None),
                    metadata=self.sequence_metadata.get(either_id, None),
                    asset_id=self.asset_ids.get(self.sequence_asset_external_ids.get(either_id, None), None),  # type: ignore
                    data_set_id=self.dataset_ids.get(self.sequence_dataset_external_ids.get(either_id, None), None),  # type: ignore
                    columns=column_def,  # type: ignore  # We already  checked for None, mypy is wrong
                )
            )

        except CogniteDuplicatedError:
            self.logger.info(f"Sequence already exist: {either_id}")
            seq = self.cdf_client.sequences.retrieve(  # type: ignore [assignment]
                id=either_id.internal_id,
                external_id=either_id.external_id,
            )

        # Update definition of cached sequence
        cseq = self.upload_queue[either_id]
        cseq.columns = seq.columns  # type: ignore[assignment]

    def _resolve_asset_ids(self) -> None:
        """
        Resolve id of assets if specified, for use in sequence creation.
        """
        assets = set(self.sequence_asset_external_ids.values())
        assets.discard(None)  # type: ignore  # safeguard, remove Nones if any

        if len(assets) > 0:
            try:
                self.asset_ids = {
                    asset.external_id: asset.id
                    for asset in self.cdf_client.assets.retrieve_multiple(
                        external_ids=list(assets), ignore_unknown_ids=True
                    )
                    if asset.id is not None and asset.external_id is not None
                }
            except Exception as e:
                self.logger.error("Error in resolving asset id: %s", str(e))
                self.asset_ids = {}

    def _resolve_dataset_ids(self) -> None:
        """
        Resolve id of datasets if specified, for use in sequence creation.
        """
        datasets = set(self.sequence_dataset_external_ids.values())
        datasets.discard(None)  # type: ignore  # safeguard, remove Nones if any

        if len(datasets) > 0:
            try:
                self.dataset_ids = {
                    dataset.external_id: dataset.id
                    for dataset in self.cdf_client.data_sets.retrieve_multiple(
                        external_ids=list(datasets), ignore_unknown_ids=True
                    )
                    if dataset.id is not None and dataset.external_id is not None
                }
            except Exception as e:
                self.logger.error("Error in resolving dataset id: %s", str(e))
                self.dataset_ids = {}

    def __enter__(self) -> "SequenceUploadQueue":
        """
        Wraps around start method, for use as context manager.

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """
        Wraps around stop method, for use as context manager.

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()

    def __len__(self) -> int:
        """
        The size of the upload queue.

        Returns:
            Number of data points in queue
        """
        return self.upload_queue_size
