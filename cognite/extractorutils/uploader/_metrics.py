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

from prometheus_client import Counter, Gauge

RAW_UPLOADER_ROWS_QUEUED = Counter(
    "cognite_raw_uploader_rows_queued", "Total number of records queued", labelnames=["destination"]
)
RAW_UPLOADER_ROWS_WRITTEN = Counter(
    "cognite_raw_uploader_rows_written", "Total number of records written", labelnames=["destination"]
)
RAW_UPLOADER_ROWS_DUPLICATES = Counter(
    "cognite_raw_uploader_rows_duplicates", "Total number of duplicates found", labelnames=["destination"]
)
RAW_UPLOADER_QUEUE_SIZE = Gauge("cognite_raw_uploader_queue_size", "Internal queue size")
TIMESERIES_UPLOADER_POINTS_QUEUED = Counter(
    "cognite_timeseries_uploader_points_queued", "Total number of datapoints queued"
)
TIMESERIES_UPLOADER_POINTS_WRITTEN = Counter(
    "cognite_timeseries_uploader_points_written", "Total number of datapoints written"
)
TIMESERIES_UPLOADER_QUEUE_SIZE = Gauge("cognite_timeseries_uploader_queue_size", "Internal queue size")
TIMESERIES_UPLOADER_POINTS_DISCARDED = Counter(
    "cognite_timeseries_uploader_points_discarded",
    "Total number of datapoints discarded due to invalid timestamp or value",
)
SEQUENCES_UPLOADER_POINTS_QUEUED = Counter(
    "cognite_sequences_uploader_points_queued", "Total number of sequences queued"
)
SEQUENCES_UPLOADER_POINTS_WRITTEN = Counter(
    "cognite_sequences_uploader_points_written", "Total number of sequences written"
)
SEQUENCES_UPLOADER_QUEUE_SIZE = Gauge("cognite_sequences_uploader_queue_size", "Internal queue size")
EVENTS_UPLOADER_QUEUED = Counter("cognite_events_uploader_queued", "Total number of events queued")
EVENTS_UPLOADER_WRITTEN = Counter("cognite_events_uploader_written", "Total number of events written")
EVENTS_UPLOADER_QUEUE_SIZE = Gauge("cognite_events_uploader_queue_size", "Internal queue size")
FILES_UPLOADER_QUEUED = Counter("cognite_files_uploader_queued", "Total number of files queued")
FILES_UPLOADER_WRITTEN = Counter("cognite_files_uploader_written", "Total number of files written")
FILES_UPLOADER_QUEUE_SIZE = Gauge("cognite_files_uploader_queue_size", "Internal queue size")
BYTES_UPLOADER_QUEUED = Counter("cognite_bytes_uploader_queued", "Total number of frames queued")
BYTES_UPLOADER_WRITTEN = Counter("cognite_bytes_uploader_written", "Total number of frames written")
BYTES_UPLOADER_QUEUE_SIZE = Gauge("cognite_bytes_uploader_queue_size", "Internal queue size")
ASSETS_UPLOADER_QUEUED = Counter("cognite_assets_uploader_queued", "Total number of assets queued")
ASSETS_UPLOADER_WRITTEN = Counter("cognite_assets_uploader_written", "Total number of assets written")
ASSETS_UPLOADER_QUEUE_SIZE = Gauge("cognite_assets_uploader_queue_size", "Internal queue size")
