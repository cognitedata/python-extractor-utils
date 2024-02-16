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

from types import TracebackType
from typing import Callable, List, Optional, Type

from cognite.client import CogniteClient
from cognite.client.data_classes import Event
from cognite.client.exceptions import CogniteDuplicatedError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.uploader._base import (
    RETRIES,
    RETRY_BACKOFF_FACTOR,
    RETRY_DELAY,
    RETRY_MAX_DELAY,
    AbstractUploadQueue,
)
from cognite.extractorutils.uploader._metrics import (
    EVENTS_UPLOADER_QUEUE_SIZE,
    EVENTS_UPLOADER_QUEUED,
    EVENTS_UPLOADER_WRITTEN,
)
from cognite.extractorutils.util import cognite_exceptions, retry


class EventUploadQueue(AbstractUploadQueue):
    """
    Upload queue for events

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the events that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Event]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ):
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

        self.upload_queue: List[Event] = []

        self.events_queued = EVENTS_UPLOADER_QUEUED
        self.events_written = EVENTS_UPLOADER_WRITTEN
        self.queue_size = EVENTS_UPLOADER_QUEUE_SIZE

    def add_to_upload_queue(self, event: Event) -> None:
        """
        Add event to upload queue. The queue will be uploaded if the queue size is larger than the threshold
        specified in the __init__.

        Args:
            event: Event to add
        """
        with self.lock:
            self.upload_queue.append(event)
            self.events_queued.inc()
            self.upload_queue_size += 1
            self.queue_size.set(self.upload_queue_size)

            self._check_triggers()

    def upload(self) -> None:
        """
        Trigger an upload of the queue, clears queue afterwards
        """

        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def _upload_batch() -> None:
            try:
                self.cdf_client.events.create([e for e in self.upload_queue])
            except CogniteDuplicatedError as e:
                duplicated_ids = set([dup["externalId"] for dup in e.duplicated if "externalId" in dup])
                failed: List[Event] = [e for e in e.failed]
                to_create = []
                to_update = []
                for evt in failed:
                    if evt.external_id is not None and evt.external_id in duplicated_ids:
                        to_update.append(evt)
                    else:
                        to_create.append(evt)
                if to_create:
                    self.cdf_client.events.create(to_create)
                if to_update:
                    self.cdf_client.events.update(to_update)

        if len(self.upload_queue) == 0:
            return

        with self.lock:
            _upload_batch()

            self.events_written.inc(self.upload_queue_size)

            try:
                self._post_upload(self.upload_queue)
            except Exception as e:
                self.logger.error("Error in upload callback: %s", str(e))
            self.upload_queue.clear()
            self.logger.info(f"Uploaded {self.upload_queue_size} events")
            self.upload_queue_size = 0
            self.queue_size.set(self.upload_queue_size)

    def __enter__(self) -> "EventUploadQueue":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        """
        Wraps around stop method, for use as context manager

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()
