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

import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, List, Optional

from arrow import Arrow

from cognite.client import CogniteClient
from cognite.extractorutils._inner_util import _resolve_log_level
from cognite.extractorutils.threading import CancellationToken


class AbstractUploadQueue(ABC):
    """
    Abstract uploader class.

    Args:
        cdf_client: Cognite Data Fusion client to use
        post_upload_function: A function that will be called after each upload. The function will be given one argument:
            A list of the elements that were uploaded.
        max_queue_size: Maximum size of upload queue. Defaults to no max size.
        max_upload_interval: Automatically trigger an upload each m seconds when run as a thread (use start/stop
            methods).
        trigger_log_level: Log level to log upload triggers to.
        thread_name: Thread name of uploader thread.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Optional[Callable[[List[Any]], None]] = None,
        max_queue_size: Optional[int] = None,
        max_upload_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ):
        self.cdf_client = cdf_client

        self.threshold = max_queue_size if max_queue_size is not None else -1
        self.upload_queue_size = 0

        self.trigger_log_level = _resolve_log_level(trigger_log_level)
        self.logger = logging.getLogger(__name__)

        self.thread = threading.Thread(target=self._run, daemon=cancellation_token is None, name=thread_name)
        self.lock = threading.RLock()
        self.cancellation_token: CancellationToken = (
            cancellation_token.create_child_token() if cancellation_token else CancellationToken()
        )

        self.max_upload_interval = max_upload_interval

        self.post_upload_function = post_upload_function

    def _check_triggers(self) -> None:
        """
        Check if upload triggers are met, call upload if they are. Called by subclasses.
        """
        if self.upload_queue_size >= self.threshold >= 0:
            self.logger.log(
                self.trigger_log_level,
                f"Upload queue reached threshold size {self.upload_queue_size}/{self.threshold}, triggering upload",
            )
            return self.upload()

        return None

    def _post_upload(self, uploaded: List[Any]) -> None:
        """
        Perform post_upload_function to uploaded data, if applicable

        Args:
            uploaded: List of uploaded data
        """
        if self.post_upload_function is not None:
            try:
                self.post_upload_function(uploaded)
            except Exception:
                logging.getLogger(__name__).exception("Error during upload callback")

    @abstractmethod
    def upload(self) -> None:
        """
        Uploads the queue.
        """

    def _run(self) -> None:
        """
        Internal run method for upload thread
        """
        while not self.cancellation_token.wait(timeout=self.max_upload_interval):
            try:
                self.logger.log(self.trigger_log_level, "Triggering scheduled upload")
                self.upload()
            except Exception as e:
                self.logger.error("Unexpected error while uploading: %s. Skipping this upload.", str(e))

        # trigger stop event explicitly to drain the queue
        self.stop(ensure_upload=True)

    def start(self) -> None:
        """
        Start upload thread if max_upload_interval is set, this called the upload method every max_upload_interval
        seconds.
        """
        if self.max_upload_interval is not None:
            self.thread.start()

    def stop(self, ensure_upload: bool = True) -> None:
        """
        Stop upload thread if running, and ensures that the upload queue is empty if ensure_upload is True.

        Args:
            ensure_upload (bool): (Optional). Call upload one last time after shutting down thread to ensure empty
                upload queue.
        """
        self.cancellation_token.cancel()
        if ensure_upload:
            self.upload()

    def __len__(self) -> int:
        """
        The size of the upload queue

        Returns:
            Number of events in queue
        """
        return self.upload_queue_size


@dataclass(frozen=True)
class TimestampedObject:
    payload: Any
    created: Arrow


RETRY_BACKOFF_FACTOR = 2
RETRY_MAX_DELAY = 60
RETRY_DELAY = 1
RETRIES = 10
