"""
This module provides a mechanism to handle file upload failures by logging details to a newline delimited JSON file.
"""

from collections.abc import Iterator
from datetime import datetime, timezone

import jsonlines


class FileErrorMapping:
    """
    A class to represent a mapping of file name to its error reason.
    """

    def __init__(self, file_name: str, error_reason: str) -> None:
        self.file_name = file_name
        self.error_reason = error_reason

    def __iter__(self) -> Iterator[list[str]]:
        """
        Returns an single-item iterator containing the file name and error reason.
        """
        return iter([[self.file_name, self.error_reason]])


class FileFailureManager:
    """
    A class to manage file upload failures by logging them to a newline delimited JSON file.
    """

    MAX_QUEUE_SIZE = 500
    START_TIME_KEY = "start_time"
    FILE_REASON_MAP_KEY = "file_error_reason_map"

    def __init__(self, start_time: str | None = None, path_to_file: str | None = None) -> None:
        self.failure_logs: dict[str, str] = {}

        self.path_to_failure_log: str = self._pre_process_file_extension(path_to_file)
        self.start_time = start_time or str(datetime.now(tz=timezone.utc))
        self._initialize_failure_logs()

    def _pre_process_file_extension(self, path_to_file: str | None) -> str:
        if path_to_file and not path_to_file.endswith(".jsonl"):
            return path_to_file + ".jsonl"
        return str(path_to_file)

    def _initialize_failure_logs(self) -> None:
        self.failure_logs = {}

    def __len__(self) -> int:
        """
        Returns the number of failure logs currently stored.
        """
        return len(self.failure_logs)

    def clear(self) -> None:
        """
        Clears the queue of failure logs.
        """
        self.failure_logs.clear()
        self._initialize_failure_logs()

    def add(self, file_name: str, error_reason: str) -> None:
        """
        Adds a file name and its error reason to the failure logs.

        If the number of logs exceeds the maximum queue size, it writes the logs to a file.

        Args:
            file_name: The name of the file that failed to upload.
            error_reason: The reason for the failure.
        """
        error_file_object = FileErrorMapping(file_name=file_name, error_reason=error_reason)
        error_file_dict = dict(error_file_object)

        self.failure_logs.update(error_file_dict)

        if len(self) >= self.MAX_QUEUE_SIZE:
            self.write_to_file()

    def write_to_file(self) -> None:
        """
        Flushes the current failure logs to a newline delimited JSON file and clears the queue.
        """
        if len(self) == 0:
            return

        dict_to_write = {
            self.START_TIME_KEY: self.start_time,
            self.FILE_REASON_MAP_KEY: self.failure_logs,
        }

        with jsonlines.open(self.path_to_failure_log, mode="a") as writer:
            writer.write(dict_to_write)

        self.clear()
