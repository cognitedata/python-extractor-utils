from datetime import datetime
from typing import Any, Iterator, List

import jsonlines


class FileErrorMapping:
    def __init__(self, file_name: str, error_reason: str) -> None:
        self.file_name = file_name
        self.error_reason = error_reason

    def __iter__(self) -> Iterator[List[str]]:
        return iter([[self.file_name, self.error_reason]])


class FileFailureManager:
    MAX_QUEUE_SIZE = 500
    START_TIME_KEY = "start_time"
    FILE_REASON_MAP_KEY = "file_error_reason_map"

    def __init__(self, start_time: str | None = None, path_to_file: str | None = None) -> None:
        self.failure_logs: dict[str, Any] = {}

        self.path_to_failure_log: str = self._pre_process_file_extension(path_to_file)
        self.start_time = start_time or str(datetime.now())
        self._initialize_failure_logs()

    def _pre_process_file_extension(self, path_to_file: str | None) -> str:
        if path_to_file and not path_to_file.endswith(".jsonl"):
            return path_to_file + ".jsonl"
        return str(path_to_file)

    def _initialize_failure_logs(self) -> None:
        self.failure_logs[FileFailureManager.START_TIME_KEY] = self.start_time
        self.failure_logs[FileFailureManager.FILE_REASON_MAP_KEY] = {}

    def __len__(self) -> int:
        return len(self.failure_logs[FileFailureManager.FILE_REASON_MAP_KEY])

    def clear(self) -> None:
        self.failure_logs.clear()
        self._initialize_failure_logs()

    def add(self, file_name: str, error_reason: str) -> None:
        error_file_object = FileErrorMapping(file_name=file_name, error_reason=error_reason)

        self.failure_logs[FileFailureManager.FILE_REASON_MAP_KEY].update(error_file_object)

        if len(self) >= self.MAX_QUEUE_SIZE:
            self.write_to_file()
            self.clear()

    def write_to_file(self) -> None:
        if len(self.failure_logs[self.FILE_REASON_MAP_KEY]) == 0:
            return

        with jsonlines.open(self.path_to_failure_log, mode="a") as writer:
            writer.write(self.failure_logs)
