"""
A module containing utilities meant for use inside the extractor-utils package
"""
from typing import Union


class _MockLogger:
    """
    A class with an interface similar to logging.Logger that does nothing.
    """

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass

    def critical(self, *args, **kwargs):
        pass

    def log(self, *args, **kwargs):
        pass

    def debug(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass


class _EitherId:
    def __init__(self, id: int = None, externalId: str = None):
        if id is None and externalId is None:
            raise TypeError("Either id or external_id must be set")

        if id is not None and externalId is not None:
            raise TypeError("Only one of id and external_id can be set")

        self.id = id
        self.externalId = externalId

    def type(self) -> str:
        return "id" if self.id is not None else "externalId"

    def content(self) -> Union[int, str]:
        return self.id or self.externalId

    def __eq__(self, other) -> bool:
        return self.id == other.id and self.externalId == other.externalId

    def __hash__(self) -> int:
        return hash((self.id, self.externalId))

    def __str__(self) -> str:
        return "{}: {}".format(self.type(), self.content())

    def __repr__(self) -> str:
        return self.__str__()


def _resolve_log_level(level: str) -> int:
    return {"NOTSET": 0, "DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}[level.upper()]
