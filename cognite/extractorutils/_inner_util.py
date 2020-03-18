"""
A module containing utilities meant for use inside the extractor-utils package
"""


def _resolve_log_level(level: str) -> int:
    return {"NOTSET": 0, "DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}[level.upper()]
