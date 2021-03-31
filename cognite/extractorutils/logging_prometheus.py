import logging

from prometheus_client import Counter

log_entries = Counter(
    "cognite_python_logging_messages", "Count of log entries by logger and level.", ["logger", "level"]
)


class ExportingLogHandler(logging.Handler):
    """A LogHandler that exports logging metrics for Prometheus.io."""

    def emit(self, record):
        log_entries.labels(record.name, record.levelname).inc()


def export_log_stats_on_root_logger(logger=logging.getLogger()):
    """Attaches an ExportingLogHandler to the root logger.
    This should be sufficient to get metrics about all logging in a
    Python application, unless a part of the application defines its
    own logger and sets this logger's `propagate` attribute to
    False. The `propagate` attribute is True by default, which means
    that by default all loggers propagate all their logged messages to
    the root logger.

    param: logger the logger to attach to (root logger is default)
    """

    logger.addHandler(ExportingLogHandler())
