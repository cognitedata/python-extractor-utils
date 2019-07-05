"""
A module containing utilites ment for use inside the extractor-utils package
"""


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
