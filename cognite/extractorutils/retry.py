import logging
import random
import threading
from functools import partial
from typing import Iterable, Optional, Tuple, Type, Union

from decorator import decorator

logging_logger = logging.getLogger(__name__)


def _retry_internal(
    f,
    cancelation_token: threading.Event = threading.Event(),
    exceptions: Iterable[Type[Exception]] = Exception,
    tries: int = -1,
    delay: float = 0,
    max_delay: Optional[float] = None,
    backoff: float = 1,
    jitter: Union[float, Tuple[float, float]] = 0,
    logger: logging.Logger = logging_logger,
):
    _tries, _delay = tries, delay
    while _tries and not cancelation_token.is_set():
        try:
            return f()
        except exceptions as e:
            _tries -= 1
            if not _tries:
                raise

            if logger is not None:
                logger.warning("%s, retrying in %s seconds...", e, _delay)

            cancelation_token.wait(_delay)
            _delay *= backoff

            if isinstance(jitter, tuple):
                _delay += random.uniform(*jitter)
            else:
                _delay += jitter

            if max_delay is not None:
                _delay = min(_delay, max_delay)


def retry(
    cancelation_token: threading.Event = threading.Event(),
    exceptions: Iterable[Type[Exception]] = Exception,
    tries: int = -1,
    delay: float = 0,
    max_delay: Optional[float] = None,
    backoff: float = 1,
    jitter: Union[float, Tuple[float, float]] = 0,
    logger: logging.Logger = logging_logger,
):
    """Returns a retry decorator.

    This is adapted from https://github.com/invl/retry

    Args:
        cancelation_token: a threading token that is waited on.
        exceptions: an exception or a tuple of exceptions to catch. default: Exception.
        tries: the maximum number of attempts. default: -1 (infinite).
        delay: initial delay between attempts. default: 0.
        max_delay: the maximum value of delay. default: None (no limit).
        backoff: multiplier applied to delay between attempts. default: 1 (no backoff).
        jitter: extra seconds added to delay between attempts. default: 0.
                   fixed if a number, random if a range tuple (min, max)
        logger: logger.warning(fmt, error, delay) will be called on failed attempts.
                   default: retry.logging_logger. if None, logging is disabled.
    """

    @decorator
    def retry_decorator(f, *fargs, **fkwargs):
        args = fargs if fargs else list()
        kwargs = fkwargs if fkwargs else dict()

        return _retry_internal(
            partial(f, *args, **kwargs), cancelation_token, exceptions, tries, delay, max_delay, backoff, jitter, logger
        )

    return retry_decorator
