#  Copyright 2020 Cognite AS
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

"""
The ``util`` package contains miscellaneous functions and classes that can some times be useful while developing
extractors.
"""
import logging
import random
from functools import partial, wraps
from threading import Thread
from time import time
from typing import Any, Callable, Dict, Generator, Iterable, List, Optional, Tuple, Type, TypeVar, Union

from decorator import decorator

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, ExtractionPipelineRun, TimeSeries
from cognite.client.exceptions import CogniteAPIError, CogniteException, CogniteNotFoundError
from cognite.extractorutils.threading import CancellationToken


def _ensure(endpoint: Any, items: Iterable[Any]) -> None:
    try:
        external_ids = [ts.external_id for ts in items]

        # Not doing anything with the result, only want to make sure they exist. This will throw an exception if not.
        endpoint.retrieve_multiple(external_ids=external_ids)

    except CogniteNotFoundError as e:
        # Create the missing time series
        external_ids = [obj["externalId"] for obj in e.not_found]

        create_these = [ts for ts in items if ts.external_id in external_ids]
        endpoint.create(create_these)


def ensure_time_series(cdf_client: CogniteClient, time_series: Iterable[TimeSeries]) -> None:
    """
    Ensure that all the given time series exists in CDF.

    Searches through the tenant after the external IDs of the time series given, and creates those that are missing.

    Args:
        cdf_client: Tenant to create time series in
        time_series: Time series to create
    """
    _ensure(cdf_client.time_series, time_series)


def ensure_assets(cdf_client: CogniteClient, assets: Iterable[Asset]) -> None:
    """
    Ensure that all the given assets exists in CDF.

    Searches through the tenant after the external IDs of the assets given, and creates those that are missing.

    Args:
        cdf_client: Tenant to create assets in
        assets: Assets to create
    """
    _ensure(cdf_client.assets, assets)


class EitherId:
    """
    Class representing an ID in CDF, which can either be an external or internal ID. An EitherId can only hold one ID
    type, not both.

    Args:
        id: Internal ID
        externalId or external_id: external ID

    Raises:
        TypeError: If none of both of id types are set.
    """

    def __init__(self, **kwargs: Union[int, str, None]):
        internal_id = kwargs.get("id")
        external_id = kwargs.get("externalId") or kwargs.get("external_id")

        if internal_id is None and external_id is None:
            raise TypeError("Either id or external_id must be set")

        if internal_id is not None and external_id is not None:
            raise TypeError("Only one of id and external_id can be set")

        if internal_id is not None and not isinstance(internal_id, int):
            raise TypeError("Internal IDs must be integers")

        if external_id is not None and not isinstance(external_id, str):
            raise TypeError("External IDs must be strings")

        self.internal_id: Optional[int] = internal_id
        self.external_id: Optional[str] = external_id

    def type(self) -> str:
        """
        Get the type of the ID

        Returns:
            'id' if the EitherId represents an internal ID, 'externalId' if the EitherId represents an external ID
        """
        return "id" if self.internal_id is not None else "externalId"

    def content(self) -> Union[int, str]:
        """
        Get the value of the ID

        Returns:
            The ID
        """
        return self.internal_id or self.external_id  # type: ignore  # checked to be not None in init

    def __eq__(self, other: Any) -> bool:
        """
        Compare with another object. Only returns true if other is an EitherId with the same type and content

        Args:
            other: Another object

        Returns:
            True if the other object is equal to this
        """
        if not isinstance(other, EitherId):
            return False

        return self.internal_id == other.internal_id and self.external_id == other.external_id

    def __hash__(self) -> int:
        """
        Returns a hash of the internal or external ID

        Returns:
            Hash code of ID
        """
        return hash((self.internal_id, self.external_id))

    def __str__(self) -> str:
        """
        Get a string representation of the EitherId on the format "type: content".

        Returns:
            A string rep of the EitherId
        """
        return "{}: {}".format(self.type(), self.content())

    def __repr__(self) -> str:
        """
        Get a string representation of the EitherId on the format "type: content".

        Returns:
            A string rep of the EitherId
        """
        return self.__str__()


_T1 = TypeVar("_T1")


def add_extraction_pipeline(
    extraction_pipeline_ext_id: str,
    cognite_client: CogniteClient,
    heartbeat_waiting_time: int = 600,
    added_message: str = "",
) -> Callable[[Callable[..., _T1]], Callable[..., _T1]]:
    """
    This is to be used as a decorator for extractor functions to add extraction pipeline information

    Args:
        extraction_pipeline_ext_id:
        cognite_client:
        heartbeat_waiting_time:
        added_message:

    Usage:
        If you have a function named "extract_data(*args, **kwargs)" and want to connect it to an extraction
        pipeline, you can use this decorator function as:
        @add_extraction_pipeline(
            extraction_pipeline_ext_id=<INSERT EXTERNAL ID>,
            cognite_client=<INSERT COGNITE CLIENT OBJECT>,
            logger=<INSERT LOGGER>,
        )
        def extract_data(*args, **kwargs):
            <INSERT FUNCTION BODY>
    """

    # TODO 1. Consider refactoring this decorator to share methods with the Extractor context manager in .base.py
    # as they serve a similar purpose

    cancellation_token: CancellationToken = CancellationToken()

    _logger = logging.getLogger(__name__)

    def decorator_ext_pip(input_function: Callable[..., _T1]) -> Callable[..., _T1]:
        @wraps(input_function)
        def wrapper_ext_pip(*args: Any, **kwargs: Any) -> _T1:
            ##############################
            # Setup Extraction Pipelines #
            ##############################
            _logger.info("Setting up Extraction Pipelines")

            def _report_success() -> None:
                message = f"Successful shutdown of function '{input_function.__name__}'. {added_message}"
                cognite_client.extraction_pipelines.runs.create(  # cognite_client.extraction_pipelines.runs.create(
                    ExtractionPipelineRun(
                        extpipe_external_id=extraction_pipeline_ext_id, status="success", message=message
                    )
                )

            def _report_error(exception: Exception) -> None:
                """
                Called on an unsuccessful exit of the extractor
                """
                message = (
                    f"Exception for function '{input_function.__name__}'. {added_message}:\n" f"{str(exception)[:1000]}"
                )
                cognite_client.extraction_pipelines.runs.create(
                    ExtractionPipelineRun(
                        extpipe_external_id=extraction_pipeline_ext_id, status="failure", message=message
                    )
                )

            def heartbeat_loop() -> None:
                while not cancellation_token.is_cancelled:
                    cognite_client.extraction_pipelines.runs.create(
                        ExtractionPipelineRun(extpipe_external_id=extraction_pipeline_ext_id, status="seen")
                    )

                    cancellation_token.wait(heartbeat_waiting_time)

            ##############################
            # Run the extractor function #
            ##############################
            _logger.info(f"Starting to run function: {input_function.__name__}")

            heartbeat_thread: Optional[Thread] = None
            try:
                heartbeat_thread = Thread(target=heartbeat_loop, name="HeartbeatLoop", daemon=True)
                heartbeat_thread.start()
                output = input_function(*args, **kwargs)
            except Exception as e:
                _report_error(exception=e)
                _logger.error(f"Extraction failed with exception: {e}")
                raise e
            else:
                _report_success()
                _logger.info("Extraction ran successfully")
            finally:
                cancellation_token.cancel()
                if heartbeat_thread:
                    heartbeat_thread.join()

            return output

        return wrapper_ext_pip

    return decorator_ext_pip


def throttled_loop(target_time: int, cancellation_token: CancellationToken) -> Generator[None, None, None]:
    """
    A loop generator that automatically sleeps until each iteration has taken the desired amount of time. Useful for
    when you want to avoid overloading a source system with requests.

    Example:
        This example will throttle printing to only print every 10th second:

        .. code-block:: python

            for _ in throttled_loop(10, stop_event):
                print("Hello every 10 seconds!")

    Args:
        target_time: How long (in seconds) an iteration should take om total
        cancellation_token: An Event object that will act as the stop event. When set, the loop will stop.

    Returns:
        A generator that will only yield when the target iteration time is met
    """
    logger = logging.getLogger(__name__)

    while not cancellation_token.is_cancelled:
        start_time = time()
        yield
        iteration_time = time() - start_time
        if iteration_time > target_time:
            logger.warning("Iteration time longer than target time, will not sleep")

        else:
            logger.debug(f"Iteration took {iteration_time:.1f} s, sleeping {target_time - iteration_time:.1f} s")
            cancellation_token.wait(target_time - iteration_time)


_T2 = TypeVar("_T2")


def _retry_internal(
    f: Callable[..., _T2],
    cancellation_token: CancellationToken,
    exceptions: Union[Tuple[Type[Exception], ...], Dict[Type[Exception], Callable[[Exception], bool]]],
    tries: int,
    delay: float,
    max_delay: Optional[float],
    backoff: float,
    jitter: Union[float, Tuple[float, float]],
) -> _T2:
    logger = logging.getLogger(__name__)

    while tries and not cancellation_token.is_cancelled:
        try:
            return f()

        except Exception as e:
            if isinstance(exceptions, tuple):
                for ex_type in exceptions:
                    if isinstance(e, ex_type):
                        break
                else:
                    raise e

            else:
                for ex_type in exceptions:
                    if isinstance(e, ex_type) and exceptions[ex_type](e):
                        break
                else:
                    raise e

            tries -= 1
            if not tries:
                raise e

            if logger is not None:
                logger.warning("%s, retrying in %.1f seconds...", str(e), delay)

            cancellation_token.wait(delay)
            delay *= backoff

            if isinstance(jitter, tuple):
                delay += random.uniform(*jitter)
            else:
                delay += jitter

            if max_delay is not None:
                delay = min(delay, max_delay)

    return None  # type: ignore  # unreachable, we will have raised an exception before this


def retry(
    cancellation_token: Optional[CancellationToken] = None,
    exceptions: Union[Tuple[Type[Exception], ...], Dict[Type[Exception], Callable[[Any], bool]]] = (Exception,),
    tries: int = 10,
    delay: float = 1,
    max_delay: Optional[float] = 60,
    backoff: float = 2,
    jitter: Union[float, Tuple[float, float]] = (0, 2),
) -> Callable[[Callable[..., _T2]], Callable[..., _T2]]:
    """
    Returns a retry decorator.

    This is adapted from https://github.com/invl/retry

    Args:
        cancellation_token: a threading token that is waited on.
        exceptions: a tuple of exceptions to catch, or a dictionary from exception types to a callback determining
            whether to retry the exception or not. The callback will be given the exception object as argument.
            default: retry all exceptions.
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
    def retry_decorator(f: Callable[..., _T2], *fargs: Any, **fkwargs: Any) -> _T2:
        args = fargs if fargs else []
        kwargs = fkwargs if fkwargs else {}

        return _retry_internal(
            partial(f, *args, **kwargs),
            cancellation_token or CancellationToken(),
            exceptions,
            tries,
            delay,
            max_delay,
            backoff,
            jitter,
        )

    return retry_decorator


def requests_exceptions(
    status_codes: Optional[List[int]] = None,
) -> Dict[Type[Exception], Callable[[Any], bool]]:
    """
    Retry exceptions from using the ``requests`` library. This will retry all connection and HTTP errors matching
    the given status codes.

    Example:

    .. code-block:: python

        @retry(exceptions = requests_exceptions())
        def my_function() -> None:
            ...

    """
    status_codes = status_codes or [408, 425, 429, 500, 502, 503, 504]
    # types ignored, since they are not installed as we don't depend on the package
    from requests.exceptions import HTTPError, RequestException  # type: ignore

    def handle_http_errors(exception: RequestException) -> bool:
        if isinstance(exception, HTTPError):
            response = exception.response
            if response is None:
                return True

            return response.status_code in status_codes

        else:
            return True

    return {RequestException: handle_http_errors}


def httpx_exceptions(
    status_codes: Optional[List[int]] = None,
) -> Dict[Type[Exception], Callable[[Any], bool]]:
    """
    Retry exceptions from using the ``httpx`` library. This will retry all connection and HTTP errors matching
    the given status codes.

    Example:

    .. code-block:: python

        @retry(exceptions = httpx_exceptions())
        def my_function() -> None:
            ...

    """
    status_codes = status_codes or [408, 425, 429, 500, 502, 503, 504]
    # types ignored, since they are not installed as we don't depend on the package
    from httpx import HTTPError, HTTPStatusError  # type: ignore

    def handle_http_errors(exception: HTTPError) -> bool:
        if isinstance(exception, HTTPStatusError):
            response = exception.response
            if response is None:
                return True

            return response.status_code in status_codes

        else:
            return True

    return {HTTPError: handle_http_errors}


def cognite_exceptions(
    status_codes: Optional[List[int]] = None,
) -> Dict[Type[Exception], Callable[[Any], bool]]:
    """
    Retry exceptions from using the Cognite SDK. This will retry all connection and HTTP errors matching
    the given status codes.

    Example:

    .. code-block:: python

        @retry(exceptions = cognite_exceptions())
        def my_function() -> None:
            ...
    """
    status_codes = status_codes or [408, 425, 429, 500, 502, 503, 504]

    def handle_cognite_errors(exception: CogniteException) -> bool:
        if isinstance(exception, CogniteAPIError):
            return exception.code in status_codes
        return True

    return {CogniteException: handle_cognite_errors}
