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
import signal
from functools import wraps
from threading import Event, Thread
from typing import Any, Dict, Iterable, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, ExtractionPipelineRun, TimeSeries
from cognite.client.exceptions import CogniteNotFoundError


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


def set_event_on_interrupt(stop_event: Event) -> None:
    """
    Set given event on SIGINT (Ctrl-C) instead of throwing a KeyboardInterrupt exception.

    Args:
        stop_event: Event to set
    """

    def sigint_handler(sig, frame):
        logger = logging.getLogger(__name__)
        logger.warning("Interrupt signal received, stopping extractor gracefully")
        stop_event.set()
        logger.info("Waiting for threads to complete. Send another interrupt to force quit.")
        signal.signal(signal.SIGINT, signal.default_int_handler)

    try:
        signal.signal(signal.SIGINT, sigint_handler)
    except ValueError as e:
        logging.getLogger(__name__).warning(f"Could not register handler for interrupt signals: {str(e)}")


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

    def __init__(self, **kwargs):
        internal_id = kwargs.get("id")
        external_id = kwargs.get("externalId") or kwargs.get("external_id")

        if internal_id is None and external_id is None:
            raise TypeError("Either id or external_id must be set")

        if internal_id is not None and external_id is not None:
            raise TypeError("Only one of id and external_id can be set")

        self.internal_id = internal_id
        self.external_id = external_id

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
        return self.internal_id or self.external_id

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


def add_extraction_pipeline(
    extraction_pipeline_ext_id: str,
    cognite_client: CogniteClient,
    heartbeat_waiting_time: int = 600,
    added_message: str = "",
):
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

    cancellation_token: Event = Event()

    _logger = logging.getLogger(__name__)

    def decorator_ext_pip(input_function):
        @wraps(input_function)
        def wrapper_ext_pip(*args, **kwargs):
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
                while not cancellation_token.is_set():
                    cognite_client.extraction_pipelines.runs.create(
                        ExtractionPipelineRun(extpipe_external_id=extraction_pipeline_ext_id, status="seen")
                    )

                    cancellation_token.wait(heartbeat_waiting_time)

            ##############################
            # Run the extractor function #
            ##############################
            _logger.info(f"Starting to run function: {input_function.__name__}")

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
                _logger.info(f"Extraction ran successfully")
            finally:
                cancellation_token.set()
                heartbeat_thread.join()

            return output

        return wrapper_ext_pip

    return decorator_ext_pip
