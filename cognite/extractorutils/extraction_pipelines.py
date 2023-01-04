#  Copyright 2022 Cognite AS
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
from functools import wraps
from threading import Event, Thread

from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRun


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
    # TODO 2. Change 'cognite_client.extraction_pipeline_runs' -> 'cognite_client.extraction_pipelines.runs'
    # when Cognite SDK is updated

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
                cognite_client.extraction_pipeline_runs.create(  # cognite_client.extraction_pipelines.runs.create(
                    ExtractionPipelineRun(external_id=extraction_pipeline_ext_id, status="success", message=message)
                )

            def _report_error(exception: Exception) -> None:
                """
                Called on an unsuccessful exit of the extractor
                """
                message = (
                    f"Exception for function '{input_function.__name__}'. {added_message}:\n" f"{str(exception)[:1000]}"
                )
                cognite_client.extraction_pipeline_runs.create(
                    ExtractionPipelineRun(external_id=extraction_pipeline_ext_id, status="failure", message=message)
                )

            def heartbeat_loop() -> None:
                while not cancellation_token.is_set():
                    cognite_client.extraction_pipeline_runs.create(
                        ExtractionPipelineRun(external_id=extraction_pipeline_ext_id, status="seen")
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
