import logging
import time
from functools import wraps
from threading import Event, Thread

from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipelineRun


def add_extraction_pipeline(
    extraction_pipeline_ext_id: str,
    cognite_client: CogniteClient,
    logger: logging.Logger,
    heartbeat_waiting_time: int = 5,
    added_message: str = "",
):
    """
    This is to be used as a decorator for extractor functions to add extraction pipeline information

    Args:
        extraction_pipeline_ext_id:
        cognite_client:
        logger:
        heartbeat_waiting_time:
        added_message:
        cancellation_token:

    Potential Improvements:
    -- Refactor so this decorator share methods with the Extractor context manager in .base.py as they serve a similar
    purpose
    -- Change 'cognite_client.extraction_pipeline_runs' -> 'cognite_client.extraction_pipelines.runs'
    when SDK is updated

    """

    cancellation_token: Event = Event()

    def decorator_ext_pip(input_function):
        @wraps(input_function)
        def wrapper_ext_pip(*args, **kwargs):
            ##############################
            # Setup Extraction Pipelines #
            ##############################
            logger.info("Setting up Extraction Pipelines")

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
            logger.info(f"Starting to run function: {input_function.__name__}")

            print(cancellation_token.is_set())
            try:
                heartbeat_thread = Thread(target=heartbeat_loop, name="HeartbeatLoop", daemon=True)
                heartbeat_thread.start()
                output = input_function(*args, **kwargs)
            except Exception as e:
                _report_error(exception=e)
                logger.error(f"Extraction failed with exception: {e}")
                raise e
            else:
                _report_success()
                logger.info(f"Extraction ran successfully")
            finally:
                cancellation_token.set()
                heartbeat_thread.join()

            return output

        return wrapper_ext_pip

    return decorator_ext_pip
