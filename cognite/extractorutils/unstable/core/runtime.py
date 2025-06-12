"""
Module providing the runtime for an extractor.

The runtime is responsible for starting the extractor in a separate process, managing its lifecycle, and handling
configuration loading and updates. It also handles errors and restarts the extractor if necessary.

It is the preferred way to run an extractor, as it provides a more robust and flexible way to manage the extractor's
lifecycle compared to running it directly in the main process.

The runtime also contains a command line interface (CLI) for starting the extractor, which allows users to specify
the connection configuration and other parameters.

.. code-block:: python

    from cognite.extractorutils.unstable.core.runtime import Runtime
    from my_extractor import MyExtractor

    def main() -> None:
        runtime = Runtime(MyExtractor)
        runtime.run()

    if __name__ == "__main__":
        main()
"""

import logging
import os
import sys
import time
from argparse import ArgumentParser, Namespace
from multiprocessing import Process, Queue
from pathlib import Path
from random import randint
from typing import Any, Generic, TypeVar
from uuid import uuid4

from requests.exceptions import ConnectionError as RequestsConnectionError
from typing_extensions import assert_never

from cognite.client import CogniteClient
from cognite.client.exceptions import (
    CogniteAPIError,
    CogniteAuthError,
    CogniteConnectionError,
)
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.exceptions import InvalidConfigError
from cognite.extractorutils.unstable.configuration.loaders import (
    load_file,
    load_from_cdf,
)
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core._dto import Error
from cognite.extractorutils.unstable.core.errors import ErrorLevel
from cognite.extractorutils.util import now

from ._messaging import RuntimeMessage
from .base import ConfigRevision, ConfigType, Extractor, FullConfig

__all__ = ["ExtractorType", "Runtime"]

ExtractorType = TypeVar("ExtractorType", bound=Extractor)


class Runtime(Generic[ExtractorType]):
    """
    The runtime for an extractor.

    This class is responsible for starting the extractor in a separate process, managing its lifecycle, and handling
    configuration loading and updates. It also handles errors and restarts the extractor if necessary.
    """

    RETRY_CONFIG_INTERVAL = 30

    def __init__(
        self,
        extractor: type[ExtractorType],
    ) -> None:
        self._extractor_class = extractor
        self._cancellation_token = CancellationToken()
        self._cancellation_token.cancel_on_interrupt()
        self._message_queue: Queue[RuntimeMessage] = Queue()
        self.logger = logging.getLogger(f"{self._extractor_class.EXTERNAL_ID}.runtime")
        self._setup_logging()

        self._cognite_client: CogniteClient

    def _create_argparser(self) -> ArgumentParser:
        argparser = ArgumentParser(
            prog=sys.argv[0],
            description=self._extractor_class.DESCRIPTION,
        )
        argparser.add_argument(
            "-v",
            "--version",
            action="version",
            version=f"{self._extractor_class.NAME} v{self._extractor_class.VERSION}",
        )
        argparser.add_argument(
            "-c",
            "--connection-config",
            nargs=1,
            type=Path,
            required=True,
            help="Connection parameters",
        )
        argparser.add_argument(
            "-f",
            "--force-local-config",
            nargs=1,
            type=Path,
            required=False,
            default=None,
            help="Include to use a local application configuration instead of fetching it from CDF",
        )
        argparser.add_argument(
            "--skip-init-checks",
            action="store_true",
            help="Skip any checks during startup. Useful for debugging, not recommended for production deployments.",
        )

        return argparser

    def _setup_logging(self) -> None:
        # TODO: Figure out file logging for runtime
        fmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d UTC [%(levelname)-8s] %(process)d %(threadName)s - %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        # Set logging to UTC
        fmt.converter = time.gmtime

        root = logging.getLogger()
        root.setLevel(logging.INFO)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(fmt)

        root.addHandler(console_handler)

    def _inner_run(
        self,
        message_queue: Queue,
        config: FullConfig,
    ) -> None:
        # This code is run inside the new extractor process
        extractor = self._extractor_class._init_from_runtime(config)
        extractor._set_runtime_message_queue(message_queue)

        try:
            with extractor:
                extractor.run()

        except Exception:
            self.logger.exception("Extractor crashed, will attempt restart")
            message_queue.put(RuntimeMessage.RESTART)

    def _spawn_extractor(
        self,
        config: FullConfig,
    ) -> Process:
        self._message_queue = Queue()
        process = Process(
            target=self._inner_run,
            args=(self._message_queue, config),
        )

        process.start()
        self.logger.info(f"Started extractor with PID {process.pid}")
        return process

    def _try_get_application_config(
        self,
        args: Namespace,
        connection_config: ConnectionConfig,
    ) -> tuple[ConfigType, ConfigRevision]:
        current_config_revision: ConfigRevision

        if args.local_override:
            self.logger.info("Loading local application config")

            current_config_revision = "local"
            try:
                application_config = load_file(args.local_override[0], self._extractor_class.CONFIG_TYPE)
            except InvalidConfigError as e:
                self.logger.critical(str(e))
                raise e
            except FileNotFoundError as e:
                self.logger.critical(str(e))
                raise InvalidConfigError(str(e)) from e

        else:
            self.logger.info("Loading application config from CDF")

            application_config, current_config_revision = load_from_cdf(
                self._cognite_client,
                connection_config.integration,
                self._extractor_class.CONFIG_TYPE,
            )

        return application_config, current_config_revision

    def _safe_get_application_config(
        self,
        args: Namespace,
        connection_config: ConnectionConfig,
    ) -> tuple[ConfigType, ConfigRevision] | None:
        prev_error: str | None = None

        while not self._cancellation_token.is_cancelled:
            try:
                return self._try_get_application_config(args, connection_config)

            except Exception as e:
                error_message = str(e)
                if error_message == prev_error:
                    # Same error as before, no need to log it again
                    self._cancellation_token.wait(randint(1, self.RETRY_CONFIG_INTERVAL))
                    continue
                prev_error = error_message

                ts = now()
                error = Error(
                    external_id=str(uuid4()),
                    level=ErrorLevel.fatal.value,
                    start_time=ts,
                    end_time=ts,
                    description=error_message,
                    details=None,
                    task=None,
                )

                self._cognite_client.post(
                    f"/api/v1/projects/{self._cognite_client.config.project}/odin/checkin",
                    json={
                        "externalId": connection_config.integration,
                        "errors": [error.model_dump()],
                    },
                    headers={"cdf-version": "alpha"},
                )

                self._cancellation_token.wait(randint(1, self.RETRY_CONFIG_INTERVAL))

        return None

    def _verify_connection_config(self, connection_config: ConnectionConfig) -> bool:
        self._cognite_client = connection_config.get_cognite_client(
            f"{self._extractor_class.EXTERNAL_ID}-{self._extractor_class.VERSION}"
        )
        try:
            self._cognite_client.post(
                f"/api/v1/projects/{self._cognite_client.config.project}/odin/checkin",
                json={
                    "externalId": connection_config.integration,
                },
                headers={"cdf-version": "alpha"},
            )

        except CogniteConnectionError as e:
            if e.__cause__ is not None:
                self.logger.error(str(e.__cause__))
            self.logger.critical("Could not connect to CDF. Please check your configuration.")
            return False

        except CogniteAuthError as e:
            # Error while fetching auth token
            self.logger.error(str(e))
            self.logger.critical("Could not get an access token. Please check your configuration.")
            return False

        except CogniteAPIError as e:
            # Error response from the CDF API
            if e.code == 401:
                self.logger.critical(
                    "Got a 401 error from CDF. Please check your configuration. "
                    "Make sure the credentials and project is correct."
                )

            elif e.message:
                self.logger.critical(str(e.message))

            else:
                self.logger.critical(f"Error while connecting to CDF {e!s}")

            return False

        except RequestsConnectionError as e:
            # This is sometime thrown, I've seen it when trying to get an auth token but it might happen elsewhere too
            self.logger.error(str(e))
            self.logger.critical("Could not initiate connection. Please check your configuration.")
            return False

        return True

    def run(self) -> None:
        """
        Run the extractor runtime.

        This is intended as the main entry point for the extractor runtime, and starts by parsing command line
        arguments.
        """
        argparser = self._create_argparser()
        args = argparser.parse_args()

        self.logger.info(f"Started runtime with PID {os.getpid()}")

        try:
            connection_config = load_file(args.connection_config[0], ConnectionConfig)
        except InvalidConfigError as e:
            self.logger.error(str(e))
            self.logger.critical("Could not load connection config")
            sys.exit(1)

        if not args.skip_init_checks and not self._verify_connection_config(connection_config):
            sys.exit(1)

        # This has to be Any. We don't know the type of the extractors' config at type checking since the self doesn't
        # exist yet, and I have not found a way to represent it in a generic way that isn't just an Any in disguise.
        application_config: Any
        config: tuple[Any, ConfigRevision] | None

        while not self._cancellation_token.is_cancelled:
            config = self._safe_get_application_config(args, connection_config)
            if config is None:
                if self._cancellation_token.is_cancelled:
                    break
                continue

            application_config, current_config_revision = config

            # Start extractor in separate process, and wait for it to end
            process = self._spawn_extractor(
                FullConfig(
                    connection_config=connection_config,
                    application_config=application_config,
                    current_config_revision=current_config_revision,
                )
            )
            process.join()

            # Check if we are asked to restart the extractor, shut down otherwise
            if not self._message_queue.empty():
                message = self._message_queue.get_nowait()
                match message:
                    case RuntimeMessage.RESTART:
                        continue

                    case _:
                        assert_never(message)

            else:
                self.logger.info("Shutting down runtime")
                self._cancellation_token.cancel()
