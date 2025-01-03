import logging
import os
import sys
import time
from argparse import ArgumentParser, Namespace
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Any, Generic, Type, TypeVar
from uuid import uuid4

from requests.exceptions import ConnectionError
from typing_extensions import assert_never

from cognite.client.exceptions import CogniteAPIError, CogniteAuthError, CogniteConnectionError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.exceptions import InvalidConfigError
from cognite.extractorutils.unstable.configuration.loaders import load_file, load_from_cdf
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig
from cognite.extractorutils.unstable.core._dto import Error
from cognite.extractorutils.util import now

from ._messaging import RuntimeMessage
from .base import ConfigRevision, ConfigType, Extractor, FullConfig

__all__ = ["Runtime", "ExtractorType"]

ExtractorType = TypeVar("ExtractorType", bound=Extractor)


class Runtime(Generic[ExtractorType]):
    def __init__(
        self,
        extractor: Type[ExtractorType],
    ) -> None:
        self._extractor_class = extractor
        self._cancellation_token = CancellationToken()
        self._cancellation_token.cancel_on_interrupt()
        self._message_queue: Queue[RuntimeMessage] = Queue()
        self.logger = logging.getLogger(f"{self._extractor_class.EXTERNAL_ID}.runtime")
        self._setup_logging()

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
            "-l",
            "--local-override",
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

    def _get_application_config(
        self,
        args: Namespace,
        connection_config: ConnectionConfig,
    ) -> tuple[ConfigType, ConfigRevision, ConfigRevision]:
        current_config_revision: ConfigRevision
        newest_config_revision: ConfigRevision

        if args.local_override:
            self.logger.info("Loading local application config")

            current_config_revision = "local"
            newest_config_revision = "local"
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
            client = connection_config.get_cognite_client(
                f"{self._extractor_class.EXTERNAL_ID}-{self._extractor_class.VERSION}"
            )

            errors: list[Error] = []

            revision: int | None = None
            try:
                while True:
                    try:
                        application_config, current_config_revision = load_from_cdf(
                            client,
                            connection_config.integration,
                            self._extractor_class.CONFIG_TYPE,
                            revision=revision,
                        )
                        break

                    except InvalidConfigError as e:
                        if e.attempted_revision is None:
                            # Should never happen, attempted_revision is set in every handler in load_from_cdf, but it's
                            # needed for type checks to pass
                            raise e

                        self.logger.error(f"Revision {e.attempted_revision} is invalid: {e.message}")

                        t = now()
                        errors.append(
                            Error(
                                external_id=str(uuid4()),
                                level="error",
                                description=f"Revision {e.attempted_revision} is invalid",
                                details=e.message,
                                start_time=t,
                                end_time=t,
                                task=None,
                            )
                        )

                        if revision is None:
                            revision = e.attempted_revision - 1
                            newest_config_revision = e.attempted_revision
                        else:
                            revision -= 1

                        if revision > 0:
                            self.logger.info(f"Falling back to revision {revision}")
                        else:
                            self.logger.critical("No more revisions to fall back to")
                            raise e

            finally:
                if errors:
                    client.post(
                        f"/api/v1/projects/{client.config.project}/odin/checkin",
                        json={
                            "externalId": connection_config.integration,
                            "errors": [e.model_dump() for e in errors],
                        },
                        headers={"cdf-version": "alpha"},
                    )

        return application_config, current_config_revision, newest_config_revision

    def _verify_connection_config(self, connection_config: ConnectionConfig) -> bool:
        client = connection_config.get_cognite_client(
            f"{self._extractor_class.EXTERNAL_ID}-{self._extractor_class.VERSION}"
        )
        try:
            client.post(
                f"/api/v1/projects/{client.config.project}/odin/checkin",
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
                self.logger.critical(f"Error while connecting to CDF {str(e)}")

            return False

        except ConnectionError as e:
            # This is sometime thrown, I've seen it when trying to get an auth token but it might happen elsewhere too
            self.logger.error(str(e))
            self.logger.critical("Could not initiate connection. Please check your configuration.")
            return False

        return True

    def run(self) -> None:
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

        # This has to be Any. We don't know the type of the extractors' config at type checking since the sel doesn't
        # exist yet, and I have not found a way to represent it in a generic way that isn't just an Any in disguise.
        application_config: Any
        while not self._cancellation_token.is_cancelled:
            try:
                application_config, current_config_revision, newest_config_revision = self._get_application_config(
                    args, connection_config
                )

            except InvalidConfigError:
                self.logger.critical("Could not get a valid application config file. Shutting down")
                sys.exit(1)

            # Start extractor in separate process, and wait for it to end
            process = self._spawn_extractor(
                FullConfig(
                    connection_config=connection_config,
                    application_config=application_config,
                    current_config_revision=current_config_revision,
                    newest_config_revision=newest_config_revision,
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
