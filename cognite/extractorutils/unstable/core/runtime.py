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
from dataclasses import dataclass
from logging.handlers import NTEventLogHandler as WindowsEventHandler
from multiprocessing import Event, Process, Queue
from multiprocessing.synchronize import Event as MpEvent
from pathlib import Path
from random import randint
from threading import Thread
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
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.exceptions import InvalidArgumentError, InvalidConfigError
from cognite.extractorutils.unstable.configuration.loaders import (
    load_file,
    load_from_cdf,
)
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig, ExtractorConfig
from cognite.extractorutils.unstable.core._dto import Error
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.errors import ErrorLevel
from cognite.extractorutils.util import now

from ._messaging import RuntimeMessage
from .base import ConfigRevision, Extractor, FullConfig

__all__ = ["ExtractorType", "Runtime"]

ExtractorType = TypeVar("ExtractorType", bound=Extractor)


@dataclass
class _RuntimeControls:
    cancel_event: MpEvent
    message_queue: Queue


def _extractor_process_entrypoint(
    extractor_class: type[Extractor],
    controls: _RuntimeControls,
    config: FullConfig,
    checkin_worker: CheckinWorker,
    metrics: BaseMetrics | None = None,
) -> None:
    logger = logging.getLogger(f"{extractor_class.EXTERNAL_ID}.runtime")
    checkin_worker.active_revision = config.current_config_revision
    checkin_worker.set_on_fatal_error_handler(lambda _: on_fatal_error(controls))
    checkin_worker.set_on_revision_change_handler(lambda _: on_revision_changed(controls))
    checkin_worker.set_retry_startup(extractor_class.RETRY_STARTUP)
    if not metrics:
        metrics = BaseMetrics(extractor_name=extractor_class.NAME, extractor_version=extractor_class.VERSION)
    extractor = extractor_class._init_from_runtime(config, checkin_worker, metrics)
    extractor._attach_runtime_controls(
        cancel_event=controls.cancel_event,
        message_queue=controls.message_queue,
    )

    try:
        with extractor:
            extractor.run()

    except Exception:
        logger.exception("Extractor crashed, will attempt restart")
        controls.message_queue.put(RuntimeMessage.RESTART)


def on_revision_changed(controls: _RuntimeControls) -> None:
    """
    Handle a change in the configuration revision.

    Args:
        controls(_RuntimeControls): The runtime controls containing the message queue and cancellation event.
    """
    controls.message_queue.put(RuntimeMessage.RESTART)
    controls.cancel_event.set()


def on_fatal_error(controls: _RuntimeControls) -> None:
    """
    Handle a fatal error in the extractor.

    Args:
        logger(logging.Logger): The logger to use for logging messages.
        controls(_RuntimeControls): The runtime controls containing the message queue and cancellation event.
    """
    controls.cancel_event.set()


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
        metrics: BaseMetrics | None = None,
    ) -> None:
        self._extractor_class = extractor
        self._cancellation_token = CancellationToken()
        self._cancellation_token.cancel_on_interrupt()
        self._message_queue: Queue[RuntimeMessage] = Queue()
        self._metrics = metrics
        self.logger = logging.getLogger(f"{self._extractor_class.EXTERNAL_ID}.runtime")
        self._setup_logging()
        self._cancel_event: MpEvent | None = None

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
            "-l",
            "--log-level",
            choices=["debug", "info", "warning", "error", "critical"],
            type=str,
            required=False,
            default=None,
            help="Set the logging level for the runtime.",
        )
        argparser.add_argument(
            "--skip-init-checks",
            action="store_true",
            help="Skip any checks during startup. Useful for debugging, not recommended for production deployments.",
        )
        argparser.add_argument(
            "--cwd",
            nargs=1,
            type=Path,
            required=False,
            help="Set the current working directory for the extractor.",
        )
        argparser.add_argument(
            "--service",
            action="store_true",
            help="Run the extractor as a Windows service (only supported on Windows).",
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

        if sys.platform == "win32":
            try:
                event_log_handler = WindowsEventHandler(self._extractor_class.NAME)

                event_log_handler.setLevel(logging.INFO)
                root.addHandler(event_log_handler)

                self.logger.info("Windows Event Log handler enabled for startup.")
            except ImportError:
                self.logger.warning(
                    "Failed to import the 'pywin32' package. This should install automatically on windows. "
                    "Please try reinstalling to resolve this issue."
                )
            except Exception as e:
                self.logger.warning(f"Failed to initialize Windows Event Log handler: {e}")

    def _start_cancellation_watcher(self, mp_cancel_event: MpEvent) -> None:
        """
        Start the inter-process cancellation watcher thread.

        This creates a daemon thread that waits for the runtime's CancellationToken
        and sets the multiprocessing Event to signal the child process.
        """

        def cancellation_watcher() -> None:
            """Waits for the runtime token and sets the shared event."""
            self._cancellation_token.wait()
            mp_cancel_event.set()

        watcher_thread = Thread(target=cancellation_watcher, daemon=True, name="RuntimeCancelWatcher")
        watcher_thread.start()

    def _spawn_extractor(
        self,
        config: FullConfig,
        checkin_worker: CheckinWorker,
    ) -> Process:
        self._cancel_event = Event()

        self._start_cancellation_watcher(self._cancel_event)

        controls = _RuntimeControls(
            cancel_event=self._cancel_event,
            message_queue=self._message_queue,
        )

        process = Process(
            target=_extractor_process_entrypoint,
            args=(self._extractor_class, controls, config, checkin_worker, self._metrics),
        )

        process.start()
        self.logger.info(f"Started extractor with PID {process.pid}")
        return process

    def _try_get_application_config(
        self,
        args: Namespace,
        connection_config: ConnectionConfig,
    ) -> tuple[ExtractorConfig, ConfigRevision]:
        current_config_revision: ConfigRevision

        if args.force_local_config:
            self.logger.info("Loading local application config")

            current_config_revision = "local"
            try:
                application_config = load_file(args.force_local_config[0], self._extractor_class.CONFIG_TYPE)
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
                connection_config.integration.external_id,
                self._extractor_class.CONFIG_TYPE,
            )

        return application_config, current_config_revision

    def _try_set_cwd(self, args: Namespace) -> None:
        if args.cwd is not None and len(args.cwd) > 0:
            try:
                resolved_path = Path(args.cwd[0]).resolve(strict=True)
                os.chdir(resolved_path)
                self.logger.info(f"Changed working directory to {resolved_path}")
            except (FileNotFoundError, OSError) as e:
                self.logger.critical(f"Could not change working directory to {args.cwd[0]}: {e}")
                raise InvalidArgumentError(f"Could not change working directory to {args.cwd[0]}: {e}") from e

        self.logger.info(f"Using {os.getcwd()} as working directory")

    def _safe_get_application_config(
        self,
        args: Namespace,
        connection_config: ConnectionConfig,
    ) -> tuple[ExtractorConfig, ConfigRevision] | None:
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
                    level=ErrorLevel.fatal,
                    start_time=ts,
                    end_time=ts,
                    description=error_message,
                    details=None,
                    task=None,
                )

                self._cognite_client.post(
                    f"/api/v1/projects/{self._cognite_client.config.project}/odin/checkin",
                    json={
                        "externalId": connection_config.integration.external_id,
                        "errors": [error.model_dump(mode="json")],
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
                    "externalId": connection_config.integration.external_id,
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

        if args.service and sys.platform == "win32":
            # Import here to avoid dependency on non-Windows systems
            try:
                from simple_winservice import (  # type: ignore[import-not-found]
                    ServiceHandle,
                    register_service,
                    run_service,
                )
            except ImportError:
                self.logger.critical("simple-winservice library is not installed.")
                sys.exit(1)

            # Cancellation function for the service
            def cancel_service() -> None:
                self.logger.info("Service cancellation requested.")
                self._cancellation_token.cancel()

            # Wrap the main runtime loop in a function for the service
            def service_main(handle: ServiceHandle, service_args: list[str]) -> None:
                handle.event_log_info("Extractor Windows service is starting.")
                try:
                    self._main_runtime(args)
                except Exception as exc:
                    handle.event_log_error(f"Service crashed: {exc}")
                    self.logger.critical(f"Service crashed: {exc}", exc_info=True)
                    sys.exit(1)
                handle.event_log_info("Extractor Windows service is stopping.")

            # Register and run the service
            register_service(service_main, self._extractor_class.NAME, cancel_service)
            run_service()
            return
        elif args.service and sys.platform != "win32":
            self.logger.critical("--service is only supported on Windows.")
            sys.exit(1)

        self._main_runtime(args)

    def _main_runtime(self, args: Namespace) -> None:
        try:
            self._try_set_cwd(args)
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
        cognite_client = connection_config.get_cognite_client(
            f"{self._extractor_class.EXTERNAL_ID}-{self._extractor_class.VERSION}"
        )

        checkin_worker = CheckinWorker(
            cognite_client,
            connection_config.integration.external_id,
            self.logger,
        )

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
                    log_level_override=args.log_level,
                ),
                checkin_worker,
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
