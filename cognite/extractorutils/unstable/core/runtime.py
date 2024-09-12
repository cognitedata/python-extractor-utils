import logging
import os
import sys
import time
from argparse import ArgumentParser, Namespace
from multiprocessing import Process, Queue
from pathlib import Path
from typing import Any, Generic, Type, TypeVar

from typing_extensions import assert_never

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.loaders import load_file, load_from_cdf
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig

from ._messaging import RuntimeMessage
from .base import ConfigRevision, ConfigType, Extractor

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

        return argparser

    def _setup_logging(self) -> None:
        # TODO: Figure out file logging for runtime
        fmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d UTC [%(levelname)-8s] %(threadName)s - %(message)s",
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
        connection_config: ConnectionConfig,
        application_config: ConfigType,
        current_config_revision: ConfigRevision,
    ) -> None:
        # This code is run inside the new extractor process
        extractor = self._extractor_class.init_from_runtime(
            connection_config,
            application_config,
            current_config_revision,
        )
        extractor._set_runtime_message_queue(message_queue)

        try:
            with extractor:
                extractor.run()

        except Exception:
            self.logger.exception("Extractor crashed, will attempt restart")
            message_queue.put(RuntimeMessage.RESTART)

    def _spawn_extractor(
        self,
        connection_config: ConnectionConfig,
        application_config: ConfigType,
        current_config_revision: ConfigRevision,
    ) -> Process:
        self._message_queue = Queue()
        process = Process(
            target=self._inner_run,
            args=(self._message_queue, connection_config, application_config, current_config_revision),
        )

        process.start()
        self.logger.info(f"Started extractor as {process.pid}")
        return process

    def _get_application_config(
        self,
        args: Namespace,
        connection_config: ConnectionConfig,
    ) -> tuple[ConfigType, ConfigRevision]:
        current_config_revision: ConfigRevision
        if args.local_override:
            current_config_revision = "local"
            application_config = load_file(args.local_override[0], self._extractor_class.CONFIG_TYPE)
        else:
            client = connection_config.get_cognite_client(
                f"{self._extractor_class.EXTERNAL_ID}-{self._extractor_class.VERSION}"
            )
            application_config, current_config_revision = load_from_cdf(
                client,
                connection_config.extraction_pipeline,
                self._extractor_class.CONFIG_TYPE,
            )

        return application_config, current_config_revision

    def run(self) -> None:
        argparser = self._create_argparser()
        args = argparser.parse_args()

        self.logger.info(f"Started runtime as {os.getpid()}")

        connection_config = load_file(args.connection_config[0], ConnectionConfig)

        # This has to be Any. We don't know the type of the extractors' config at type checking since the sel doesn't
        # exist yet, and I have not found a way to represent it in a generic way that isn't just an Any in disguise.
        application_config: Any
        while not self._cancellation_token.is_cancelled:
            application_config, current_config_revision = self._get_application_config(args, connection_config)
            # Start extractor in separate process, and wait for it to end
            process = self._spawn_extractor(connection_config, application_config, current_config_revision)
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
