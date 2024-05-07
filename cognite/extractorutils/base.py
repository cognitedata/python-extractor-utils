#  Copyright 2021 Cognite AS
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
import os
import sys
from dataclasses import is_dataclass
from enum import Enum
from threading import Thread
from types import TracebackType
from typing import Any, Callable, Dict, Generic, Optional, Type, TypeVar

from dotenv import find_dotenv, load_dotenv

from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipeline, ExtractionPipelineRun
from cognite.extractorutils.configtools import BaseConfig, ConfigResolver, StateStoreConfig
from cognite.extractorutils.exceptions import InvalidConfigError
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.statestore import AbstractStateStore, LocalStateStore, NoStateStore
from cognite.extractorutils.threading import CancellationToken


class ReloadConfigAction(Enum):
    DO_NOTHING = 1
    REPLACE_ATTRIBUTE = 2
    SHUTDOWN = 3
    CALLBACK = 4


CustomConfigClass = TypeVar("CustomConfigClass", bound=BaseConfig)


class Extractor(Generic[CustomConfigClass]):
    """
    Base class for extractors.

    When used as a context manager, the Extractor class will parse command line arguments, load a configuration file,
    set up everything needed for the extractor to run, and call the ``run_handle``. If the extractor raises an
    exception, the exception will be handled by the Extractor class and logged and reported as an error.

    Args:
        name: Name of the extractor, how it's invoked from the command line.
        description: A short 1-2 sentence description of the extractor.
        version: Version number, following semantic versioning.
        run_handle: A function to call when setup is done that runs the extractor, taking a cognite client, state store
            config object and a shutdown event as arguments.
        config_class: A class (based on the BaseConfig class) that defines the configuration schema for the extractor
        metrics: Metrics collection, a default one with be created if omitted.
        use_default_state_store: Create a simple instance of the LocalStateStore to provide to the run handle. If false
            a NoStateStore will be created in its place.
        cancellation_token: An event that will be set when the extractor should shut down, an empty one will be created
            if omitted.
        config_file_path: If supplied, the extractor will not use command line arguments to get a config file, but
            rather use the supplied path.
        continuous_extractor: If True, extractor will both successful start and end time. Else, only show run on exit.
        heartbeat_waiting_time: Time interval between each heartbeat to the extraction pipeline in seconds.
    """

    _config_singleton: Optional[CustomConfigClass] = None
    _statestore_singleton: Optional[AbstractStateStore] = None

    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        run_handle: Optional[
            Callable[[CogniteClient, AbstractStateStore, CustomConfigClass, CancellationToken], None]
        ] = None,
        config_class: Type[CustomConfigClass],
        metrics: Optional[BaseMetrics] = None,
        use_default_state_store: bool = True,
        cancellation_token: Optional[CancellationToken] = None,
        config_file_path: Optional[str] = None,
        continuous_extractor: bool = False,
        heartbeat_waiting_time: int = 600,
        handle_interrupts: bool = True,
        reload_config_interval: Optional[int] = 300,
        reload_config_action: ReloadConfigAction = ReloadConfigAction.DO_NOTHING,
    ):
        self.name = name
        self.description = description
        self.run_handle = run_handle
        self.config_class = config_class
        self.use_default_state_store = use_default_state_store
        self.version = version or "unknown"
        self.cancellation_token = cancellation_token.create_child_token() if cancellation_token else CancellationToken()
        self.config_file_path = config_file_path
        self.continuous_extractor = continuous_extractor
        self.heartbeat_waiting_time = heartbeat_waiting_time
        self.handle_interrupts = handle_interrupts
        self.reload_config_interval = reload_config_interval
        self.reload_config_action = reload_config_action

        self.started = False
        self.configured_logger = False

        self.cognite_client: CogniteClient
        self.state_store: AbstractStateStore
        self.config: CustomConfigClass
        self.extraction_pipeline: Optional[ExtractionPipeline]
        self.logger: logging.Logger

        self.should_be_restarted = False

        if metrics:
            self.metrics = metrics
        else:
            self.metrics = BaseMetrics(extractor_name=name, extractor_version=self.version)

    def _initial_load_config(self, override_path: Optional[str] = None) -> None:
        """
        Load a configuration file, either from the specified path, or by a path specified by the user in a command line
        arg. Will quit further execution of no path is given.

        Args:
            override_path: Optional override for file path, ie don't parse command line arguments
        """
        if override_path:
            self.config_resolver = ConfigResolver(override_path, self.config_class)
        else:
            self.config_resolver = ConfigResolver.from_cli(self.name, self.description, self.version, self.config_class)

        self.config = self.config_resolver.config
        Extractor._config_singleton = self.config  # type: ignore

        def config_refresher() -> None:
            while not self.cancellation_token.is_cancelled:
                self.cancellation_token.wait(self.reload_config_interval)
                if self.config_resolver.has_changed:
                    self._reload_config()

        if self.reload_config_interval and self.reload_config_action != ReloadConfigAction.DO_NOTHING:
            Thread(target=config_refresher, name="ConfigReloader", daemon=True).start()

    def reload_config_callback(self) -> None:
        self.logger.error("Method for reloading configs has not been overridden in subclass")

    def _reload_config(self) -> None:
        self.logger.info("Config file has changed")

        if self.reload_config_action is ReloadConfigAction.REPLACE_ATTRIBUTE:
            self.logger.info("Loading in new config file")
            self.config_resolver.accept_new_config()
            self.config = self.config_resolver.config
            Extractor._config_singleton = self.config  # type: ignore

        elif self.reload_config_action is ReloadConfigAction.SHUTDOWN:
            self.logger.info("Shutting down, expecting to be restarted")
            self.cancellation_token.cancel()

        elif self.reload_config_action is ReloadConfigAction.CALLBACK:
            self.logger.info("Loading in new config file")
            self.config_resolver.accept_new_config()
            self.config = self.config_resolver.config
            self.reload_config_callback()

    def _load_state_store(self) -> None:
        """
        Searches through the config object for a StateStoreConfig. If found, it will use that configuration to generate
        a state store, if no such config is found it will either create a LocalStateStore or a NoStateStore depending
        on whether the  ``use_default_state_store`` argument to the constructor was true or false.

        Either way, the state_store attribute is guaranteed to be set after calling this method.
        """

        def recursive_find_state_store(d: Dict[str, Any]) -> Optional[StateStoreConfig]:
            for k in d:
                if is_dataclass(d[k]):
                    res = recursive_find_state_store(d[k].__dict__)
                    if res:
                        return res
                if isinstance(d[k], StateStoreConfig):
                    return d[k]
            return None

        state_store_config = recursive_find_state_store(self.config.__dict__)
        if state_store_config:
            self.state_store = state_store_config.create_state_store(
                cdf_client=self.cognite_client,
                default_to_local=self.use_default_state_store,
                cancellation_token=self.cancellation_token,
            )
        else:
            self.state_store = (
                LocalStateStore("states.json", cancellation_token=self.cancellation_token)
                if self.use_default_state_store
                else NoStateStore()
            )

        try:
            self.state_store.initialize()
        except ValueError:
            self.logger.exception("Could not load state store, using an empty state store as default")

        Extractor._statestore_singleton = self.state_store

    def _report_success(self) -> None:
        """
        Called on a successful exit of the extractor
        """
        if self.extraction_pipeline:
            self.logger.info("Reporting new successful run")
            self.cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=self.extraction_pipeline.external_id,
                    status="success",
                    message="Successful shutdown",
                )
            )

    def _report_error(self, exception: BaseException) -> None:
        """
        Called on an unsuccessful exit of the extractor

        Args:
            exception: Exception object that caused the extractor to fail
        """
        self.logger.error("Unexpected error during extraction", exc_info=exception)
        if self.extraction_pipeline:
            message = f"{type(exception).__name__}: {str(exception)}"[:1000]

            self.logger.info(f"Reporting new failed run: {message}")
            self.cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=self.extraction_pipeline.external_id, status="failure", message=message
                )
            )

    def __enter__(self) -> "Extractor":
        """
        Configures and initializes the global logger, cognite client, loads config file, state store, etc.

        Returns:
            self
        """

        if str(os.getenv("COGNITE_FUNCTION_RUNTIME", False)).lower() != "true":
            # Environment Variables
            env_file_path = find_dotenv(usecwd=True)
            if env_file_path:
                load_dotenv(dotenv_path=env_file_path, override=True)
                dotenv_message = f"Successfully ingested environment variables from {env_file_path}"
            else:
                dotenv_message = "No .env file found"
        else:
            dotenv_message = "No .env file imported when using Cognite Functions"

        try:
            self._initial_load_config(override_path=self.config_file_path)
        except InvalidConfigError as e:
            print("Critical error: Could not read config file", file=sys.stderr)  # noqa: T201
            print(str(e), file=sys.stderr)  # noqa: T201
            sys.exit(1)

        if not self.configured_logger:
            self.config.logger.setup_logging()
            self.configured_logger = True

        self.logger = logging.getLogger(__name__)
        self.logger.info(dotenv_message)
        self.logger.info(f"Loaded {'remote' if self.config_resolver.is_remote else 'local'} config file")

        if self.handle_interrupts:
            self.cancellation_token.cancel_on_interrupt()

        self.cognite_client = self.config.cognite.get_cognite_client(self.name)
        self._load_state_store()
        self.state_store.start()

        self.extraction_pipeline = self.config.cognite.get_extraction_pipeline(self.cognite_client)

        try:
            self.config.metrics.start_pushers(self.cognite_client)  # type: ignore
        except AttributeError:
            pass

        def heartbeat_loop() -> None:
            while not self.cancellation_token.is_cancelled:
                self.cancellation_token.wait(self.heartbeat_waiting_time)

                if not self.cancellation_token.is_cancelled:
                    self.logger.info("Reporting new heartbeat")
                    try:
                        self.cognite_client.extraction_pipelines.runs.create(
                            ExtractionPipelineRun(
                                extpipe_external_id=self.extraction_pipeline.external_id,  # type: ignore
                                status="seen",
                            )
                        )
                    except Exception:
                        self.logger.exception("Failed to report heartbeat")

        if self.extraction_pipeline:
            self.logger.info("Starting heartbeat loop")
            Thread(target=heartbeat_loop, name="HeartbeatLoop", daemon=True).start()
        else:
            self.logger.info("No extraction pipeline configured")

        if self.extraction_pipeline and self.continuous_extractor:
            self.cognite_client.extraction_pipelines.runs.create(
                ExtractionPipelineRun(
                    extpipe_external_id=self.extraction_pipeline.external_id,
                    status="success",
                    message=f"New startup of {self.name}",
                )
            )

        self.started = True
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> bool:
        """
        Shuts down the extractor. Makes sure states are preserved, that all uploads of data and metrics are done, etc.

        Args:
            exc_type: Will be provided by runtime. If an unhandled exception occurred, this will be the type of that
                exception.
            exc_val: Will be provided by runtime. If an unhandled exception occurred, this will be the instance of that
                exception.
            exc_tb: Will be provided by runtime. If an unhandled exception occurred, this will be the stack trace of
                that exception.

        Returns:
            True if the extractor shut down cleanly, False if the extractor was shut down due to an unhandled error
        """
        self.cancellation_token.cancel()

        if self.state_store:
            self.state_store.synchronize()

        try:
            self.config.metrics.stop_pushers()  # type: ignore
        except AttributeError:
            pass

        if exc_val:
            self._report_error(exc_val)
        else:
            self._report_success()

        self.started = False
        return exc_val is None

    def run(self) -> None:
        """
        Run the extractor. Ensures that the Extractor is set up correctly (``run`` called within a ``with``) and calls
        the ``run_handle``.

        Can be overrided in subclasses.
        """
        if not self.started:
            raise ValueError("You must run the extractor in a context manager")
        if self.run_handle:
            self.run_handle(self.cognite_client, self.state_store, self.config, self.cancellation_token)
        else:
            raise ValueError("No run_handle defined")

    @classmethod
    def get_current_config(cls) -> CustomConfigClass:
        if Extractor._config_singleton is None:  # type: ignore
            raise ValueError("No config singleton created. Have a config file been loaded?")
        return Extractor._config_singleton  # type: ignore

    @classmethod
    def get_current_statestore(cls) -> AbstractStateStore:
        if Extractor._statestore_singleton is None:
            raise ValueError("No state store singleton created. Have a state store been loaded?")
        return Extractor._statestore_singleton
