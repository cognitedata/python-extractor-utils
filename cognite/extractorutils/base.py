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

import argparse
import logging
import sys
import traceback
from dataclasses import is_dataclass
from threading import Event, Thread
from types import TracebackType
from typing import Any, Callable, Dict, Generic, Optional, Type

from cognite.client import CogniteClient
from cognite.extractorutils.configtools import CustomConfigClass, StateStoreConfig, load_yaml
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.statestore import AbstractStateStore, NoStateStore
from cognite.extractorutils.util import set_event_on_interrupt


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
        cancelation_token: An event that will be set when the extractor should shut down, an empty one will be created
            if omitted.
        config_file_path: If supplied, the extractor will not use command line arguments to get a config file, but
            rather use the supplied path.
    """

    def __init__(
        self,
        *,
        name: str,
        description: str,
        version: Optional[str] = None,
        run_handle: Optional[Callable[[CogniteClient, AbstractStateStore, CustomConfigClass, Event], None]] = None,
        config_class: Type[CustomConfigClass],
        metrics: Optional[BaseMetrics] = None,
        use_default_state_store: bool = True,
        cancelation_token: Event = Event(),
        config_file_path: Optional[str] = None,
    ):
        self.name = name
        self.description = description
        self.run_handle = run_handle
        self.config_class = config_class
        self.use_default_state_store = use_default_state_store
        self.version = version
        self.cancelation_token = cancelation_token
        self.config_file_path = config_file_path

        self.started = False
        self.configured_logger = False

        self.cognite_client: CogniteClient
        self.state_store: AbstractStateStore
        self.config: CustomConfigClass

        if metrics:
            self.metrics = metrics
        else:
            self.metrics = BaseMetrics(extractor_name=name, extractor_version=version)

    def _load_config(self, override_path: Optional[str] = None) -> None:
        """
        Load a configuration file, either from the specified path, or by a path specified by the user in a command line
        arg. Will quit further execution of no path is given.

        Args:
            override_path: Optional override for file path, ie don't parse command line arguments
        """
        if override_path:
            with open(override_path) as stream:
                self.config = load_yaml(source=stream, config_type=self.config_class)

        else:
            argument_parser = argparse.ArgumentParser(sys.argv[0], description=self.description)
            argument_parser.add_argument(
                "config", nargs=1, type=str, help="The YAML file containing configuration for the extractor."
            )
            argument_parser.add_argument("-v", "--version", action="version", version=f"{self.name} v{self.version}")
            args = argument_parser.parse_args()

            with open(args.config[0], "r") as stream:
                self.config_file_path = args.config[0]
                self.config = load_yaml(source=stream, config_type=self.config_class)

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
            self.state_store = state_store_config.create_state_store(self.cognite_client, self.use_default_state_store)
        else:
            self.state_store = NoStateStore()

    def _report_success(self) -> None:
        """
        Called on a successful exit of the extractor
        """
        pass

    def _report_error(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        """
        Called on an unsuccessful exit of the extractor

        Args:
            exc_type: Type of exception that caused the extractor to fail
            exc_val: Exception object that caused the extractor to fail
            exc_tb: Stack trace of where the extractor failed
        """
        logger = logging.getLogger(__name__)
        logger.error("Unexpected error during extraction", exc_info=exc_val)

    def __enter__(self) -> "Extractor":
        """
        Configures and initializes the global logger, cognite client, loads config file, state store, etc.

        Returns:
            self
        """
        self._load_config(override_path=self.config_file_path)

        set_event_on_interrupt(self.cancelation_token)

        if not self.configured_logger:
            self.config.logger.setup_logging()
            self.configured_logger = True

        self.cognite_client = self.config.cognite.get_cognite_client(self.name)
        self._load_state_store()

        try:
            self.config.metrics.start_pushers(self.cognite_client)
        except AttributeError:
            pass

        self.state_store.initialize()

        def heartbeat_loop():
            while not self.cancelation_token.is_set():
                self.cancelation_token.wait(600)

        Thread(target=heartbeat_loop, name="HeartbeatLoop", daemon=True).start()

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
        self.cancelation_token.set()

        if self.state_store:
            self.state_store.synchronize()

        try:
            self.config.metrics.stop_pushers()
        except AttributeError:
            pass

        if exc_val:
            self._report_error(exc_type, exc_val, exc_tb)
        else:
            self._report_success()

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
            self.run_handle(self.cognite_client, self.state_store, self.config, self.cancelation_token)
        else:
            raise ValueError("No run_handle defined")
