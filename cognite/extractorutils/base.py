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
from typing import Any, Callable, Dict, Generic, Optional, Type, TypeVar

from cognite.client import CogniteClient
from cognite.extractorutils.configtools import CustomConfigClass, StateStoreConfig, load_yaml
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.statestore import AbstractStateStore, NoStateStore
from cognite.extractorutils.util import set_event_on_interrupt


class Extractor(Generic[CustomConfigClass]):
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
    ):
        self.name = name
        self.description = description
        self.run_handle = run_handle
        self.config_class = config_class
        self.use_default_state_store = use_default_state_store
        self.version = version
        self.cancelation_token = cancelation_token

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
                self.config = load_yaml(source=stream, config_type=self.config_class)

    def _load_state_store(self) -> None:
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
        pass

    def _report_error(self, exc_type: Type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        pass

    def __enter__(self) -> "Extractor":
        self._load_config()

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
                self.cancelation_token.wait(60)

        Thread(target=heartbeat_loop, name="HeartbeatLoop", daemon=True).start()

        self.started = True
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> bool:
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
        if not self.started:
            raise ValueError("You must run the extractor in a context manager")
        if self.run_handle:
            self.run_handle(self.cognite_client, self.state_store, self.config, self.cancelation_token)
        else:
            raise ValueError("No run_handle defined")
