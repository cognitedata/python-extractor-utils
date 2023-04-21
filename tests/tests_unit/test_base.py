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
import unittest
from dataclasses import dataclass
from unittest.mock import patch

import pytest

from cognite.extractorutils import Extractor
from cognite.extractorutils.configtools import BaseConfig, StateStoreConfig
from cognite.extractorutils.statestore import LocalStateStore, NoStateStore


@dataclass
class SourceConfig:
    frequency: float


@dataclass
class ExtractorConfig:
    state_store: StateStoreConfig = StateStoreConfig()


@dataclass
class ConfigWithStates(BaseConfig):
    source: SourceConfig
    extractor: ExtractorConfig = ExtractorConfig()


@dataclass
class ConfigWithoutStates(BaseConfig):
    source: SourceConfig


class TestExtractorClass(unittest.TestCase):
    def test_load_config(self):
        e1 = Extractor(name="my_extractor1", description="description", config_class=ConfigWithStates)
        e1._initial_load_config("tests/tests_unit/dummyconfig.yaml")
        self.assertIsInstance(e1.config, ConfigWithStates)

    @patch("cognite.client.CogniteClient")
    def test_load_state_store(self, get_client_mock):
        e2 = Extractor(name="my_extractor2", description="description", config_class=ConfigWithStates)
        e2._initial_load_config("tests/tests_unit/dummyconfig.yaml")
        e2.cognite_client = get_client_mock()
        e2._load_state_store()
        self.assertIsInstance(e2.state_store, LocalStateStore)

        e3 = Extractor(
            name="my_extractor3",
            description="description",
            config_class=ConfigWithoutStates,
            use_default_state_store=True,
        )
        e3._initial_load_config("tests/tests_unit/dummyconfig.yaml")
        e3.cognite_client = get_client_mock()
        e3._load_state_store()
        self.assertIsInstance(e3.state_store, LocalStateStore)

        e6 = Extractor(
            name="my_extractor6",
            description="description",
            config_class=ConfigWithoutStates,
            use_default_state_store=False,
        )
        e6._initial_load_config("tests/tests_unit/dummyconfig.yaml")
        e6.cognite_client = get_client_mock()
        e6._load_state_store()
        self.assertIsInstance(e6.state_store, NoStateStore)

    @pytest.mark.order(1)
    def test_config_getter(self):
        with self.assertRaises(ValueError):
            Extractor.get_current_config()

        e4 = Extractor(name="my_extractor4", description="description", config_class=ConfigWithStates)

        with self.assertRaises(ValueError):
            Extractor.get_current_config()

        e4._initial_load_config("tests/tests_unit/dummyconfig.yaml")

        self.assertIsInstance(Extractor.get_current_config(), ConfigWithStates)

    @pytest.mark.order(2)
    def test_state_store_getter(self):
        with self.assertRaises(ValueError):
            Extractor.get_current_statestore()

        e5 = Extractor(name="my_extractor5", description="description", config_class=ConfigWithStates)

        with self.assertRaises(ValueError):
            Extractor.get_current_statestore()

        e5._initial_load_config("tests/tests_unit/dummyconfig.yaml")
        e5.cognite_client = e5.config.cognite.get_cognite_client(e5.name)
        e5._load_state_store()

        self.assertIsInstance(Extractor.get_current_statestore(), LocalStateStore)
