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
from dataclasses import dataclass, field
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
    state_store: StateStoreConfig = field(default_factory=StateStoreConfig)


@dataclass
class ConfigWithStates(BaseConfig):
    source: SourceConfig
    extractor: ExtractorConfig = field(default_factory=ExtractorConfig)


@dataclass
class ConfigWithoutStates(BaseConfig):
    source: SourceConfig


def test_load_config():
    e1 = Extractor(name="my_extractor1", description="description", config_class=ConfigWithStates)
    e1._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    assert isinstance(e1.config, ConfigWithStates)


def test_load_config_keyvault():
    e7 = Extractor(name="my_extractor7", description="description", config_class=ConfigWithoutStates)
    e7._initial_load_config("tests/tests_unit/dummyconfig_keyvault.yaml")

    # dummy Azure KeyVault secrets
    assert e7.config.cognite.idp_authentication.client_id == "12345"
    assert e7.config.cognite.idp_authentication.secret == "abcde"


@patch("cognite.client.CogniteClient")
def test_load_state_store(get_client_mock):
    e2 = Extractor(name="my_extractor2", description="description", config_class=ConfigWithStates)
    e2._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    e2.cognite_client = get_client_mock()
    e2._load_state_store()
    assert isinstance(e2.state_store, LocalStateStore)

    e3 = Extractor(
        name="my_extractor3",
        description="description",
        config_class=ConfigWithoutStates,
        use_default_state_store=True,
    )
    e3._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    e3.cognite_client = get_client_mock()
    e3._load_state_store()
    assert isinstance(e3.state_store, LocalStateStore)

    e6 = Extractor(
        name="my_extractor6",
        description="description",
        config_class=ConfigWithoutStates,
        use_default_state_store=False,
    )
    e6._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    e6.cognite_client = get_client_mock()
    e6._load_state_store()
    assert isinstance(e6.state_store, NoStateStore)


@pytest.mark.order(1)
def test_config_getter():
    with pytest.raises(ValueError):
        Extractor.get_current_config()

    e4 = Extractor(name="my_extractor4", description="description", config_class=ConfigWithStates)

    with pytest.raises(ValueError):
        Extractor.get_current_config()

    e4._initial_load_config("tests/tests_unit/dummyconfig.yaml")

    assert isinstance(Extractor.get_current_config(), ConfigWithStates)


@pytest.mark.order(2)
def test_state_store_getter():
    with pytest.raises(ValueError):
        Extractor.get_current_statestore()

    e5 = Extractor(name="my_extractor5", description="description", config_class=ConfigWithStates)

    with pytest.raises(ValueError):
        Extractor.get_current_statestore()

    e5._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    e5.cognite_client = e5.config.cognite.get_cognite_client(e5.name)
    e5._load_state_store()

    assert isinstance(Extractor.get_current_statestore(), LocalStateStore)
