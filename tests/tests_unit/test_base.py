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
from collections.abc import Callable
from dataclasses import dataclass, field
from textwrap import shorten
from unittest.mock import Mock, patch

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import ExtractionPipeline, ExtractionPipelineRun
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


def test_load_config() -> None:
    e1 = Extractor(name="my_extractor1", description="description", config_class=ConfigWithStates)
    e1._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    assert isinstance(e1.config, ConfigWithStates)


def test_load_config_keyvault() -> None:
    e7 = Extractor(name="my_extractor7", description="description", config_class=ConfigWithoutStates)
    e7._initial_load_config("tests/tests_unit/dummyconfig_keyvault.yaml")

    # dummy Azure KeyVault secrets
    assert e7.config.cognite.idp_authentication.client_id == "12345"
    assert e7.config.cognite.idp_authentication.secret == "abcde"


@patch("cognite.client.CogniteClient")
def test_load_state_store(get_client_mock: Callable[[], CogniteClient]) -> None:
    e2 = Extractor(name="my_extractor2", description="description", config_class=ConfigWithStates)
    e2._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    e2.cognite_client = get_client_mock()
    e2._load_state_store()
    assert isinstance(e2.state_store, LocalStateStore)

    # Make sure the state store have been given a child token
    assert e2.state_store.cancellation_token._parent is e2.cancellation_token

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

    # Make sure the state store have been given a child token
    assert e3.state_store.cancellation_token._parent is e3.cancellation_token

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
def test_config_getter() -> None:
    with pytest.raises(ValueError):
        Extractor.get_current_config()

    e4 = Extractor(name="my_extractor4", description="description", config_class=ConfigWithStates)

    with pytest.raises(ValueError):
        Extractor.get_current_config()

    e4._initial_load_config("tests/tests_unit/dummyconfig.yaml")

    assert isinstance(Extractor.get_current_config(), ConfigWithStates)


@pytest.mark.order(2)
def test_state_store_getter() -> None:
    with pytest.raises(ValueError):
        Extractor.get_current_statestore()

    e5 = Extractor(name="my_extractor5", description="description", config_class=ConfigWithStates)

    with pytest.raises(ValueError):
        Extractor.get_current_statestore()

    e5._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    e5.cognite_client = e5.config.cognite.get_cognite_client(e5.name)
    e5._load_state_store()

    assert isinstance(Extractor.get_current_statestore(), LocalStateStore)


@patch("cognite.client.CogniteClient")
def test_report_success(
    get_client_mock: Callable[[], CogniteClient],
) -> None:
    print("Report success test")

    EXTRACTION_PIPELINE = "test_extraction_pipeline"
    MESSAGE = "test message"

    def validate_message(run: ExtractionPipelineRun):
        print(f"Validating message: {run.message}")
        assert run.extpipe_external_id == EXTRACTION_PIPELINE
        assert run.status == "success"
        assert run.message == MESSAGE, "Message does not match expected value"

    extractor = Extractor(
        name="extractor_test_report_success",
        description="description",
        config_class=ConfigWithoutStates,
    )
    extractor._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    extractor.cognite_client = get_client_mock()
    extractor.logger = logging.getLogger("test_logger")

    extractor.extraction_pipeline = ExtractionPipeline(external_id=EXTRACTION_PIPELINE, name=EXTRACTION_PIPELINE)
    extractor.cognite_client.extraction_pipelines.runs.create = Mock(side_effect=validate_message)

    extractor._report_success(message=MESSAGE)
    extractor.extraction_pipeline = None


@patch("cognite.client.CogniteClient")
def test_report_failure(
    get_client_mock: Callable[[], CogniteClient],
) -> None:
    print("Report success test")

    EXTRACTION_PIPELINE = "test_extraction_pipeline"
    MESSAGE = "test message"

    def validate_message(run: ExtractionPipelineRun):
        print(f"Validating message: {run.message}")
        assert run.extpipe_external_id == EXTRACTION_PIPELINE
        assert run.status == "failure"
        assert run.message == MESSAGE, "Message does not match expected value"

    extractor = Extractor(
        name="extractor_test_report_failure",
        description="description",
        config_class=ConfigWithoutStates,
    )
    extractor._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    extractor.cognite_client = get_client_mock()
    extractor.logger = logging.getLogger("test_logger")

    extractor.extraction_pipeline = ExtractionPipeline(external_id=EXTRACTION_PIPELINE, name=EXTRACTION_PIPELINE)
    extractor.cognite_client.extraction_pipelines.runs.create = Mock(side_effect=validate_message)

    extractor._report_failure(message=MESSAGE)
    extractor.extraction_pipeline = None


@patch("cognite.client.CogniteClient")
def test_report_error(
    get_client_mock: Callable[[], CogniteClient],
) -> None:
    print("Report error test")

    EXTRACTION_PIPELINE = "test_extraction_pipeline"
    MESSAGE = "test exception"
    expected_message = f"Exception: {MESSAGE}"

    def validate_message(run: ExtractionPipelineRun):
        print(f"Validating message: {run.message}")
        assert run.extpipe_external_id == EXTRACTION_PIPELINE
        assert run.status == "failure"
        assert run.message == expected_message, "Message does not match expected value"

    extractor = Extractor(
        name="extractor_test_report_error",
        description="description",
        config_class=ConfigWithoutStates,
    )
    extractor._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    extractor.cognite_client = get_client_mock()
    extractor.logger = logging.getLogger("test_logger")

    extractor.extraction_pipeline = ExtractionPipeline(external_id=EXTRACTION_PIPELINE, name=EXTRACTION_PIPELINE)
    extractor.cognite_client.extraction_pipelines.runs.create = Mock(side_effect=validate_message)

    exception = Exception(MESSAGE)

    extractor._report_error(exception=exception)
    extractor.extraction_pipeline = None


@patch("cognite.client.CogniteClient")
def test_report_run(get_client_mock: Callable[[], CogniteClient]):
    print("Report run test")

    MAX_MESSAGE_LENGTH_FOR_EXTRACTION_PIPELINE_RUN = 1000

    EXTRACTION_PIPELINE = "test_extraction_pipeline"
    SHORT_MESSAGE = "hello world"
    LONG_MESSAGE = "x " * 1500

    expected_long_message = shorten(
        text=LONG_MESSAGE,
        width=MAX_MESSAGE_LENGTH_FOR_EXTRACTION_PIPELINE_RUN,
        placeholder="...",
    )

    # Mock method for reporting run
    def validate_short_message(run: ExtractionPipelineRun):
        print(f"Validating short message: {run.message}")
        assert run.extpipe_external_id == EXTRACTION_PIPELINE
        assert len(run.message) <= MAX_MESSAGE_LENGTH_FOR_EXTRACTION_PIPELINE_RUN, (
            f"Short message length exceeds maximum allowed length: {MAX_MESSAGE_LENGTH_FOR_EXTRACTION_PIPELINE_RUN}"
        )
        assert run.message == SHORT_MESSAGE, "Short message does not match expected value"

    def validate_long_message(run: ExtractionPipelineRun):
        print(f"Validating long message: {run.message}")
        assert len(run.message) <= MAX_MESSAGE_LENGTH_FOR_EXTRACTION_PIPELINE_RUN, (
            f"Long message length exceeds maximum allowed length: {MAX_MESSAGE_LENGTH_FOR_EXTRACTION_PIPELINE_RUN}"
        )
        assert run.message == expected_long_message, "Long message does not match expected value"

    extractor = Extractor(
        name="extractor_test_report_run",
        description="description",
        config_class=ConfigWithoutStates,
    )
    extractor._initial_load_config("tests/tests_unit/dummyconfig.yaml")
    extractor.cognite_client = get_client_mock()
    extractor.logger = logging.getLogger("test_logger")

    extractor.extraction_pipeline = ExtractionPipeline(external_id=EXTRACTION_PIPELINE, name=EXTRACTION_PIPELINE)
    extractor.cognite_client.extraction_pipelines.runs.create = Mock(side_effect=validate_short_message)
    extractor._report_run(status="success", message=SHORT_MESSAGE)

    extractor.cognite_client.extraction_pipelines.runs.create = Mock(side_effect=validate_long_message)
    extractor._report_run(status="success", message=LONG_MESSAGE)

    extractor.extraction_pipeline = None
    # assert False
