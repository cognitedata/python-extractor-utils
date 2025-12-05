#  Copyright 2020 Cognite AS
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

import time
from types import ModuleType
from unittest.mock import Mock, patch

import arrow
import pytest
from prometheus_client import Gauge

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset
from cognite.client.exceptions import CogniteDuplicatedError, CogniteNotFoundError
from cognite.extractorutils import metrics
from cognite.extractorutils.metrics import CognitePusher, safe_get


# For testing PrometheusPusher
@pytest.fixture
def altered_metrics() -> ModuleType:
    altered_metrics = metrics
    altered_metrics.delete_from_gateway = Mock()
    altered_metrics.pushadd_to_gateway = Mock()
    return altered_metrics


# For testing CognitePusher
class GaugeSetUp:
    gauge = Gauge("gauge", "Test gauge")

    @classmethod
    def init_gauge(self) -> None:
        if GaugeSetUp.gauge is None:
            GaugeSetUp.gauge = Gauge("gauge", "Test gauge")


def init_gauge() -> None:
    GaugeSetUp.init_gauge()


# For testing the metrics utils
my_class_counter = 0


class MyClass:
    def __init__(self) -> None:
        global my_class_counter
        my_class_counter += 1


class AnotherClass:
    def __init__(self, value: int) -> None:
        global my_class_counter
        my_class_counter += value


# PrometheusPusher test
def test_normal_run(altered_metrics: ModuleType) -> None:
    prom = altered_metrics.PrometheusPusher(job_name="test-job", url="none", push_interval=1)

    last = altered_metrics.pushadd_to_gateway.call_count
    prom.start()
    time.sleep(0.1)
    assert altered_metrics.pushadd_to_gateway.call_count >= last + 1
    last = altered_metrics.pushadd_to_gateway.call_count
    time.sleep(1.1)
    assert altered_metrics.pushadd_to_gateway.call_count >= last + 1
    last = altered_metrics.pushadd_to_gateway.call_count
    prom.stop()
    assert altered_metrics.pushadd_to_gateway.call_count >= last + 1
    last = altered_metrics.pushadd_to_gateway.call_count
    time.sleep(1.1)
    assert altered_metrics.pushadd_to_gateway.call_count >= last


def test_error_doesnt_stop1(altered_metrics: ModuleType) -> None:
    altered_metrics.pushadd_to_gateway = Mock(side_effect=OSError)

    prom = altered_metrics.PrometheusPusher(job_name="test-job", url="none", push_interval=1)

    prom.start()
    time.sleep(0.1)
    assert altered_metrics.pushadd_to_gateway.call_count == 1
    time.sleep(1.1)
    assert altered_metrics.pushadd_to_gateway.call_count == 2
    prom.stop()


def test_error_doesnt_stop2(altered_metrics: ModuleType) -> None:
    altered_metrics.pushadd_to_gateway = Mock(side_effect=Exception)

    prom = altered_metrics.PrometheusPusher(job_name="test-job", url="none", push_interval=1)

    prom.start()
    time.sleep(0.1)
    assert altered_metrics.pushadd_to_gateway.call_count == 1
    time.sleep(1.1)
    assert altered_metrics.pushadd_to_gateway.call_count == 2
    prom.stop()


def test_clear(altered_metrics: ModuleType) -> None:
    prom = altered_metrics.PrometheusPusher(job_name="test-job", url="none", push_interval=1)
    prom.clear_gateway()

    altered_metrics.delete_from_gateway.assert_called()


# CognitePusher test
@patch("cognite.client.CogniteClient")
def test_init_empty_cdf(MockCogniteClient: Mock) -> None:
    """Test that initialization sets up asset but doesn't create timeseries (created on-demand)."""
    init_gauge()
    client = MockCogniteClient()

    return_asset = Asset(id=123, external_id="asset", name="asset")
    new_asset = Asset(external_id="asset", name="asset")

    client.assets.create = Mock(return_value=return_asset)

    pusher = CognitePusher(client, external_id_prefix="pre_", asset=new_asset, push_interval=1)

    # Assert asset created and asset_id was set
    client.assets.create.assert_called_once_with(new_asset)
    assert pusher._asset_id == 123


@patch("cognite.client.CogniteClient")
def test_init_existing_asset(MockCogniteClient: Mock) -> None:
    """Test that initialization retrieves existing asset."""
    init_gauge()
    client = MockCogniteClient()

    return_asset = Asset(id=123, external_id="assetid", name="asset")
    new_asset = Asset(external_id="assetid", name="asset")

    client.assets.create = Mock(side_effect=CogniteDuplicatedError(["assetid"]))
    client.assets.retrieve = Mock(return_value=return_asset)

    pusher = CognitePusher(client, external_id_prefix="pre_", asset=new_asset, push_interval=1)

    # Assert asset retrieved
    client.assets.create.assert_called_once_with(new_asset)
    client.assets.retrieve.assert_called_once_with(external_id="assetid")
    assert pusher._asset_id == 123


@patch("cognite.client.CogniteClient")
def test_init_existing_all(MockCogniteClient: Mock) -> None:
    init_gauge()
    client = MockCogniteClient()
    return_asset = Asset(id=123, external_id="assetid", name="asset")
    new_asset = Asset(external_id="assetid", name="asset")

    client.assets.create = Mock(side_effect=CogniteDuplicatedError(["assetid"]))
    client.assets.retrieve = Mock(return_value=return_asset)

    pusher = CognitePusher(client, external_id_prefix="pre_", asset=new_asset, push_interval=1)

    # Assert time series created
    client.time_series.create.assert_not_called()

    # Assert asset created
    client.assets.create.assert_called_once_with(new_asset)
    client.assets.retrieve.assert_called_once_with(external_id="assetid")


@patch("cognite.client.CogniteClient")
def test_push(MockCogniteClient: Mock) -> None:
    init_gauge()
    client: CogniteClient = MockCogniteClient()
    pusher = CognitePusher(client, "pre_", push_interval=1)

    GaugeSetUp.gauge.set(5)
    pusher._push_to_server()

    client.time_series.data.insert_multiple.assert_called_once()
    for time_series in client.time_series.data.insert_multiple.call_args_list[0][0][0]:
        if time_series["externalId"] == "pre_gauge":
            ts = time_series
            break

    timestamp = int(arrow.get().float_timestamp * 1000)
    assert ts is not None
    assert abs(timestamp - ts["datapoints"][0][0]) < 100  # less than 100 ms
    assert ts["datapoints"][0][1] == pytest.approx(5)


@patch("cognite.client.CogniteClient")
def test_push_creates_missing_timeseries(MockCogniteClient: Mock) -> None:
    """Test that push logic creates missing time series when enabled."""
    init_gauge()
    client: CogniteClient = MockCogniteClient()

    # Create a mock CogniteNotFoundError with not_found and failed attributes
    not_found_error = CogniteNotFoundError([{"externalId": "pre_gauge"}])
    not_found_error.not_found = [{"externalId": "pre_gauge"}]
    not_found_error.failed = []

    # Simulate CogniteNotFoundError on first push, then success on retry
    client.time_series.data.insert_multiple.side_effect = [
        not_found_error,
        None,  # Success on retry
    ]

    pusher = CognitePusher(client, "pre_", push_interval=1)

    GaugeSetUp.gauge.set(5)
    pusher._push_to_server()

    # Assert that we tried to create the timeseries
    client.time_series.create.assert_called_once()
    created_ts_list = client.time_series.create.call_args[0][0]
    assert len(created_ts_list) == 1
    assert created_ts_list[0].external_id == "pre_gauge"
    assert created_ts_list[0].name == "gauge"
    assert created_ts_list[0].description == "Test gauge"

    # Assert that insert_multiple was called twice (initial attempt + retry)
    assert client.time_series.data.insert_multiple.call_count == 2


# MetricsUtils test
@pytest.fixture
def init_counter() -> None:
    global my_class_counter
    my_class_counter = 0


def test_safe_get(init_counter: None) -> None:
    assert my_class_counter == 0

    a = safe_get(MyClass)
    assert my_class_counter == 1
    assert isinstance(a, MyClass)

    b = safe_get(MyClass)
    assert my_class_counter == 1
    assert isinstance(b, MyClass)
    assert a == b

    c = safe_get(AnotherClass, 5)
    assert my_class_counter == 6
    assert isinstance(c, AnotherClass)
