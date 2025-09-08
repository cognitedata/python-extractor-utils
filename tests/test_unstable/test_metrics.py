import time
from types import ModuleType
from unittest.mock import Mock, patch

import arrow
import pytest
from prometheus_client import Gauge

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries
from cognite.client.exceptions import CogniteDuplicatedError, CogniteNotFoundError
from cognite.extractorutils.unstable.core import metrics
from cognite.extractorutils.unstable.core.metrics import CognitePusher, safe_get


@pytest.fixture
def altered_metrics() -> ModuleType:
    altered_metrics = metrics
    altered_metrics.delete_from_gateway = Mock()
    altered_metrics.pushadd_to_gateway = Mock()
    return altered_metrics


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


# For testing CognitePusher
class GaugeSetUp:
    gauge = Gauge("unstable_gauge", "Test gauge")

    @classmethod
    def init_gauge(self) -> None:
        if GaugeSetUp.gauge is None:
            GaugeSetUp.gauge = Gauge("unstable_gauge", "Test gauge")


def init_gauge() -> None:
    GaugeSetUp.init_gauge()


my_class_counter = 0


class MyClass:
    def __init__(self) -> None:
        global my_class_counter
        my_class_counter += 1


class AnotherClass:
    def __init__(self, value: int) -> None:
        global my_class_counter
        my_class_counter += value


def test_prometheus_normal_run(altered_metrics: ModuleType) -> None:
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


@pytest.mark.parametrize("side_effect", [OSError, Exception])
def test_push_to_gateway_with_error(altered_metrics: ModuleType, side_effect: OSError | Exception) -> None:
    altered_metrics.pushadd_to_gateway = Mock(side_effect=side_effect)

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


@patch("cognite.client.CogniteClient")
def test_init_empty_cdf(MockCogniteClient: Mock) -> None:
    init_gauge()
    client = MockCogniteClient()
    client.time_series.retrieve_multiple = Mock(side_effect=CogniteNotFoundError([{"externalId": "pre_gauge"}]))

    return_asset = Asset(id=123, external_id="asset", name="asset")
    new_asset = Asset(external_id="asset", name="asset")

    client.assets.create = Mock(return_value=return_asset)

    pusher = CognitePusher(client, external_id_prefix="pre_", asset=new_asset, push_interval=1)
    # Assert time series created
    client.time_series.create.assert_called_once()
    assert (
        client.time_series.create.call_args_list[0][0][0][0].dump()
        == TimeSeries(
            external_id="pre_gauge", name="gauge", legacy_name="pre_gauge", description="Test gauge", asset_id=123
        ).dump()
    )

    # Assert asset created
    client.assets.create.assert_called_once_with(new_asset)


@patch("cognite.client.CogniteClient")
def test_init_existing_asset(MockCogniteClient: Mock) -> None:
    init_gauge()
    client = MockCogniteClient()
    client.time_series.retrieve_multiple = Mock(side_effect=CogniteNotFoundError([{"externalId": "pre_gauge"}]))

    return_asset = Asset(id=123, external_id="assetid", name="asset")
    new_asset = Asset(external_id="assetid", name="asset")

    client.assets.create = Mock(side_effect=CogniteDuplicatedError(["assetid"]))
    client.assets.retrieve = Mock(return_value=return_asset)

    pusher = CognitePusher(client, external_id_prefix="pre_", asset=new_asset, push_interval=1)
    # Assert time series created
    client.time_series.create.assert_called_once()
    assert (
        client.time_series.create.call_args_list[0][0][0][0].dump()
        == TimeSeries(
            external_id="pre_gauge",
            name="gauge",
            legacy_name="pre_gauge",
            description="Test gauge",
            asset_id=123,
        ).dump()
    )

    # Assert asset created
    client.assets.create.assert_called_once_with(new_asset)
    client.assets.retrieve.assert_called_once_with(external_id="assetid")


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
