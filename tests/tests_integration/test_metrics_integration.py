"""
Integration tests for CognitePusher with late-registered metrics.

This test verifies that CognitePusher correctly handles metrics that are registered
after initialization (like python_gc_* and python_info metrics from Prometheus).
"""

import logging
import random
import time
from collections.abc import Callable, Generator
from typing import Any

import pytest
from prometheus_client import Counter, Gauge
from prometheus_client.core import REGISTRY

from cognite.client import CogniteClient
from cognite.extractorutils.metrics import CognitePusher

logger = logging.getLogger(__name__)


@pytest.fixture
def test_prefix() -> str:
    """Generate a unique prefix for this test run to avoid conflicts."""
    test_id = random.randint(0, 2**31)
    return f"integration_test_{test_id}_"


@pytest.fixture
def metrics_registry() -> Generator[Callable[[Any], Any], None, None]:
    """
    Fixture that tracks and cleans up Prometheus metrics.

    Ensures metrics are unregistered even if the test fails.
    """
    metrics_to_unregister: list[Any] = []

    def _register(metric: Any) -> Any:
        metrics_to_unregister.append(metric)
        return metric

    yield _register

    for metric in metrics_to_unregister:
        try:
            REGISTRY.unregister(metric)
        except KeyError:
            logger.debug("Metric %s was already unregistered", metric)


@pytest.fixture
def cognite_pusher_test(
    set_client: CogniteClient, test_prefix: str
) -> Generator[tuple[CogniteClient, str, list[str]], None, None]:
    """
    Fixture that sets up and tears down a CognitePusher test.

    Yields:
        Tuple of (client, test_prefix, list of created timeseries external_ids)
    """
    client = set_client
    created_external_ids: list[str] = []

    yield client, test_prefix, created_external_ids

    if created_external_ids:
        try:
            client.time_series.delete(external_id=created_external_ids, ignore_unknown_ids=True)
        except Exception as e:
            logger.warning("Failed to cleanup timeseries: %s", e)


def test_cognite_pusher_with_late_registered_metrics(
    cognite_pusher_test: tuple[CogniteClient, str, list[str]],
    metrics_registry: Callable[[Any], Any],
) -> None:
    """
    Test that CognitePusher handles both early and late-registered metrics.

    This simulates the real-world scenario where:
    1. Some metrics (like extractor-specific metrics) are registered before CognitePusher init
    2. Other metrics (like python_gc_*, python_info) are registered after initialization
    3. All metrics should be uploaded correctly during push
    """
    client, test_prefix, created_external_ids = cognite_pusher_test

    early_gauge_name = f"early_gauge_{random.randint(0, 2**31)}"
    early_gauge = metrics_registry(Gauge(early_gauge_name, "A metric registered before CognitePusher init"))
    early_gauge.set(42.0)

    early_external_id = test_prefix + early_gauge_name
    created_external_ids.append(early_external_id)

    pusher = CognitePusher(
        cdf_client=client,
        external_id_prefix=test_prefix,
        push_interval=60,
    )

    late_gauge_name = f"late_gauge_{random.randint(0, 2**31)}"
    late_gauge = metrics_registry(
        Gauge(late_gauge_name, "A metric registered AFTER CognitePusher init (like python_gc)")
    )
    late_gauge.set(99.0)

    late_counter_name = f"late_counter_{random.randint(0, 2**31)}"
    late_counter = metrics_registry(
        Counter(late_counter_name, "A counter registered AFTER CognitePusher init (like python_info)")
    )
    late_counter.inc(5)

    late_gauge_external_id = test_prefix + late_gauge_name
    late_counter_external_id = test_prefix + late_counter_name
    created_external_ids.append(late_gauge_external_id)
    created_external_ids.append(late_counter_external_id)

    # This should create timeseries for ALL metrics (early + late)
    pusher._push_to_server()

    time.sleep(2)

    early_ts = client.time_series.retrieve(external_id=early_external_id)
    assert early_ts is not None, f"Early metric timeseries {early_external_id} was not created"
    assert early_ts.name == early_gauge_name
    assert early_ts.description == "A metric registered before CognitePusher init"

    late_gauge_ts = client.time_series.retrieve(external_id=late_gauge_external_id)
    assert late_gauge_ts is not None, f"Late gauge timeseries {late_gauge_external_id} was not created"
    assert late_gauge_ts.name == late_gauge_name
    assert late_gauge_ts.description == "A metric registered AFTER CognitePusher init (like python_gc)"

    late_counter_ts = client.time_series.retrieve(external_id=late_counter_external_id)
    assert late_counter_ts is not None, f"Late counter timeseries {late_counter_external_id} was not created"
    assert late_counter_ts.name == late_counter_name

    early_datapoints = client.time_series.data.retrieve(
        external_id=early_external_id, start="1h-ago", end="now", limit=10
    )
    assert len(early_datapoints) > 0, "No datapoints for early metric"
    assert early_datapoints.value[0] == pytest.approx(42.0)

    late_gauge_datapoints = client.time_series.data.retrieve(
        external_id=late_gauge_external_id, start="1h-ago", end="now", limit=10
    )
    assert len(late_gauge_datapoints) > 0, "No datapoints for late gauge metric"
    assert late_gauge_datapoints.value[0] == pytest.approx(99.0)

    late_counter_datapoints = client.time_series.data.retrieve(
        external_id=late_counter_external_id, start="1h-ago", end="now", limit=10
    )
    assert len(late_counter_datapoints) > 0, "No datapoints for late counter metric"
    assert late_counter_datapoints.value[0] == pytest.approx(5.0)

    pusher.stop()


def test_cognite_pusher_stop_uploads_late_metrics(
    cognite_pusher_test: tuple[CogniteClient, str, list[str]],
    metrics_registry: Callable[[Any], Any],
) -> None:
    """
    Test that stop() correctly uploads all metrics including late-registered ones.

    This is the scenario where:
    1. CognitePusher is initialized
    2. Metrics are registered after
    3. stop() is called during shutdown
    4. All metrics (including late ones) should be uploaded
    """
    client, test_prefix, created_external_ids = cognite_pusher_test

    pusher = CognitePusher(
        cdf_client=client,
        external_id_prefix=test_prefix,
        push_interval=60,
    )

    late_metric_name = f"shutdown_metric_{random.randint(0, 2**31)}"
    late_metric = metrics_registry(Gauge(late_metric_name, "A metric registered after init, uploaded during shutdown"))
    late_metric.set(123.0)

    late_external_id = test_prefix + late_metric_name
    created_external_ids.append(late_external_id)

    pusher.stop()

    time.sleep(2)

    late_ts = client.time_series.retrieve(external_id=late_external_id)
    assert late_ts is not None, f"Late metric {late_external_id} was not created during shutdown"

    late_datapoints = client.time_series.data.retrieve(
        external_id=late_external_id, start="1h-ago", end="now", limit=10
    )
    assert len(late_datapoints) > 0, "No datapoints for late metric after shutdown"
    assert late_datapoints.value[0] == pytest.approx(123.0)


def test_cognite_pusher_multiple_pushes_with_late_metrics(
    cognite_pusher_test: tuple[CogniteClient, str, list[str]],
    metrics_registry: Callable[[Any], Any],
) -> None:
    """
    Test that multiple pushes work correctly with late-registered metrics.

    Scenario:
    1. Push with some metrics
    2. Register new metrics
    3. Push again - new metrics should be created and uploaded
    """
    client, test_prefix, created_external_ids = cognite_pusher_test

    initial_metric_name = f"initial_{random.randint(0, 2**31)}"
    initial_metric = metrics_registry(Gauge(initial_metric_name, "Initial metric"))
    initial_metric.set(10.0)

    initial_external_id = test_prefix + initial_metric_name
    created_external_ids.append(initial_external_id)

    pusher = CognitePusher(
        cdf_client=client,
        external_id_prefix=test_prefix,
        push_interval=60,
    )

    pusher._push_to_server()
    time.sleep(1)

    initial_ts = client.time_series.retrieve(external_id=initial_external_id)
    assert initial_ts is not None

    late_metric_name = f"later_{random.randint(0, 2**31)}"
    late_metric = metrics_registry(Gauge(late_metric_name, "Late metric added between pushes"))
    late_metric.set(20.0)

    late_external_id = test_prefix + late_metric_name
    created_external_ids.append(late_external_id)

    initial_metric.set(11.0)
    pusher._push_to_server()
    time.sleep(2)

    late_ts = client.time_series.retrieve(external_id=late_external_id)
    assert late_ts is not None, "Late metric was not created on second push"

    late_datapoints = client.time_series.data.retrieve(
        external_id=late_external_id, start="1h-ago", end="now", limit=10
    )
    assert len(late_datapoints) > 0
    assert late_datapoints.value[0] == pytest.approx(20.0)

    pusher.stop()
