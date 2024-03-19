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

"""
Module containing tools for pushers for metric reporting.

The classes in this module scrape the default Prometheus registry and uploads it periodically to either a Prometheus
push gateway, or to CDF as time series.

The ``BaseMetrics`` class forms the basis for a metrics collection for an extractor, containing some general metrics
that all extractors should report. To create your own set of metrics, subclass this class and populate it with
extractor-specific metrics, as such:

.. code-block:: python

    class MyMetrics(BaseMetrics):
        def __init__(self):
            super().__init__(extractor_name="my_extractor", extractor_version=__version__)

            self.a_counter = Counter("my_extractor_example_counter", "An example counter")
            ...

The metrics module also contains some Pusher classes that are used to routinely send metrics to a
remote server, these can be automatically created with the ``start_pushers`` method described in
``configtools``.

"""

import logging
import os
import threading
from abc import ABC, abstractmethod
from time import sleep
from types import TracebackType
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, TypeVar, Union

import arrow
import psutil
from prometheus_client import Gauge, Info, Metric
from prometheus_client.core import REGISTRY
from prometheus_client.exposition import basic_auth_handler, delete_from_gateway, pushadd_to_gateway

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, Datapoints, DatapointsArray, TimeSeries
from cognite.client.exceptions import CogniteDuplicatedError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.util import EitherId

from .util import ensure_time_series

_metrics_singularities = {}


T = TypeVar("T")


def safe_get(cls: Type[T], *args: Any, **kwargs: Any) -> T:
    """
    A factory for instances of metrics collections.

    Since Prometheus doesn't allow multiple metrics with the same name, any subclass of BaseMetrics must never be
    created more than once. This function creates an instance of the given class on the first call and stores it, any
    subsequent calls with the same class as argument will return the same instance.

    .. code-block:: python

        >>> a = safe_get(MyMetrics)  # This will create a new instance of MyMetrics
        >>> b = safe_get(MyMetrics)  # This will return the same instance
        >>> a is b
        True


    Args:
        cls: Metrics class to either create or get a cached version of

    Returns:
        An instance of given class
    """
    global _metrics_singularities

    if cls not in _metrics_singularities:
        _metrics_singularities[cls] = cls(*args, **kwargs)

    return _metrics_singularities[cls]


class BaseMetrics:
    """
    Base collection of extractor metrics. The class also spawns a collector thread on init that regularly fetches
    process information and update the ``process_*`` gauges.

    To create a set of metrics for an extractor, create a subclass of this class.

    **Note that only one instance of this class (or any subclass) can exist simultaneously**

    The collection includes the following metrics:
     * startup:                     Startup time (unix epoch)
     * finish:                      Finish time (unix epoch)
     * process_num_threads          Number of active threads. Set automatically.
     * process_memory_bytes         Memory usage of extractor. Set automatically.
     * process_cpu_percent          CPU usage of extractor. Set automatically.

    Args:
        extractor_name: Name of extractor, used to prefix metric names
        process_scrape_interval: Interval (in seconds) between each fetch of data for the ``process_*`` gauges
    """

    def __init__(self, extractor_name: str, extractor_version: str, process_scrape_interval: float = 15):
        extractor_name = extractor_name.strip().replace(" ", "_")

        self.startup = Gauge(f"{extractor_name}_start_time", "Timestamp (seconds) of when the extractor last started")
        self.finish = Gauge(
            f"{extractor_name}_finish_time", "Timestamp (seconds) of then the extractor last finished cleanly"
        )

        self._process = psutil.Process(os.getpid())

        self.process_num_threads = Gauge(f"{extractor_name}_num_threads", "Number of threads")
        self.process_memory_bytes = Gauge(f"{extractor_name}_memory_bytes", "Memory usage in bytes")
        self.process_memory_bytes_available = Gauge(
            f"{extractor_name}_memory_bytes_available", "Memory available in bytes"
        )
        self.process_cpu_percent = Gauge(f"{extractor_name}_cpu_percent", "CPU usage percent")

        self.info = Info(f"{extractor_name}_info", "Information about running extractor")
        self.info.info({"extractor_version": extractor_version, "extractor_type": extractor_name})

        self.process_scrape_interval = process_scrape_interval
        self._start_proc_collector()

        self.startup.set_to_current_time()

    def _proc_collect(self) -> None:
        """
        Collect values for process metrics
        """
        total_memory_available = psutil.virtual_memory().total
        while True:
            self.process_num_threads.set(self._process.num_threads())
            self.process_memory_bytes.set(self._process.memory_info().rss)
            self.process_memory_bytes_available.set(total_memory_available)
            self.process_cpu_percent.set(self._process.cpu_percent())

            sleep(self.process_scrape_interval)

    def _start_proc_collector(self) -> None:
        """
        Start a thread that collects process metrics at a regular interval
        """
        thread = threading.Thread(target=self._proc_collect, name="ProcessMetricsCollector", daemon=True)
        thread.start()


class AbstractMetricsPusher(ABC):
    """
    Base class for metric pushers. Metric pushers spawns a thread that routinely pushes metrics to a configured
    destination.

    Contains all the logic for starting and running threads.

    Args:
        push_interval: Seconds between each upload call
        thread_name: Name of thread to start. If omitted, a standard name such as Thread-4 will be generated.
        cancellation_token: Event object to be used as a thread cancelation event
    """

    def __init__(
        self,
        push_interval: Optional[int] = None,
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ):
        self.push_interval = push_interval
        self.thread_name = thread_name

        self.thread: Optional[threading.Thread] = None
        self.thread_name = thread_name
        self.cancellation_token = cancellation_token.create_child_token() if cancellation_token else CancellationToken()

        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def _push_to_server(self) -> None:
        """
        Push metrics to a remote server, to be overrided in subclasses.
        """
        pass

    def _run(self) -> None:
        """
        Run push loop.
        """
        while not self.cancellation_token.is_cancelled:
            self._push_to_server()
            self.cancellation_token.wait(self.push_interval)

    def start(self) -> None:
        """
        Starts a thread that pushes the default registry to the configured gateway at certain intervals.

        """
        self.thread = threading.Thread(target=self._run, daemon=True, name=self.thread_name)
        self.thread.start()

    def stop(self) -> None:
        """
        Stop the push loop.
        """
        # Make sure everything is pushed
        self._push_to_server()
        self.cancellation_token.cancel()

    def __enter__(self) -> "AbstractMetricsPusher":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        """
        Wraps around stop method, for use as context manager

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()


class PrometheusPusher(AbstractMetricsPusher):
    """
    Pusher to a Prometheus push gateway.

    Args:
        job_name: Prometheus job name
        username: Push gateway credentials
        password: Push gateway credentials
        url: URL (with portnum) of push gateway
        push_interval: Seconds between each upload call
        thread_name: Name of thread to start. If omitted, a standard name such as Thread-4 will be generated.
        cancellation_token: Event object to be used as a thread cancelation event
    """

    def __init__(
        self,
        job_name: str,
        url: str,
        push_interval: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ):
        super(PrometheusPusher, self).__init__(push_interval, thread_name, cancellation_token)

        self.username = username
        self.job_name = job_name
        self.password = password

        self.url = url

    def _auth_handler(self, url: str, method: str, timeout: int, headers: List[Tuple[str, str]], data: Any) -> Callable:
        """
        Returns a authentication handler against the Prometheus Pushgateway to use in the pushadd_to_gateway method.

        Args:
            url:      Push gateway
            method:   HTTP method
            timeout:  Request timeout (seconds)
            headers:  HTTP headers
            data:     Data to send

        Returns:
            prometheus_client.exposition.basic_auth_handler: A authentication handler based on this client.
        """
        return basic_auth_handler(url, method, timeout, headers, data, self.username, self.password)

    def _push_to_server(self) -> None:
        """
        Push the default metrics registry to the configured Prometheus Pushgateway.
        """
        if not self.url or not self.job_name:
            return

        try:
            pushadd_to_gateway(self.url, job=self.job_name, registry=REGISTRY, handler=self._auth_handler)

        except OSError as exp:
            self.logger.warning("Failed to push metrics to %s: %s", self.url, str(exp))
        except Exception:
            self.logger.exception("Failed to push metrics to %s", self.url)

        self.logger.debug("Pushed metrics to %s", self.url)

    def clear_gateway(self) -> None:
        """
        Delete metrics stored at the gateway (reset gateway).
        """
        delete_from_gateway(self.url, job=self.job_name, handler=self._auth_handler)
        self.logger.debug("Deleted metrics from push gateway %s", self.url)


class CognitePusher(AbstractMetricsPusher):
    """
    Pusher to CDF. Creates time series in CDF for all Gauges and Counters in the default Prometheus registry.

    Optional contextualization with an Asset to make the time series observable in Asset Data Insight. The given asset
    will be created at root level in the tenant if it doesn't already exist.

    Args:
        cdf_client: The CDF tenant to upload time series to
        external_id_prefix: Unique external ID prefix for this pusher.
        push_interval: Seconds between each upload call
        asset: Optional contextualization.
        data_set: Data set the metrics timeseries created under.
        thread_name: Name of thread to start. If omitted, a standard name such as Thread-4 will be generated.
        cancellation_token: Event object to be used as a thread cancelation event
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        external_id_prefix: str,
        push_interval: int,
        asset: Optional[Asset] = None,
        data_set: Optional[EitherId] = None,
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ):
        super(CognitePusher, self).__init__(push_interval, thread_name, cancellation_token)

        self.cdf_client = cdf_client
        self.asset = asset
        self.external_id_prefix = external_id_prefix
        self.data_set = data_set

        self._init_cdf()

        self._cdf_project = cdf_client.config.project

    def _init_cdf(self) -> None:
        """
        Initialize the CDF tenant with the necessary time series and asset.
        """
        time_series: List[TimeSeries] = []

        if self.asset is not None:
            # Ensure that asset exist, and retrieve internal ID
            asset: Optional[Asset]
            try:
                asset = self.cdf_client.assets.create(self.asset)
            except CogniteDuplicatedError:
                asset = self.cdf_client.assets.retrieve(external_id=self.asset.external_id)

            asset_id = asset.id if asset is not None else None

        else:
            asset_id = None

        data_set_id = None
        if self.data_set:
            dataset = self.cdf_client.data_sets.retrieve(
                id=self.data_set.internal_id, external_id=self.data_set.external_id
            )
            if dataset:
                data_set_id = dataset.id

        for metric in REGISTRY.collect():
            if type(metric) is Metric and metric.type in ["gauge", "counter"]:
                external_id = self.external_id_prefix + metric.name

                time_series.append(
                    TimeSeries(
                        external_id=external_id,
                        name=metric.name,
                        legacy_name=external_id,
                        description=metric.documentation,
                        asset_id=asset_id,
                        data_set_id=data_set_id,
                    )
                )

        ensure_time_series(self.cdf_client, time_series)

    def _push_to_server(self) -> None:
        """
        Create datapoints an push them to their respective time series
        """
        timestamp = int(arrow.get().float_timestamp * 1000)

        datapoints: List[Dict[str, Union[str, int, List[Any], Datapoints, DatapointsArray]]] = []

        for metric in REGISTRY.collect():
            if type(metric) == Metric and metric.type in ["gauge", "counter"]:
                if len(metric.samples) == 0:
                    continue

                external_id = self.external_id_prefix + metric.name
                datapoints.append({"externalId": external_id, "datapoints": [(timestamp, metric.samples[0].value)]})

        self.cdf_client.time_series.data.insert_multiple(datapoints)
        self.logger.debug("Pushed metrics to CDF tenant '%s'", self._cdf_project)
