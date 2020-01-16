"""
Module containing tools for pushers for metric reporting.

The classes in this module scrape the default Prometheus registry and uploads it periodically to either a Prometheus
push gateway, or to CDF as time series.
"""
import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import arrow
from prometheus_client import Metric
from prometheus_client.core import REGISTRY
from prometheus_client.exposition import basic_auth_handler, delete_from_gateway, pushadd_to_gateway

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries
from cognite.client.exceptions import CogniteDuplicatedError
from cognite.extractorutils.util import ensure_time_series


class AbstractMetricsPusher(ABC):
    """
    Base class for metric pushers. Metric pushers spawns a thread that routinely pushes metrics to a configured
    destination.

    Contains all the logic for starting and running threads.

    Args:
        push_interval: Seconds between each upload call
        thread_name: Name of thread to start. If omitted, a standard name such as Thread-4 will be generated.
    """

    def __init__(self, push_interval: Optional[int] = None, thread_name: Optional[str] = None):
        self.push_interval = push_interval
        self.thread_name = thread_name

        self.thread: Optional[threading.Thread] = None
        self.thread_name = thread_name
        self.stopping = threading.Event()

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
        while not self.stopping.is_set():
            self._push_to_server()
            self.stopping.wait(self.push_interval)

    def start(self) -> None:
        """
        Starts a thread that pushes the default registry to the configured gateway at certain intervals.
        """
        self.stopping.clear()
        self.thread = threading.Thread(target=self._run, daemon=True, name=self.thread_name)
        self.thread.start()

    def stop(self) -> None:
        """
        Stop the push loop.
        """
        # Make sure everything is pushed
        self._push_to_server()
        self.stopping.set()

    def __enter__(self) -> "AbstractMetricsPusher":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
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
    """

    def __init__(
        self,
        job_name: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        url: Optional[str] = None,
        push_interval: Optional[int] = None,
        thread_name: Optional[str] = None,
    ):
        super(PrometheusPusher, self).__init__(push_interval, thread_name)

        self.username = username
        self.job_name = job_name
        self.password = password

        self.url = url
        self.push_interval = push_interval

    def configure(self, config: Dict[str, Union[str, int]]) -> None:
        """
        Configure the client from a dictionary. The keys accessed in the dict are job_name or job-name, username,
        password, gateway_url or host and push_interval or push-interval.

        Args:
            config:      Configuration dictionary
        """
        self.job_name = config.get("job_name") or config.get("job-name")
        self.username = config.get("username")
        self.password = config.get("password")
        self.url = config.get("gateway_url") or config.get("host")

        self.push_interval = int(config.get("push_interval") or config.get("push-interval") or 5)

    def _auth_handler(self, url: str, method: str, timeout: int, headers: Dict[str, str], data: Any) -> Callable:
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
        except:  # pylint: disable=bare-except
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
        asset: Optional contextualization.
        push_interval: Seconds between each upload call
        thread_name: Name of thread to start. If omitted, a standard name such as Thread-4 will be generated.
    """

    def __init__(
        self,
        cdf_client: CogniteClient,
        external_id_prefix: str,
        asset: Optional[Asset] = None,
        push_interval: Optional[int] = None,
        thread_name: Optional[str] = None,
    ):
        super(CognitePusher, self).__init__(push_interval, thread_name)

        self.cdf_client = cdf_client
        self.asset = asset
        self.external_id_prefix = external_id_prefix

        self._init_cdf()

        self._cdf_project = cdf_client.login.status().project

    def _init_cdf(self) -> None:
        """
        Initialize the CDF tenant with the necessary time series and asset.
        """
        time_series: List[TimeSeries] = []

        if self.asset is not None:
            # Ensure that asset exist, and retrieve internal ID
            try:
                asset = self.cdf_client.assets.create(self.asset)
            except CogniteDuplicatedError:
                asset = self.cdf_client.assets.retrieve(external_id=self.asset.external_id)

            asset_id = asset.id if asset is not None else None

        else:
            asset_id = None

        for metric in REGISTRY.collect():
            if type(metric) == Metric and metric.type in ["gauge", "counter"]:
                external_id = self.external_id_prefix + metric.name

                time_series.append(
                    TimeSeries(
                        external_id=external_id,
                        name=metric.name,
                        legacy_name=external_id,
                        description=metric.documentation,
                        asset_id=asset_id,
                    )
                )

        ensure_time_series(self.cdf_client, time_series)

    def _push_to_server(self) -> None:
        """
        Create datapoints an push them to their respective time series
        """
        timestamp = int(arrow.get().float_timestamp * 1000)

        datapoints: List[Dict[str, Union[str, List[Tuple[float, float]]]]] = []

        for metric in REGISTRY.collect():
            if type(metric) == Metric and metric.type in ["gauge", "counter"]:
                if len(metric.samples) == 0:
                    continue

                external_id = self.external_id_prefix + metric.name
                datapoints.append({"externalId": external_id, "datapoints": [(timestamp, metric.samples[0].value)]})

        self.cdf_client.datapoints.insert_multiple(datapoints)
        self.logger.debug("Pushed metrics to CDF tenant '%s'", self._cdf_project)
