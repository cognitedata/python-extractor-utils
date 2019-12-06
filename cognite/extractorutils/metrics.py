"""
Module containing tools for metric reporting.
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


class MetricsPusher(ABC):
    def __init__(self, push_interval: Optional[int] = None, thread_name: Optional[str] = None):
        self.push_interval = push_interval
        self.thread_name = thread_name

        self.thread: Optional[threading.Thread] = None
        self.thread_name = thread_name
        self.stopping = threading.Event()

        self.logger = logging.getLogger(__name__)

    @abstractmethod
    def _push_to_server(self):
        pass

    def _run(self):
        """
        Run push loop.
        """
        while not self.stopping.is_set():
            self._push_to_server()
            self.stopping.wait(self.push_interval)

    def start(self):
        """
        Starts a thread that pushes the default registry to the configured gateway at certain intervals.
        """
        self.stopping.clear()
        self.thread = threading.Thread(target=self._run, daemon=True, name=self.thread_name)
        self.thread.start()

    def stop(self):
        """
        Stop the push loop.
        """
        # Make sure everything is pushed
        self._push_to_server()
        self.stopping.set()

    def __enter__(self):
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Wraps around stop method, for use as context manager

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()


class PrometheusPusher(MetricsPusher):
    """
    Client to Prometheus Push Gateway, pushing default metrics registry. Runs a separate thread that routinely pushes
    updated metrics to the push gateway.

    Initializes to an unconfigured client.
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

    def configure(self, config: Dict[str, Union[str, int]]):
        """
        Configure the client from a dictionary. The keys accessed in the dict are job_name, username, password,
        gateway_url and push_interval.

        Args:
            config (dict):      Configuration dictionary
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
            url (str):      Push gateway
            method (str):   HTTP method
            timeout (int):  Request timeout (seconds)
            headers (dict): HTTP headers
            data (any):     Data to send

        Returns:
            prometheus_client.exposition.basic_auth_handler: A authentication handler based on this client.
        """
        return basic_auth_handler(url, method, timeout, headers, data, self.username, self.password)

    def _push_to_server(self):
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

    def clear_gateway(self):
        """
        Delete metrics stored at the gateway (reset gateway).
        """
        delete_from_gateway(self.url, job=self.job_name, handler=self._auth_handler)
        self.logger.debug("Deleted metrics from push gateway %s", self.url)


class CognitePusher(MetricsPusher):
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

    def _init_cdf(self):
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

    def _push_to_server(self):
        timestamp = int(arrow.get().float_timestamp * 1000)

        datapoints: List[Dict[str, Union[str, List[Tuple[float, float]]]]] = []

        for metric in REGISTRY.collect():
            if type(metric) == Metric:
                if len(metric.samples) == 0:
                    continue

                external_id = self.external_id_prefix + metric.name
                datapoints.append({"externalId": external_id, "datapoints": [(timestamp, metric.samples[0].value)]})

        self.cdf_client.datapoints.insert_multiple(datapoints)
