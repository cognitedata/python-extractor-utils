"""
Module containing tools for metric reporting.
"""
import logging
import threading
import time
from typing import Any, Callable, Dict, Optional, Union

from prometheus_client.core import REGISTRY
from prometheus_client.exposition import basic_auth_handler, delete_from_gateway, pushadd_to_gateway

from ._inner_util import _MockLogger


class PrometheusClient:
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

        self.username = username
        self.job_name = job_name
        self.password = password

        self.url = url
        self.push_interval = push_interval

        self.thread: Optional[threading.Thread] = None
        self.thread_name = thread_name
        self.stopping = threading.Event()

        self.logger = logging.getLogger(__name__)

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

    def _run(self):
        """
        Run push loop.
        """
        self.logger.debug("Starting metric push thread to %s", self.url)
        while not self.stopping.is_set():
            self._push_to_server()
            time.sleep(self.push_interval)

    def start(self):
        """
        Starts a thread that pushes the default registery to the configured gateway at certain intervals.
        """
        self.stopping.clear()
        self.thread = threading.Thread(target=self._run, daemon=True, name=self.thread_name)
        self.thread.start()

    def stop(self):
        """
        Stop the push loop.
        """
        # Make sure everything is pushed, and cleanup gateway
        self._push_to_server()

        self.logger.debug("Sending stop event to metrics push thread for gateway %s", self.url)
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
