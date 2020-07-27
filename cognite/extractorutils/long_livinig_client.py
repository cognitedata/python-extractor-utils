from threading import Lock

import arrow

from cognite.client import CogniteClient


class LongLivingClient:
    """
    Cognite's Client such will be recreated once in while. Useful to have when you have infrequent calls.
    Helps to survive timeouts errors and pool connection drops.
    Provides either original Client or replica with same config after certain interval

    Args:
        client: CogniteClient object to build from
        ttl: Interval (in seconds) to dispose old client and create new one with same config
    """

    def __init__(self, client: CogniteClient, ttl: int):
        self._client = client
        self._lifetime = arrow.utcnow()
        self._ttl = ttl
        self.lock = Lock()

    def client(self) -> CogniteClient:
        _now = arrow.utcnow()
        with self.lock:
            if (_now - self._lifetime).seconds > self._ttl:
                self._lifetime = arrow.utcnow()
                config = self._client.config
                self._client = CogniteClient(
                    api_key=config.api_key,
                    project=config.project,
                    client_name=config.client_name,
                    base_url=config.base_url,
                    max_workers=config.max_workers,
                    headers=config.headers,
                    timeout=config.timeout,
                    token=config.token,
                    disable_pypi_version_check=config.disable_pypi_version_check,
                )

        return self._client
