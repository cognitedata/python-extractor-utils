import logging
import time
from dataclasses import dataclass

import requests

_logger = logging.getLogger(__name__)


@dataclass
class AuthenticatorConfig:
    tenant: str
    client_id: str
    scope: str
    secret: str
    min_ttl: float = 30  # minimum time to live: refresh token ahead of expiration


class Authenticator:
    def __init__(self, config: AuthenticatorConfig):
        self._config = config
        self._request_time = None
        self._response = None

    def _request(self):
        body = {
            "client_id": self._config.client_id,
            "tenant": self._config.tenant,
            "client_secret": self._config.secret,
            "grant_type": "client_credentials",
            "scope": self._config.scope,
        }
        url = f"https://login.microsoftonline.com/{self._config.tenant}/oauth2/v2.0/token"
        r = requests.post(url, data=body)
        _logger.debug("Request AAD token: %d %s", r.status_code, r.reason)
        return r.json()

    def _valid(self):
        if self._response is None or "access_token" not in self._response:
            return False

        expires_in = self._response["expires_in"]
        return self._request_time + expires_in > time.time() + self._config.min_ttl

    def get_token(self):
        if not self._valid():
            self._request_time = time.time()
            self._response = self._request()

        if not self._valid():
            raise Exception("Invalid token")

        return self._response["access_token"]
