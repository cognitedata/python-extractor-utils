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
Module containing an authenticator to use Azure AD as an Identity Provider, as an alternative to API keys.

You should have to use this module directly, but if you use the ``get_cogntie_client`` method in the ``CogniteConfig``
class from ``cognite.extractorutils.configtools`` your extractor will be configurable to use Azure AD automatically.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Dict

import requests

_logger = logging.getLogger(__name__)


@dataclass
class AuthenticatorConfig:
    """
    Configuration parameters for Azure AD
    """

    tenant: str
    client_id: str
    scope: str
    secret: str
    min_ttl: float = 30  # minimum time to live: refresh token ahead of expiration


class Authenticator:
    """
    A class gathering an access token

    Args:
        config: Config parameters for the Authenticator
    """

    def __init__(self, config: AuthenticatorConfig):
        self._config = config
        self._request_time = None
        self._response = None

    def _request(self) -> Dict[str, Any]:
        """
        Get OAuth2 token from AAD.

        Returns:
            JSON response from AAD.
        """
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

    def _valid(self) -> bool:
        """
        Get auth state.

        Returns:
            True if authenticated, false if not.
        """
        if self._response is None or "access_token" not in self._response:
            return False

        expires_in = self._response["expires_in"]
        return self._request_time + expires_in > time.time() + self._config.min_ttl

    def get_token(self) -> str:
        """
        Get access token from AAD. Will request new token if previous is expired/invalid.

        Returns:
            Access token.
        """
        if not self._valid():
            self._request_time = time.time()
            self._response = self._request()

        if not self._valid():
            raise Exception("Invalid token")

        return self._response["access_token"]
