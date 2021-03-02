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
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import requests

from .exceptions import InvalidConfigError

_logger = logging.getLogger(__name__)


@dataclass
class AuthenticatorConfig:
    """
    Configuration parameters for an OIDC flow
    """

    client_id: str
    scopes: List[str]
    secret: str
    tenant: Optional[str] = None
    token_url: Optional[str] = None
    resource: Optional[str] = None
    authority: str = "https://login.microsoftonline.com/"
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

        if config.token_url:
            self._token_url = config.token_url

        elif config.tenant:
            base_url = urljoin(self._config.authority, self._config.tenant)
            self._token_url = f"{base_url}/oauth2/v2.0/token"

        else:
            raise InvalidConfigError("No AAD tenant or token url defined")

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
            "scope": " ".join(self._config.scopes),
        }

        if self._config.resource:
            body["resource"] = self._config.resource

        r = requests.post(self._token_url, data=body)
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
