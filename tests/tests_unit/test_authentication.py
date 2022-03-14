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

from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.authentication import Authenticator, AuthenticatorConfig

config = AuthenticatorConfig(
    tenant="tid",
    client_id="cid",
    scopes=["scp"],
    secret="scrt",
)


def token(expires_in: int, t: str):
    return {
        "expires_in": expires_in,
        "access_token": t,
    }


def test_invalid_token():
    auth = Authenticator(config)
    auth._request = MagicMock(return_value=None)
    with pytest.raises(Exception) as exc:
        auth.get_token()
    auth._request.assert_called_once()
    assert str(exc.value) == "Invalid token"


def test_valid_token():
    auth = Authenticator(config)
    auth._request = MagicMock(return_value=token(2000, "valid"))
    t1 = auth.get_token()
    auth._request.assert_called_once()
    assert t1 == "valid"

    # re-use without new request
    t2 = auth.get_token()
    auth._request.assert_called_once()
    assert t2 == t1


def test_expired_token():
    auth = Authenticator(config)

    # this section is to prime the authenticator with an expired state
    auth._request = MagicMock(return_value=token(20, "expired"))
    with pytest.raises(Exception) as exc:
        auth.get_token()
    auth._request.assert_called_once()
    assert str(exc.value) == "Invalid token"

    # test that an expired token triggers a request
    auth._request = MagicMock(return_value=token(2000, "valid"))
    t = auth.get_token()
    auth._request.assert_called_once()
    assert t == "valid"
