from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.authentication import Authenticator, AuthenticatorConfig

config = AuthenticatorConfig(tenant="tid", client_id="cid", scope="scp", secret="scrt",)


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
    assert auth._request.call_count == 1
    assert str(exc.value) == "Invalid token"


def test_valid_token():
    auth = Authenticator(config)
    auth._request = MagicMock(return_value=token(2000, "valid"))
    t = auth.get_token()
    assert auth._request.call_count == 1
    assert t == "valid"


def test_expired_token():
    auth = Authenticator(config)

    # this section is to prime the authenticator with an expired state
    auth._request = MagicMock(return_value=token(20, "expired"))
    with pytest.raises(Exception) as exc:
        auth.get_token()
    assert auth._request.call_count == 1
    assert str(exc.value) == "Invalid token"

    # test that an expired token triggers a request
    auth._request = MagicMock(return_value=token(2000, "valid"))
    t = auth.get_token()
    assert auth._request.call_count == 1
    assert t == "valid"
