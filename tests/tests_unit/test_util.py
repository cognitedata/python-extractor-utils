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

import threading
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import httpx
import pytest
import requests

from cognite.client.data_classes import Asset, TimeSeries
from cognite.client.exceptions import CogniteAPIError, CogniteFileUploadError, CogniteNotFoundError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.util import (
    EitherId,
    cognite_exceptions,
    datetime_to_timestamp,
    ensure_assets,
    ensure_time_series,
    httpx_exceptions,
    requests_exceptions,
    retry,
    timestamp_to_datetime,
    truncate_byte_len,
)


@patch("cognite.client.CogniteClient")
def test_ts_nothing_in_cdf(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()
    time_series = [TimeSeries(external_id="a"), TimeSeries(external_id="b")]

    client.time_series.retrieve_multiple = Mock(
        side_effect=CogniteNotFoundError([{"externalId": ts.external_id} for ts in time_series])
    )

    ensure_time_series(client, time_series)

    client.time_series.create.assert_called_once_with(time_series)


@patch("cognite.client.CogniteClient")
def test_ts_some_in_cdf(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()
    existing = [TimeSeries(external_id="a")]
    new = [TimeSeries(external_id="b")]

    client.time_series.retrieve_multiple = Mock(
        side_effect=CogniteNotFoundError([{"externalId": ts.external_id} for ts in new])
    )

    ensure_time_series(client, existing + new)

    client.time_series.create.assert_called_once_with(new)


@patch("cognite.client.CogniteClient")
def test_ts_all_in_cdf(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()
    time_series = [TimeSeries(external_id="a"), TimeSeries(external_id="b")]

    ensure_time_series(client, time_series)

    client.time_series.create.assert_not_called()


@patch("cognite.client.CogniteClient")
def test_assets_nothing_in_cdf(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()
    assets = [Asset(external_id="a"), Asset(external_id="b")]

    client.assets.retrieve_multiple = Mock(
        side_effect=CogniteNotFoundError([{"externalId": ts.external_id} for ts in assets])
    )

    ensure_assets(client, assets)

    client.assets.create.assert_called_once_with(assets)


@patch("cognite.client.CogniteClient")
def test_assets_some_in_cdf(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()
    existing = [Asset(external_id="a")]
    new = [Asset(external_id="b")]

    client.assets.retrieve_multiple = Mock(
        side_effect=CogniteNotFoundError([{"externalId": ts.external_id} for ts in new])
    )

    ensure_assets(client, existing + new)

    client.assets.create.assert_called_once_with(new)


@patch("cognite.client.CogniteClient")
def test_assets_all_in_cdf(MockCogniteClient: Mock) -> None:
    client = MockCogniteClient()
    assets = [Asset(external_id="a"), Asset(external_id="b")]

    ensure_assets(client, assets)

    client.assets.create.assert_not_called()


def test_init() -> None:
    with pytest.raises(TypeError):
        EitherId(id=123, external_id="extId")
    with pytest.raises(TypeError):
        EitherId()


def test_getters() -> None:
    assert EitherId(id=123).content() == 123
    assert EitherId(id=123).type() == "id"
    assert EitherId(external_id="abc").content() == "abc"
    assert EitherId(external_id="abc").type() == "externalId"
    assert EitherId(externalId="abc").content() == "abc"
    assert EitherId(externalId="abc").type() == "externalId"


def test_eq() -> None:
    id1 = EitherId(id=123)
    id2 = EitherId(id=123)

    assert id1 is not id2
    assert id1 == id2

    id1 = EitherId(external_id="abc")
    id2 = EitherId(external_id="abc")

    assert id1 is not id2
    assert id1 == id2


def test_hash() -> None:
    id1 = EitherId(id=123)
    id2 = EitherId(id=123)

    assert hash(id1) == hash(id2)

    id1 = EitherId(external_id="abc")
    id2 = EitherId(external_id="abc")

    assert hash(id1) == hash(id2)


def test_repr() -> None:
    assert EitherId(externalId="extId").__repr__() == "externalId: extId"


def test_cancel() -> None:
    token = CancellationToken()
    assert not token.is_cancelled
    token.cancel()
    assert token.is_cancelled
    token.cancel()  # Does nothing
    assert token.is_cancelled
    assert token.wait(1)  # Returns immediately.


def test_wait() -> None:
    token = CancellationToken()

    def wait() -> None:
        token.wait()

    t1 = threading.Thread(target=wait)
    t2 = threading.Thread(target=wait)
    t1.start()
    t2.start()
    # Wait a bit
    token.wait(0.5)
    assert t1.is_alive()
    assert t2.is_alive()

    token.cancel()
    t1.join(1)
    t2.join(1)
    assert not t1.is_alive()
    assert not t2.is_alive()


def test_child_token() -> None:
    token = CancellationToken()
    child_a = token.create_child_token()
    child_b = token.create_child_token()

    def wait_a() -> None:
        child_a.wait()

    def wait_b() -> None:
        child_b.wait()

    t1 = threading.Thread(target=wait_a)
    t2 = threading.Thread(target=wait_b)
    t1.start()
    t2.start()

    token.wait(0.5)
    assert t1.is_alive()
    assert t2.is_alive()

    child_b.cancel()
    t2.join(1)
    assert not t2.is_alive()
    assert t1.is_alive()

    token.cancel()
    t1.join(1)
    assert not t1.is_alive()


def test_simple_retry() -> None:
    mock = Mock()

    @retry(tries=3, delay=0, jitter=0)
    def call_mock() -> None:
        mock()
        raise ValueError()

    with pytest.raises(ValueError):
        call_mock()

    assert len(mock.call_args_list), 3


def test_simple_retry_specified() -> None:
    mock = Mock()

    @retry(tries=3, delay=0, jitter=0, exceptions=(ValueError,))
    def call_mock() -> None:
        mock()
        raise ValueError()

    with pytest.raises(ValueError):
        call_mock()

    assert len(mock.call_args_list), 3


def test_not_retry_unspecified() -> None:
    mock = Mock()

    @retry(tries=3, delay=0, jitter=0, exceptions=(TypeError,))
    def call_mock() -> None:
        mock()
        raise ValueError()

    with pytest.raises(ValueError):
        call_mock()

    assert len(mock.call_args_list) == 1


def test_retry_conditional() -> None:
    mock = Mock()

    @retry(tries=3, delay=0, jitter=0, exceptions={ValueError: lambda x: "Invalid" not in str(x)})
    def call_mock(is_none: bool) -> None:
        mock()

        if is_none:
            raise ValueError("Could not retrieve value")
        else:
            raise ValueError("Invalid value: 1234")

    with pytest.raises(ValueError):
        call_mock(True)

    assert len(mock.call_args_list) == 3

    mock.reset_mock()

    with pytest.raises(ValueError):
        call_mock(False)

    assert len(mock.call_args_list) == 1


def test_retry_requests() -> None:
    mock = Mock()

    @retry(tries=3, delay=0, jitter=0, exceptions=requests_exceptions())
    def call_mock() -> None:
        mock()
        requests.get("http://localhost:1234/nope")

    with pytest.raises(requests.ConnectionError):
        call_mock()

    assert len(mock.call_args_list) == 3
    mock.reset_mock()

    # 404 should not be retried
    @retry(tries=3, delay=0, jitter=0, exceptions=requests_exceptions())
    def call_mock2() -> None:
        mock()
        res = requests.Response()
        res.status_code = 404
        res.raise_for_status()

    with pytest.raises(requests.HTTPError):
        call_mock2()

    assert len(mock.call_args_list) == 1
    mock.reset_mock()

    # 429 should be retried
    @retry(tries=3, delay=0, jitter=0, exceptions=requests_exceptions())
    def call_mock3() -> None:
        mock()
        res = requests.Response()
        res.status_code = 429
        res.raise_for_status()

    with pytest.raises(requests.HTTPError):
        call_mock3()

    assert len(mock.call_args_list) == 3


def test_httpx_requests() -> None:
    mock = Mock()

    @retry(tries=3, delay=0, jitter=0, exceptions=httpx_exceptions())
    def call_mock() -> None:
        mock()
        httpx.get("http://localhost:1234/nope")

    with pytest.raises(httpx.ConnectError):
        call_mock()

    assert len(mock.call_args_list) == 3
    mock.reset_mock()

    # 404 should not be retried
    @retry(tries=3, delay=0, jitter=0, exceptions=httpx_exceptions())
    def call_mock2() -> None:
        mock()
        res = httpx.Response(404, request=httpx.Request("GET", "http://localhost/"))
        res.raise_for_status()

    with pytest.raises(httpx.HTTPError):
        call_mock2()

    assert len(mock.call_args_list) == 1
    mock.reset_mock()

    # 429 should be retried
    @retry(tries=3, delay=0, jitter=0, exceptions=httpx_exceptions())
    def call_mock3() -> None:
        mock()
        res = httpx.Response(429, request=httpx.Request("GET", "http://localhost/"))
        res.raise_for_status()

    with pytest.raises(httpx.HTTPError):
        call_mock3()

    assert len(mock.call_args_list) == 3


def test_retry_cognite() -> None:
    mock = Mock()

    @retry(tries=3, delay=0, jitter=0, exceptions=cognite_exceptions())
    def call_mock() -> None:
        mock()
        raise CogniteAPIError("hey", code=503)

    with pytest.raises(CogniteAPIError):
        call_mock()

    assert len(mock.call_args_list) == 3
    mock.reset_mock()

    # 403 should not be retried
    @retry(tries=3, delay=0, jitter=0, exceptions=cognite_exceptions())
    def call_mock2() -> None:
        mock()
        raise CogniteAPIError("hey", code=403)

    with pytest.raises(CogniteAPIError):
        call_mock2()

    assert len(mock.call_args_list) == 1
    mock.reset_mock()

    # file errors should be retried
    @retry(tries=3, delay=0, jitter=0, exceptions=cognite_exceptions())
    def call_mock3() -> None:
        mock()
        raise CogniteFileUploadError("hey", 408)

    with pytest.raises(CogniteFileUploadError):
        call_mock3()

    assert len(mock.call_args_list) == 3


def test_datetime_to_timestamp() -> None:
    current_time = datetime.now(tz=timezone.utc)
    ts = datetime_to_timestamp(current_time)

    assert ts == int(current_time.timestamp() * 1000)


def test_timestamp_to_datetime() -> None:
    current_time = datetime.now(tz=timezone.utc)
    dt = timestamp_to_datetime(int(current_time.timestamp() * 1000))

    assert dt.isoformat(timespec="milliseconds") == current_time.isoformat(timespec="milliseconds")


@pytest.mark.parametrize(
    "test_case,expected,ln",
    [
        ("test", "test", 4),
        ("test", "tes", 3),
        ("æææ", "ææ", 4),
        ("æææ", "ææ", 5),
        ("æææ", "æææ", 6),
        ("そそ", "そ", 4),
        ("そそ", "", 2),
        ("そそ", "そそ", 6),
        ("そそ", "そ", 5),
        ("bæbæ", "bæbæ", 6),
        ("bæbæ", "bæb", 5),
        ("bæbæ", "bæb", 4),
        ("bæbæ", "bæ", 3),
    ],
)
def test_truncate_byte_len(test_case: str, expected: str, ln: int) -> None:
    truncated = truncate_byte_len(test_case, ln)
    assert len(truncated) <= ln
    assert truncated == expected
