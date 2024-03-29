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
import unittest
from unittest.mock import Mock, patch

import httpx
import requests

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries
from cognite.client.exceptions import CogniteNotFoundError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.util import (
    EitherId,
    ensure_assets,
    ensure_time_series,
    httpx_exceptions,
    requests_exceptions,
    retry,
)


class TestEnsureTimeSeries(unittest.TestCase):
    @patch("cognite.client.CogniteClient")
    def setUp(self, MockCogniteClient) -> None:
        self.client: CogniteClient = MockCogniteClient()

    def test_nothing_in_cdf(self):
        time_series = [TimeSeries(external_id="a"), TimeSeries(external_id="b")]

        self.client.time_series.retrieve_multiple = Mock(
            side_effect=CogniteNotFoundError([{"externalId": ts.external_id} for ts in time_series])
        )

        ensure_time_series(self.client, time_series)

        self.client.time_series.create.assert_called_once_with(time_series)

    def test_some_in_cdf(self):
        existing = [TimeSeries(external_id="a")]
        new = [TimeSeries(external_id="b")]

        self.client.time_series.retrieve_multiple = Mock(
            side_effect=CogniteNotFoundError([{"externalId": ts.external_id} for ts in new])
        )

        ensure_time_series(self.client, existing + new)

        self.client.time_series.create.assert_called_once_with(new)

    def test_all_in_cdf(self):
        time_series = [TimeSeries(external_id="a"), TimeSeries(external_id="b")]

        ensure_time_series(self.client, time_series)

        self.client.time_series.create.assert_not_called()


class TestEnsureAssets(unittest.TestCase):
    @patch("cognite.client.CogniteClient")
    def setUp(self, MockCogniteClient) -> None:
        self.client: CogniteClient = MockCogniteClient()

    def test_nothing_in_cdf(self):
        assets = [Asset(external_id="a"), Asset(external_id="b")]

        self.client.assets.retrieve_multiple = Mock(
            side_effect=CogniteNotFoundError([{"externalId": ts.external_id} for ts in assets])
        )

        ensure_assets(self.client, assets)

        self.client.assets.create.assert_called_once_with(assets)

    def test_some_in_cdf(self):
        existing = [Asset(external_id="a")]
        new = [Asset(external_id="b")]

        self.client.assets.retrieve_multiple = Mock(
            side_effect=CogniteNotFoundError([{"externalId": ts.external_id} for ts in new])
        )

        ensure_assets(self.client, existing + new)

        self.client.assets.create.assert_called_once_with(new)

    def test_all_in_cdf(self):
        assets = [Asset(external_id="a"), Asset(external_id="b")]

        ensure_assets(self.client, assets)

        self.client.assets.create.assert_not_called()


class TestEitherId(unittest.TestCase):
    def test_init(self):
        with self.assertRaises(TypeError):
            EitherId(id=123, external_id="extId")
        with self.assertRaises(TypeError):
            EitherId()

    def test_getters(self):
        self.assertEqual(EitherId(id=123).content(), 123)
        self.assertEqual(EitherId(id=123).type(), "id")
        self.assertEqual(EitherId(external_id="abc").content(), "abc")
        self.assertEqual(EitherId(external_id="abc").type(), "externalId")
        self.assertEqual(EitherId(externalId="abc").content(), "abc")
        self.assertEqual(EitherId(externalId="abc").type(), "externalId")

    def test_eq(self):
        id1 = EitherId(id=123)
        id2 = EitherId(id=123)

        self.assertFalse(id1 is id2)
        self.assertTrue(id1 == id2)

        id1 = EitherId(external_id="abc")
        id2 = EitherId(external_id="abc")

        self.assertFalse(id1 is id2)
        self.assertTrue(id1 == id2)

    def test_hash(self):
        id1 = EitherId(id=123)
        id2 = EitherId(id=123)

        self.assertTrue(hash(id1) == hash(id2))

        id1 = EitherId(external_id="abc")
        id2 = EitherId(external_id="abc")

        self.assertTrue(hash(id1) == hash(id2))

    def test_repr(self):
        self.assertEqual(EitherId(externalId="extId").__repr__(), "externalId: extId")


class TestCancellationToken(unittest.TestCase):
    def test_cancel(self):
        token = CancellationToken()
        self.assertFalse(token.is_cancelled)
        token.cancel()
        self.assertTrue(token.is_cancelled)
        token.cancel()  # Does nothing
        self.assertTrue(token.is_cancelled)
        self.assertTrue(token.wait(1))  # Returns immediately.

    def test_wait(self):
        token = CancellationToken()

        def wait():
            token.wait()

        t1 = threading.Thread(target=wait)
        t2 = threading.Thread(target=wait)
        t1.start()
        t2.start()
        # Wait a bit
        token.wait(0.5)
        self.assertTrue(t1.is_alive())
        self.assertTrue(t2.is_alive())

        token.cancel()
        t1.join(1)
        t2.join(1)
        self.assertFalse(t1.is_alive())
        self.assertFalse(t2.is_alive())

    def test_child_token(self):
        token = CancellationToken()
        child_a = token.create_child_token()
        child_b = token.create_child_token()

        def wait_a():
            child_a.wait()

        def wait_b():
            child_b.wait()

        t1 = threading.Thread(target=wait_a)
        t2 = threading.Thread(target=wait_b)
        t1.start()
        t2.start()

        token.wait(0.5)
        self.assertTrue(t1.is_alive())
        self.assertTrue(t2.is_alive())

        child_b.cancel()
        t2.join(1)
        self.assertFalse(t2.is_alive())
        self.assertTrue(t1.is_alive())

        token.cancel()
        t1.join(1)
        self.assertFalse(t1.is_alive())


class TestRetries(unittest.TestCase):
    def test_simple_retry(self) -> None:
        mock = Mock()

        @retry(tries=3, delay=0, jitter=0)
        def call_mock() -> None:
            mock()
            raise ValueError()

        with self.assertRaises(ValueError):
            call_mock()

        self.assertEqual(len(mock.call_args_list), 3)

    def test_simple_retry_specified(self) -> None:
        mock = Mock()

        @retry(tries=3, delay=0, jitter=0, exceptions=(ValueError,))
        def call_mock() -> None:
            mock()
            raise ValueError()

        with self.assertRaises(ValueError):
            call_mock()

        self.assertEqual(len(mock.call_args_list), 3)

    def test_not_retry_unspecified(self) -> None:
        mock = Mock()

        @retry(tries=3, delay=0, jitter=0, exceptions=(TypeError,))
        def call_mock() -> None:
            mock()
            raise ValueError()

        with self.assertRaises(ValueError):
            call_mock()

        self.assertEqual(len(mock.call_args_list), 1)

    def test_retry_conditional(self) -> None:
        mock = Mock()

        @retry(tries=3, delay=0, jitter=0, exceptions={ValueError: lambda x: "Invalid" not in str(x)})
        def call_mock(is_none: bool) -> None:
            mock()

            if is_none:
                raise ValueError("Could not retrieve value")
            else:
                raise ValueError("Invalid value: 1234")

        with self.assertRaises(ValueError):
            call_mock(True)

        self.assertEqual(len(mock.call_args_list), 3)

        mock.reset_mock()

        with self.assertRaises(ValueError):
            call_mock(False)

        self.assertEqual(len(mock.call_args_list), 1)

    def test_retry_requests(self) -> None:
        mock = Mock()

        @retry(tries=3, delay=0, jitter=0, exceptions=requests_exceptions())
        def call_mock() -> None:
            mock()
            requests.get("http://localhost:1234/nope")

        with self.assertRaises(requests.ConnectionError):
            call_mock()

        self.assertEqual(len(mock.call_args_list), 3)
        mock.reset_mock()

        # 404 should not be retried
        @retry(tries=3, delay=0, jitter=0, exceptions=requests_exceptions())
        def call_mock2() -> None:
            mock()
            res = requests.Response()
            res.status_code = 404
            res.raise_for_status()

        with self.assertRaises(requests.HTTPError):
            call_mock2()

        self.assertEqual(len(mock.call_args_list), 1)
        mock.reset_mock()

        # 429 should be retried
        @retry(tries=3, delay=0, jitter=0, exceptions=requests_exceptions())
        def call_mock3() -> None:
            mock()
            res = requests.Response()
            res.status_code = 429
            res.raise_for_status()

        with self.assertRaises(requests.HTTPError):
            call_mock3()

        self.assertEqual(len(mock.call_args_list), 3)

    def test_httpx_requests(self) -> None:
        mock = Mock()

        @retry(tries=3, delay=0, jitter=0, exceptions=httpx_exceptions())
        def call_mock() -> None:
            mock()
            httpx.get("http://localhost:1234/nope")

        with self.assertRaises(httpx.ConnectError):
            call_mock()

        self.assertEqual(len(mock.call_args_list), 3)
        mock.reset_mock()

        # 404 should not be retried
        @retry(tries=3, delay=0, jitter=0, exceptions=httpx_exceptions())
        def call_mock2() -> None:
            mock()
            res = httpx.Response(404, request=httpx.Request("GET", "http://localhost/"))
            res.raise_for_status()

        with self.assertRaises(httpx.HTTPError):
            call_mock2()

        self.assertEqual(len(mock.call_args_list), 1)
        mock.reset_mock()

        # 429 should be retried
        @retry(tries=3, delay=0, jitter=0, exceptions=httpx_exceptions())
        def call_mock3() -> None:
            mock()
            res = httpx.Response(429, request=httpx.Request("GET", "http://localhost/"))
            res.raise_for_status()

        with self.assertRaises(httpx.HTTPError):
            call_mock3()

        self.assertEqual(len(mock.call_args_list), 3)
