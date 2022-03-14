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

import unittest
from unittest.mock import Mock, patch

from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, TimeSeries
from cognite.client.exceptions import CogniteNotFoundError

from cognite.extractorutils.util import EitherId, ensure_assets, ensure_time_series


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
