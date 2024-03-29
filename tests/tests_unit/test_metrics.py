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

import time
import unittest
from unittest.mock import Mock, patch

import arrow
from prometheus_client import Gauge

from cognite.client.data_classes import Asset, TimeSeries
from cognite.client.exceptions import CogniteDuplicatedError, CogniteNotFoundError
from cognite.extractorutils.metrics import CognitePusher, safe_get


class TestPrometheusPusher(unittest.TestCase):
    def setUp(self):
        from cognite.extractorutils import metrics

        self.altered_metrics = metrics

        self.altered_metrics.delete_from_gateway = Mock()
        self.altered_metrics.pushadd_to_gateway = Mock()

    def test_normal_run(self):
        prom = self.altered_metrics.PrometheusPusher(job_name="test-job", url="none", push_interval=1)

        last = self.altered_metrics.pushadd_to_gateway.call_count
        prom.start()
        time.sleep(0.1)
        self.assertGreaterEqual(self.altered_metrics.pushadd_to_gateway.call_count, last + 1)
        last = self.altered_metrics.pushadd_to_gateway.call_count
        time.sleep(1.1)
        self.assertGreaterEqual(self.altered_metrics.pushadd_to_gateway.call_count, last + 1)
        last = self.altered_metrics.pushadd_to_gateway.call_count
        prom.stop()
        self.assertGreaterEqual(self.altered_metrics.pushadd_to_gateway.call_count, last + 1)
        last = self.altered_metrics.pushadd_to_gateway.call_count
        time.sleep(1.1)
        self.assertGreaterEqual(self.altered_metrics.pushadd_to_gateway.call_count, last)

    def test_error_doesnt_stop1(self):
        self.altered_metrics.pushadd_to_gateway = Mock(side_effect=OSError)

        prom = self.altered_metrics.PrometheusPusher(job_name="test-job", url="none", push_interval=1)

        prom.start()
        time.sleep(0.1)
        self.assertEqual(self.altered_metrics.pushadd_to_gateway.call_count, 1)
        time.sleep(1.1)
        self.assertEqual(self.altered_metrics.pushadd_to_gateway.call_count, 2)
        prom.stop()

    def test_error_doesnt_stop2(self):
        self.altered_metrics.pushadd_to_gateway = Mock(side_effect=Exception)

        prom = self.altered_metrics.PrometheusPusher(job_name="test-job", url="none", push_interval=1)

        prom.start()
        time.sleep(0.1)
        self.assertEqual(self.altered_metrics.pushadd_to_gateway.call_count, 1)
        time.sleep(1.1)
        self.assertEqual(self.altered_metrics.pushadd_to_gateway.call_count, 2)
        prom.stop()

    def test_clear(self):
        prom = self.altered_metrics.PrometheusPusher(job_name="test-job", url="none", push_interval=1)
        prom.clear_gateway()

        self.altered_metrics.delete_from_gateway.assert_called()


class TestCognitePusher(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.gauge = Gauge("gauge", "Test gauge")

    @patch("cognite.client.CogniteClient")
    def setUp(self, MockCogniteClient) -> None:
        self.client = MockCogniteClient()

    def test_init_empty_cdf(self):
        self.client.time_series.retrieve_multiple = Mock(
            side_effect=CogniteNotFoundError([{"externalId": "pre_gauge"}])
        )

        return_asset = Asset(id=123, external_id="asset", name="asset")
        new_asset = Asset(external_id="asset", name="asset")

        self.client.assets.create = Mock(return_value=return_asset)

        pusher = CognitePusher(self.client, external_id_prefix="pre_", asset=new_asset, push_interval=1)

        # Assert time series created
        # Hacky assert_called_once_with as the TimeSeries object is not the same obj, just equal content
        self.client.time_series.create.assert_called_once()
        print(self.client.time_series.create.call_args_list)
        self.assertDictEqual(
            self.client.time_series.create.call_args_list[0][0][0][0].dump(),
            TimeSeries(
                external_id="pre_gauge", name="gauge", legacy_name="pre_gauge", description="Test gauge", asset_id=123
            ).dump(),
        )

        # Assert asset created
        self.client.assets.create.assert_called_once_with(new_asset)

    def test_init_existing_asset(self):
        self.client.time_series.retrieve_multiple = Mock(
            side_effect=CogniteNotFoundError([{"externalId": "pre_gauge"}])
        )

        return_asset = Asset(id=123, external_id="assetid", name="asset")
        new_asset = Asset(external_id="assetid", name="asset")

        self.client.assets.create = Mock(side_effect=CogniteDuplicatedError(["assetid"]))
        self.client.assets.retrieve = Mock(return_value=return_asset)

        pusher = CognitePusher(self.client, external_id_prefix="pre_", asset=new_asset, push_interval=1)

        # Assert time series created
        # Hacky assert_called_once_with as the TimeSeries object is not the same obj, just equal content
        self.client.time_series.create.assert_called_once()
        self.assertDictEqual(
            self.client.time_series.create.call_args_list[0][0][0][0].dump(),
            TimeSeries(
                external_id="pre_gauge",
                name="gauge",
                legacy_name="pre_gauge",
                description="Test gauge",
                asset_id=123,
            ).dump(),
        )

        # Assert asset created
        self.client.assets.create.assert_called_once_with(new_asset)
        self.client.assets.retrieve.assert_called_once_with(external_id="assetid")

    def test_init_existing_all(self):
        return_asset = Asset(id=123, external_id="assetid", name="asset")
        new_asset = Asset(external_id="assetid", name="asset")

        self.client.assets.create = Mock(side_effect=CogniteDuplicatedError(["assetid"]))
        self.client.assets.retrieve = Mock(return_value=return_asset)

        pusher = CognitePusher(self.client, external_id_prefix="pre_", asset=new_asset, push_interval=1)

        # Assert time series created
        self.client.time_series.create.assert_not_called()

        # Assert asset created
        self.client.assets.create.assert_called_once_with(new_asset)
        self.client.assets.retrieve.assert_called_once_with(external_id="assetid")

    def test_push(self):
        pusher = CognitePusher(self.client, "pre_", push_interval=1)

        TestCognitePusher.gauge.set(5)
        pusher._push_to_server()

        self.client.time_series.data.insert_multiple.assert_called_once()
        args = self.client.time_series.data.insert_multiple.call_args_list[0][0][0][-1]

        timestamp = int(arrow.get().float_timestamp * 1000)
        self.assertEqual(args["externalId"], "pre_gauge")
        self.assertLess(abs(timestamp - args["datapoints"][0][0]), 100)  # less than 100 ms
        self.assertAlmostEqual(args["datapoints"][0][1], 5)


my_class_counter = 0


class MyClass:
    def __init__(self):
        global my_class_counter
        my_class_counter += 1


class MyBettrerClass:
    def __init__(self, value):
        global my_class_counter
        my_class_counter += value


class TestMetricUtils(unittest.TestCase):
    def setUp(self) -> None:
        global my_class_counter
        my_class_counter = 0

    def test_safe_get(self):
        self.assertEqual(my_class_counter, 0)

        a = safe_get(MyClass)
        self.assertEqual(my_class_counter, 1)
        self.assertIsInstance(a, MyClass)

        b = safe_get(MyClass)
        self.assertEqual(my_class_counter, 1)
        self.assertIsInstance(b, MyClass)
        self.assertIs(a, b)

        c = safe_get(MyBettrerClass, 5)
        self.assertEqual(my_class_counter, 6)
        self.assertIsInstance(c, MyBettrerClass)
