import time
import unittest
from unittest.mock import Mock


class TestPrometheusClient(unittest.TestCase):
    def setUp(self):
        from cognite.extractorutils import metrics

        self.altered_metrics = metrics

        self.altered_metrics.delete_from_gateway = Mock()
        self.altered_metrics.pushadd_to_gateway = Mock()

    def test_normal_run(self):
        prom = self.altered_metrics.PrometheusClient()
        prom.configure({"job_name": "test-job", "gateway_url": "none", "push_interval": 1})

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

    def test_empty_configured(self):
        prom = self.altered_metrics.PrometheusClient()
        prom.configure({})

        prom.start()
        time.sleep(0.1)
        self.altered_metrics.pushadd_to_gateway.assert_not_called()
        time.sleep(5.1)
        self.altered_metrics.pushadd_to_gateway.assert_not_called()
        prom.stop()
        self.altered_metrics.pushadd_to_gateway.assert_not_called()

    def test_error_doesnt_stop1(self):
        self.altered_metrics.pushadd_to_gateway = Mock(side_effect=OSError)

        prom = self.altered_metrics.PrometheusClient()
        prom.configure({"job_name": "test-job", "gateway_url": "none", "push_interval": 1})

        prom.start()
        time.sleep(0.1)
        self.assertEqual(self.altered_metrics.pushadd_to_gateway.call_count, 1)
        time.sleep(1.1)
        self.assertEqual(self.altered_metrics.pushadd_to_gateway.call_count, 2)
        prom.stop()

    def test_error_doesnt_stop2(self):
        self.altered_metrics.pushadd_to_gateway = Mock(side_effect=Exception)

        prom = self.altered_metrics.PrometheusClient()
        prom.configure({"job_name": "test-job", "gateway_url": "none", "push_interval": 1})

        prom.start()
        time.sleep(0.1)
        self.assertEqual(self.altered_metrics.pushadd_to_gateway.call_count, 1)
        time.sleep(1.1)
        self.assertEqual(self.altered_metrics.pushadd_to_gateway.call_count, 2)
        prom.stop()

    def test_clear(self):
        prom = self.altered_metrics.PrometheusClient()
        prom.clear_gateway()

        self.altered_metrics.delete_from_gateway.assert_called()
