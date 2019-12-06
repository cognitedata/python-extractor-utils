import unittest
from unittest.mock import Mock, patch

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.exceptions import CogniteNotFoundError
from cognite.extractorutils.util import ensure_time_series


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
