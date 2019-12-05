import unittest
from unittest.mock import Mock, patch

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.statestore import RawStateStore


class TestStateStore(unittest.TestCase):
    database = "testDb"
    table = "testTable"

    @patch("cognite.client.CogniteClient")
    def setUp(self, MockCogniteClient) -> None:
        self.client: CogniteClient = MockCogniteClient()

    def test_init_no_preexisting_raw(self):
        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        self.client.raw.databases.create.assert_called_once_with(self.database)
        self.client.raw.tables.create.assert_called_once_with(self.database, self.table)

    def test_init_preexisting_db(self):
        self.client.raw.databases.create = Mock(side_effect=CogniteAPIError("", code=400))

        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)
        self.client.raw.tables.create.assert_called_once_with(self.database, self.table)

    def test_init_preexisting_table(self):
        self.client.raw.databases.create = Mock(side_effect=CogniteAPIError("", code=400))
        self.client.raw.tables.create = Mock(side_effect=CogniteAPIError("", code=400))

        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        self.client.raw.databases.create.assert_called_once_with(self.database)
        self.client.raw.tables.create.assert_called_once_with(self.database, self.table)

    def test_get_raw_states_empty(self):
        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        # Make sure raw is not called on init
        self.client.raw.rows.list.assert_not_called()

        # Get states and test that raw is called
        self.state_store.get_states()
        self.client.raw.rows.list.assert_called_once_with(db_name=self.database, table_name=self.table, limit=None)

        # Get states again and make sure that raw is not called twice
        self.state_store.get_states()
        self.client.raw.rows.list.assert_called_once_with(db_name=self.database, table_name=self.table, limit=None)

        # Override cache and make sure raw is called again
        self.state_store.get_states(use_cache=False)
        self.assertEqual(self.client.raw.rows.list.call_count, 2)

    def test_get_raw_states_content(self):
        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        # Make sure raw is not called on init
        self.client.raw.rows.list.assert_not_called()

        expected_states = {"extId1": {"high": 3, "low": 1}, "extId2": {"high": 5, "low": 0}}
        self.client.raw.rows.list = Mock(
            return_value=[Row(ext_id, expected_states[ext_id]) for ext_id in expected_states]
        )

        # Get states and test that raw is called
        states = self.state_store.get_states()
        self.client.raw.rows.list.assert_called_once_with(db_name=self.database, table_name=self.table, limit=None)

        self.assertDictEqual(states, expected_states)

    def test_set_state(self):
        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        self.assertFalse("extId" in self.state_store.get_states())

        self.state_store.set_state("extId", low=0, high=4)
        self.assertDictEqual(self.state_store.get_states()["extId"], {"low": 0, "high": 4})

        self.state_store.set_state("extId", low=1)
        self.assertDictEqual(self.state_store.get_states()["extId"], {"low": 1, "high": 4})

        self.state_store.set_state("extId", high=5)
        self.assertDictEqual(self.state_store.get_states()["extId"], {"low": 1, "high": 5})

        self.state_store.set_state("newExtId", high=7)
        self.assertDictEqual(self.state_store.get_states()["newExtId"], {"low": None, "high": 7})

    def test_expand_state(self):
        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        self.assertFalse("extId" in self.state_store.get_states())

        self.state_store.expand_state("extId", low=0, high=4)
        self.assertDictEqual(self.state_store.get_states()["extId"], {"low": 0, "high": 4})

        # 1 !< 0, should not overwrite
        self.state_store.expand_state("extId", low=1)
        self.assertDictEqual(self.state_store.get_states()["extId"], {"low": 0, "high": 4})

        self.state_store.expand_state("extId", high=5)
        self.assertDictEqual(self.state_store.get_states()["extId"], {"low": 0, "high": 5})

        self.state_store.expand_state("newExtId", high=7)
        self.assertDictEqual(self.state_store.get_states()["newExtId"], {"low": None, "high": 7})

        # 5 !> 7, should not overwrite
        self.state_store.expand_state("newExtId", high=5)
        self.assertDictEqual(self.state_store.get_states()["newExtId"], {"low": None, "high": 7})

    def test_delete_state(self):
        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        self.assertFalse("extId" in self.state_store.get_states())

        self.state_store.set_state("extId", low=5, high=6)
        self.assertTrue("extId" in self.state_store.get_states())

        self.state_store.delete_state("extId")
        self.assertFalse("extId" in self.state_store.get_states())
        self.assertListEqual(self.state_store._deleted, ["extId"])

    def test_cdf_sync(self):
        self.state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        self.state_store.get_states()

        states = {"extId1": {"high": 3, "low": 1}, "extId2": {"high": 5, "low": 0}}
        for ext_id in states:
            self.state_store.set_state(ext_id, **states[ext_id])

        self.state_store.synchronize()
        self.client.raw.rows.insert.assert_called_once_with(db_name=self.database, table_name=self.table, row=states)
        self.client.raw.rows.delete.assert_called_once_with(db_name=self.database, table_name=self.table, key=[])

        self.state_store.delete_state("extId1")

        self.assertListEqual(self.state_store._deleted, ["extId1"])

        self.state_store.synchronize()

        self.client.raw.rows.insert.assert_called_with(
            db_name=self.database, table_name=self.table, row={"extId2": {"high": 5, "low": 0}}
        )
        self.client.raw.rows.delete.assert_called_with(db_name=self.database, table_name=self.table, key=["extId1"])
        self.assertListEqual(self.state_store._deleted, [])
