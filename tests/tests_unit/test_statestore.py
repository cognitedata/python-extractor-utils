import os
import unittest
from unittest.mock import Mock, patch

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.statestore import LocalStateStore, RawStateStore, StateStore


class NoStateStore(StateStore):
    def initialize(self, force: bool = False) -> None:
        pass

    def synchronize(self) -> None:
        pass


class TestBaseStateStore(unittest.TestCase):
    def test_set_state(self):
        state_store = NoStateStore()

        self.assertFalse("extId" in state_store._local_state)

        state_store.set_state("extId", low=0, high=4)
        self.assertDictEqual(state_store._local_state["extId"], {"low": 0, "high": 4})

        state_store.set_state("extId", low=1)
        self.assertDictEqual(state_store._local_state["extId"], {"low": 1, "high": 4})

        state_store.set_state("extId", high=5)
        self.assertDictEqual(state_store._local_state["extId"], {"low": 1, "high": 5})

        state_store.set_state("newExtId", high=7)
        self.assertDictEqual(state_store._local_state["newExtId"], {"low": None, "high": 7})

    def test_expand_state(self):
        state_store = NoStateStore()

        self.assertFalse("extId" in state_store._local_state)

        state_store.expand_state("extId", low=0, high=4)
        self.assertDictEqual(state_store._local_state["extId"], {"low": 0, "high": 4})

        # 1 !< 0, should not overwrite
        state_store.expand_state("extId", low=1)
        self.assertDictEqual(state_store._local_state["extId"], {"low": 0, "high": 4})

        state_store.expand_state("extId", high=5)
        self.assertDictEqual(state_store._local_state["extId"], {"low": 0, "high": 5})

        state_store.expand_state("newExtId", high=7)
        self.assertDictEqual(state_store._local_state["newExtId"], {"low": None, "high": 7})

        # 5 !> 7, should not overwrite
        state_store.expand_state("newExtId", high=5)
        self.assertDictEqual(state_store._local_state["newExtId"], {"low": None, "high": 7})

    def test_delete_state(self):
        state_store = NoStateStore()

        self.assertFalse("extId" in state_store._local_state)

        state_store.set_state("extId", low=5, high=6)
        self.assertTrue("extId" in state_store._local_state)

        state_store.delete_state("extId")
        self.assertFalse("extId" in state_store._local_state)
        self.assertListEqual(state_store._deleted, ["extId"])

    def test_get_state(self):
        state_store = NoStateStore()

        state_store._local_state = {
            "extId1": {"low": 1, "high": 5},
            "extId2": {"low": None, "high": 4},
            "extId3": {"low": 0},
            "extId4": {"low": 3, "high": None},
        }

        self.assertTupleEqual(state_store.get_state("extId1"), (1, 5))
        self.assertTupleEqual(state_store.get_state("extId2"), (None, 4))
        self.assertTupleEqual(state_store.get_state("extId3"), (0, None))
        self.assertTupleEqual(state_store.get_state("extId4"), (3, None))
        self.assertTupleEqual(state_store.get_state("extId5"), (None, None))

        self.assertListEqual(state_store.get_state(["extId1", "extId3", "extId5"]), [(1, 5), (0, None), (None, None)])


class TestRawStateStore(unittest.TestCase):
    database = "testDb"
    table = "testTable"

    @patch("cognite.client.CogniteClient")
    def setUp(self, MockCogniteClient) -> None:
        self.client: CogniteClient = MockCogniteClient()

    def test_init_no_preexisting_raw(self):
        state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        self.client.raw.databases.create.assert_called_once_with(self.database)
        self.client.raw.tables.create.assert_called_once_with(self.database, self.table)

    def test_init_preexisting_db(self):
        self.client.raw.databases.create = Mock(side_effect=CogniteAPIError("", code=400))

        state_store = RawStateStore(client=self.client, database=self.database, table=self.table)
        self.client.raw.tables.create.assert_called_once_with(self.database, self.table)

    def test_init_preexisting_table(self):
        self.client.raw.databases.create = Mock(side_effect=CogniteAPIError("", code=400))
        self.client.raw.tables.create = Mock(side_effect=CogniteAPIError("", code=400))

        state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        self.client.raw.databases.create.assert_called_once_with(self.database)
        self.client.raw.tables.create.assert_called_once_with(self.database, self.table)

    def test_get_raw_states_empty(self):
        state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        # Make sure raw is not called on init
        self.client.raw.rows.list.assert_not_called()

        # Get states and test that raw is called
        state_store.initialize()
        self.client.raw.rows.list.assert_called_once_with(db_name=self.database, table_name=self.table, limit=None)

        # Get states again and make sure that raw is not called twice
        state_store.initialize()
        self.client.raw.rows.list.assert_called_once_with(db_name=self.database, table_name=self.table, limit=None)

        # Override cache and make sure raw is called again
        state_store.initialize(force=True)
        self.assertEqual(self.client.raw.rows.list.call_count, 2)

    def test_get_raw_states_content(self):
        state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        # Make sure raw is not called on init
        self.client.raw.rows.list.assert_not_called()

        expected_states = {"extId1": {"high": 3, "low": 1}, "extId2": {"high": 5, "low": 0}}
        self.client.raw.rows.list = Mock(
            return_value=[Row(ext_id, expected_states[ext_id]) for ext_id in expected_states]
        )

        # Get states and test that raw is called
        state_store.initialize()
        self.client.raw.rows.list.assert_called_once_with(db_name=self.database, table_name=self.table, limit=None)

        self.assertDictEqual(state_store._local_state, expected_states)

    def test_cdf_sync(self):
        state_store = RawStateStore(client=self.client, database=self.database, table=self.table)

        state_store.initialize()

        states = {"extId1": {"high": 3, "low": 1}, "extId2": {"high": 5, "low": 0}}
        for ext_id in states:
            state_store.set_state(ext_id, **states[ext_id])

        state_store.synchronize()
        self.client.raw.rows.insert.assert_called_once_with(db_name=self.database, table_name=self.table, row=states)
        self.client.raw.rows.delete.assert_called_once_with(db_name=self.database, table_name=self.table, key=[])

        state_store.delete_state("extId1")

        self.assertListEqual(state_store._deleted, ["extId1"])

        state_store.synchronize()

        self.client.raw.rows.insert.assert_called_with(
            db_name=self.database, table_name=self.table, row={"extId2": {"high": 5, "low": 0}}
        )
        self.client.raw.rows.delete.assert_called_with(db_name=self.database, table_name=self.table, key=["extId1"])
        self.assertListEqual(state_store._deleted, [])


class TestLocalStateStore(unittest.TestCase):
    def test_init_no_file(self):
        state_store = LocalStateStore("nosuchfile.json")
        state_store.initialize()

    def test_save_and_load(self):
        filename = "testfile-localstatestore.json"
        try:
            os.remove("testfile-localstatestore.json")
        except FileNotFoundError:
            pass

        state_store = LocalStateStore(filename)

        state_store.set_state("ext1", low=1, high=6)
        state_store.set_state("ext2", high=10)
        state_store.set_state("ext3", low=8)
        state_store.set_state("ext4")

        state_store.synchronize()

        new_state_store = LocalStateStore(filename)
        new_state_store.initialize()

        self.assertTupleEqual(new_state_store.get_state("ext1"), (1, 6))
        self.assertTupleEqual(new_state_store.get_state("ext2"), (None, 10))
        self.assertTupleEqual(new_state_store.get_state("ext3"), (8, None))
        self.assertTupleEqual(new_state_store.get_state("ext4"), (None, None))

        os.remove(filename)
