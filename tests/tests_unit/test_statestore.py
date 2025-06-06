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

import contextlib
import math
import os
import time
from collections import OrderedDict
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import Mock, patch

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.client.exceptions import CogniteAPIError
from cognite.extractorutils.statestore import LocalHashStateStore, LocalStateStore, NoStateStore, RawStateStore
from cognite.extractorutils.uploader import TimeSeriesUploadQueue


def test_set_state() -> None:
    state_store = NoStateStore()

    assert "extId" not in state_store._local_state

    state_store.set_state("extId", low=0, high=4)
    assert state_store._local_state["extId"] == {"low": 0, "high": 4}

    state_store.set_state("extId", low=1)
    assert state_store._local_state["extId"] == {"low": 1, "high": 4}

    state_store.set_state("extId", high=5)
    assert state_store._local_state["extId"] == {"low": 1, "high": 5}

    state_store.set_state("newExtId", high=7)
    assert state_store._local_state["newExtId"] == {"low": None, "high": 7}


def test_expand_state() -> None:
    state_store = NoStateStore()

    assert "extId" not in state_store._local_state

    state_store.expand_state("extId", low=0, high=4)
    assert state_store._local_state["extId"] == {"low": 0, "high": 4}

    # 1 !< 0, should not overwrite
    state_store.expand_state("extId", low=1)
    assert state_store._local_state["extId"] == {"low": 0, "high": 4}

    state_store.expand_state("extId", high=5)
    assert state_store._local_state["extId"] == {"low": 0, "high": 5}

    state_store.expand_state("newExtId", high=7)
    assert state_store._local_state["newExtId"] == {"low": None, "high": 7}

    # 5 !> 7, should not overwrite
    state_store.expand_state("newExtId", high=5)
    assert state_store._local_state["newExtId"] == {"low": None, "high": 7}


def test_outside_state() -> None:
    state_store = NoStateStore()

    state_store.set_state("extId", low=3, high=10)
    assert state_store.outside_state("extId", 1)
    assert state_store.outside_state("extId", 14)
    assert state_store.outside_state("newExtId", 5)
    assert not state_store.outside_state("extId", 5)

    state_store.set_state("onlyHigh", high=7)
    assert not state_store.outside_state("onlyHigh", 6)
    assert not state_store.outside_state("onlyHigh", 7)
    assert state_store.outside_state("onlyHigh", 8)

    state_store.set_state("onlyLow", low=2)
    assert not state_store.outside_state("onlyLow", 3)
    assert not state_store.outside_state("onlyLow", 2)
    assert state_store.outside_state("onlyLow", 1)


def test_delete_state() -> None:
    state_store = NoStateStore()

    assert "extId" not in state_store._local_state

    state_store.set_state("extId", low=5, high=6)
    assert "extId" in state_store._local_state

    state_store.delete_state("extId")
    assert "extId" not in state_store._local_state
    assert state_store._deleted == ["extId"]


def test_get_state() -> None:
    state_store = NoStateStore()

    state_store._local_state = {
        "extId1": {"low": 1, "high": 5},
        "extId2": {"low": None, "high": 4},
        "extId3": {"low": 0},
        "extId4": {"low": 3, "high": None},
    }

    assert state_store.get_state("extId1") == (1, 5)
    assert state_store.get_state("extId2") == (None, 4)
    assert state_store.get_state("extId3") == (0, None)
    assert state_store.get_state("extId4") == (3, None)
    assert state_store.get_state("extId5") == (None, None)

    assert state_store.get_state(["extId1", "extId3", "extId5"]) == [(1, 5), (0, None), (None, None)]


def test_local_state_interaction() -> None:
    state_store = NoStateStore()
    state_store._local_state = {
        "extId1": {"low": 1, "high": 5},
        "extId2": {"low": None, "high": 4},
        "extId3": {"low": 0},
        "extId4": {"low": 3, "high": None},
    }

    assert len(state_store) == len(state_store._local_state)
    for external_id in state_store:
        assert external_id in state_store._local_state


@patch("cognite.client.CogniteClient")
def test_upload_queue_integration(MockCogniteClient: type[CogniteClient]) -> None:
    state_store = NoStateStore()

    upload_queue = TimeSeriesUploadQueue(
        cdf_client=MockCogniteClient(), post_upload_function=state_store.post_upload_handler()
    )

    start: float = datetime.now(tz=timezone.utc).timestamp() * 1000.0

    upload_queue.add_to_upload_queue(external_id="testId", datapoints=[(start + 1, 1), (start + 4, 4)])
    upload_queue.upload()

    assert state_store.get_state("testId") == (start + 1, start + 4)

    upload_queue.add_to_upload_queue(external_id="testId", datapoints=[(start + 2, 2), (start + 3, 3)])
    upload_queue.upload()

    assert state_store.get_state("testId") == (start + 1, start + 4)

    upload_queue.add_to_upload_queue(external_id="testId", datapoints=[(start + 5, 5)])
    upload_queue.upload()

    assert state_store.get_state("testId") == (start + 1, start + 5)

    upload_queue.add_to_upload_queue(external_id="testId", datapoints=[(start + 0, 0)])
    upload_queue.upload()

    assert state_store.get_state("testId") == (start + 0, start + 5)


DATABASE = "testDb"
TABLE = "testTable"


@patch("cognite.client.CogniteClient")
def test_init_no_preexisting_raw(get_client_mock: Mock) -> None:
    client: CogniteClient = get_client_mock()
    state_store = RawStateStore(cdf_client=client, database=DATABASE, table=TABLE)

    client.raw.databases.create.assert_called_once_with(DATABASE)
    client.raw.tables.create.assert_called_once_with(DATABASE, TABLE)


@patch("cognite.client.CogniteClient")
def test_init_preexisting_db(get_client_mock: Mock) -> None:
    client: CogniteClient = get_client_mock()
    client.raw.databases.create = Mock(side_effect=CogniteAPIError("", code=400))

    state_store = RawStateStore(cdf_client=client, database=DATABASE, table=TABLE)
    client.raw.tables.create.assert_called_once_with(DATABASE, TABLE)


@patch("cognite.client.CogniteClient")
def test_init_preexisting_table(get_client_mock: Mock) -> None:
    client: CogniteClient = get_client_mock()
    client.raw.databases.create = Mock(side_effect=CogniteAPIError("", code=400))
    client.raw.tables.create = Mock(side_effect=CogniteAPIError("", code=400))

    state_store = RawStateStore(cdf_client=client, database=DATABASE, table=TABLE)

    client.raw.databases.create.assert_called_once_with(DATABASE)
    client.raw.tables.create.assert_called_once_with(DATABASE, TABLE)


@patch("cognite.client.CogniteClient")
def test_get_raw_states_empty(get_client_mock: Mock) -> None:
    client: CogniteClient = get_client_mock()
    state_store = RawStateStore(cdf_client=client, database=DATABASE, table=TABLE)

    # Make sure raw is not called on init
    client.raw.rows.list.assert_not_called()

    # Get states and test that raw is called
    state_store.initialize()
    client.raw.rows.list.assert_called_once_with(db_name=DATABASE, table_name=TABLE, limit=None)

    # Get states again and make sure that raw is not called twice
    state_store.initialize()
    client.raw.rows.list.assert_called_once_with(db_name=DATABASE, table_name=TABLE, limit=None)

    # Override cache and make sure raw is called again
    state_store.initialize(force=True)
    assert client.raw.rows.list.call_count == 2


@patch("cognite.client.CogniteClient")
def test_get_raw_states_content(get_client_mock: Mock) -> None:
    client: CogniteClient = get_client_mock()
    state_store = RawStateStore(cdf_client=client, database=DATABASE, table=TABLE)

    # Make sure raw is not called on init
    client.raw.rows.list.assert_not_called()

    expected_states = {"extId1": {"high": 3, "low": 1}, "extId2": {"high": 5, "low": 0}}
    client.raw.rows.list = Mock(return_value=[Row(ext_id, expected_states[ext_id]) for ext_id in expected_states])

    # Get states and test that raw is called
    state_store.initialize()
    client.raw.rows.list.assert_called_once_with(db_name=DATABASE, table_name=TABLE, limit=None)

    assert state_store._local_state == expected_states


@patch("cognite.client.CogniteClient")
def test_cdf_sync(get_client_mock: Mock) -> None:
    client: CogniteClient = get_client_mock()
    state_store = RawStateStore(cdf_client=client, database=DATABASE, table=TABLE)

    state_store.initialize()

    states = {"extId1": {"high": 3, "low": 1}, "extId2": {"high": 5, "low": 0}}
    for ext_id in states:
        state_store.set_state(ext_id, **states[ext_id])

    state_store.synchronize()
    client.raw.rows.insert.assert_called_once_with(db_name=DATABASE, table_name=TABLE, row=states)
    client.raw.rows.delete.assert_called_once_with(db_name=DATABASE, table_name=TABLE, key=[])

    state_store.delete_state("extId1")

    assert state_store._deleted == ["extId1"]

    state_store.synchronize()

    client.raw.rows.insert.assert_called_with(db_name=DATABASE, table_name=TABLE, row={"extId2": {"high": 5, "low": 0}})
    client.raw.rows.delete.assert_called_with(db_name=DATABASE, table_name=TABLE, key=["extId1"])
    assert state_store._deleted == []


def test_init_no_file() -> None:
    state_store = LocalStateStore("nosuchfile.json")
    state_store.initialize()


def test_save_and_load() -> None:
    filename = "testfile-localstatestore.json"
    with contextlib.suppress(FileNotFoundError):
        os.remove("testfile-localstatestore.json")

    state_store = LocalStateStore(filename)

    state_store.set_state("ext1", low=1, high=6)
    state_store.set_state("ext2", high=10)
    state_store.set_state("ext3", low=8)
    state_store.set_state("ext4")
    state_store.set_state("ext5", low=Decimal(math.sqrt(5)), high=Decimal(4))

    state_store.synchronize()

    new_state_store = LocalStateStore(filename)
    new_state_store.initialize()

    assert new_state_store.get_state("ext1") == (1, 6)
    assert new_state_store.get_state("ext2") == (None, 10)
    assert new_state_store.get_state("ext3") == (8, None)
    assert new_state_store.get_state("ext4") == (None, None)
    assert new_state_store.get_state("ext5") == (Decimal(math.sqrt(5)), Decimal(4))

    os.remove(filename)


def test_start_stop() -> None:
    filename = "testfile-startstop.json"
    with contextlib.suppress(FileNotFoundError):
        os.remove(filename)

    state_store = LocalStateStore(filename, 1)

    state_store.set_state("ext1", low=1, high=6)
    state_store.set_state("ext2", high=10)
    state_store.set_state("ext3", low=8)
    state_store.set_state("ext4")

    state_store.start()

    time.sleep(3)

    state_store.stop(ensure_synchronize=True)

    time.sleep(1)

    new_state_store = LocalStateStore(filename, 10)
    new_state_store.start()

    time.sleep(1)

    assert new_state_store.get_state("ext1") == (1, 6)
    assert new_state_store.get_state("ext2") == (None, 10)
    assert new_state_store.get_state("ext3") == (8, None)
    assert new_state_store.get_state("ext4") == (None, None)

    new_state_store.stop()

    os.remove(filename)


def test_invalid_file() -> None:
    filename = "testfile-invalid.json"
    with contextlib.suppress(FileNotFoundError):
        os.remove(filename)

    with open(filename, "w") as f:
        f.write("Not json :(")

    with pytest.raises(ValueError):
        LocalStateStore(filename).initialize()


def test_indexing() -> None:
    state_store = NoStateStore()

    state_store["id1"] = (1, 7)
    assert "id1" in state_store
    assert "id0" not in state_store
    assert state_store["id1"] == (1, 7)

    state_store["id2"] = (None, 6)
    assert state_store["id2"][0] is None
    assert state_store["id2"][1] == 6


def test_hash_data() -> None:
    state_store = LocalHashStateStore("hey.json")

    assert state_store._hash_row({"some": "data"}) == state_store._hash_row({"some": "data"})
    assert state_store._hash_row({"some": "data"}) != state_store._hash_row({"some": "other data"})

    # Make sure order doesn't matter
    row1 = {"key": "val1", "key2": "val2"}
    row2 = OrderedDict()
    row2["key"] = "val1"
    row2["key2"] = "val2"

    row3 = OrderedDict()
    row3["key2"] = "val2"
    row3["key"] = "val1"

    new_row = {"key": "val1", "key2": "val3"}

    assert state_store._hash_row(row1) == state_store._hash_row(row2)
    assert state_store._hash_row(row1) == state_store._hash_row(row3)
    assert state_store._hash_row(row1) != state_store._hash_row(new_row)


def test_hash_store() -> None:
    state_store = LocalHashStateStore("hey.json")

    state_store["hello"] = {"Some": "Data"}

    assert not state_store.has_changed("hello", {"Some": "Data"})
    assert state_store.has_changed("hello", {"Some": "New data"})
