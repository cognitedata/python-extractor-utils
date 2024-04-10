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

import os
import random
from typing import Tuple

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes.assets import Asset
from cognite.extractorutils.uploader.assets import AssetUploadQueue
from tests.conftest import ETestType, ParamTest


@pytest.fixture
def set_test_parameters() -> ParamTest:
    test_id = random.randint(0, 2**31)
    test_parameter = ParamTest(test_type=ETestType.ASSETS)
    test_parameter.external_ids = [
        f"util_integration_asset_test_1-{test_id}",
        f"util_integration_asset_test_2-{test_id}",
        f"util_integration_asset_test_3-{test_id}",
    ]
    return test_parameter


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_assets_upload_queue_upsert(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str):
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = AssetUploadQueue(cdf_client=client)

    # Upload a pair of events
    queue.add_to_upload_queue(Asset(external_id=test_parameter.external_ids[0], description="desc", name="name"))
    queue.add_to_upload_queue(Asset(external_id=test_parameter.external_ids[1], description="desc", name="name"))

    queue.upload()

    # This should result in an update and a create
    queue.add_to_upload_queue(
        Asset(external_id=test_parameter.external_ids[1], description="new desc", name="new name")
    )
    queue.add_to_upload_queue(
        Asset(external_id=test_parameter.external_ids[2], description="new desc", name="new name")
    )

    queue.upload()

    retrieved = client.assets.retrieve_multiple(external_ids=test_parameter.external_ids)
    assert retrieved[0].description == "desc"
    assert retrieved[1].description == "new desc"
    assert retrieved[2].description == "new desc"
    assert retrieved[1].name == "new name"
    assert retrieved[2].name == "new name"
