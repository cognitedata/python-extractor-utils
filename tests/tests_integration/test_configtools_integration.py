#  Copyright 2021 Cognite AS
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

from io import StringIO

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet
from cognite.client.exceptions import CogniteDuplicatedError
from cognite.extractorutils.configtools import BaseConfig, load_yaml


def test_dataset_resolve(set_client: CogniteClient):
    client = set_client
    data_set_name: str = "Extractor Utils Test Data Set"
    data_set_extid: str = "extractorUtils-testdataset"

    data_set_id: int

    try:
        data_set_id = client.data_sets.create(DataSet(name=data_set_name, external_id=data_set_extid)).id
    except CogniteDuplicatedError:
        data_set_id = client.data_sets.retrieve(external_id=data_set_extid).id

    config_file_id = StringIO(
        f"""
    logger:
        console:
            level: INFO

    cognite:
        host: ${{COGNITE_BASE_URL}}
        project: ${{COGNITE_PROJECT}}
        idp-authentication:
            client-id: ${{COGNITE_CLIENT_ID}}
            secret: ${{COGNITE_CLIENT_SECRET}}
            token-url: ${{COGNITE_TOKEN_URL}}
            scopes:
                - ${{COGNITE_BASE_URL}}/.default
        data-set:
            id: {data_set_id}
    """
    )

    config: BaseConfig = load_yaml(config_file_id, BaseConfig)
    print(config)
    assert config.cognite.get_data_set(client).external_id == data_set_extid
    assert config.cognite.get_data_set(client).name == data_set_name
    # TestCase.assertEqual(config.cognite.get_data_set(client).external_id, data_set_extid)
    # TestCase.assertEqual(config.cognite.get_data_set(client).name, data_set_name)

    config_file_extid = StringIO(
        f"""
    logger:
        console:
            level: INFO

    cognite:
        host: ${{COGNITE_BASE_URL}}
        project: ${{COGNITE_PROJECT}}
        idp-authentication:
            client-id: ${{COGNITE_CLIENT_ID}}
            secret: ${{COGNITE_CLIENT_SECRET}}
            token-url: ${{COGNITE_TOKEN_URL}}
            scopes:
                - ${{COGNITE_BASE_URL}}/.default
        data-set:
            external-id: {data_set_extid}
    """
    )

    config2: BaseConfig = load_yaml(config_file_extid, BaseConfig)
    assert config2.cognite.get_data_set(client).id == data_set_id
    assert config2.cognite.get_data_set(client).name == data_set_name
