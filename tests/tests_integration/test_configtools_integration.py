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
import os
import unittest
from io import StringIO

from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import DataSet
from cognite.client.exceptions import CogniteDuplicatedError
from cognite.extractorutils.configtools import BaseConfig, load_yaml


class TestConfigtools(unittest.TestCase):
    client: CogniteClient

    data_set_name: str = "Extractor Utils Test Data Set"
    data_set_extid: str = "extractorUtils-testdataset"

    data_set_id: int

    @classmethod
    def setUpClass(cls) -> None:
        cognite_project = os.environ["COGNITE_PROJECT"]
        cognite_base_url = os.environ["COGNITE_BASE_URL"]
        cognite_token_url = os.environ["COGNITE_TOKEN_URL"]
        cognite_client_id = os.environ["COGNITE_CLIENT_ID"]
        cognite_client_secret = os.environ["COGNITE_CLIENT_SECRET"]
        cognite_project_scopes = os.environ["COGNITE_TOKEN_SCOPES"].split(",")
        client_config = ClientConfig(
            project=cognite_project,
            base_url=cognite_base_url,
            credentials=OAuthClientCredentials(
                cognite_token_url, cognite_client_id, cognite_client_secret, cognite_project_scopes
            ),
            client_name="extractor-utils-integration-tests",
        )
        cls.client = CogniteClient(client_config)

        try:
            cls.data_set_id = cls.client.data_sets.create(
                DataSet(name=cls.data_set_name, external_id=cls.data_set_extid)
            ).id
        except CogniteDuplicatedError:
            cls.data_set_id = cls.client.data_sets.retrieve(external_id=cls.data_set_extid).id

    def test_dataset_resolve(self):
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
                id: {TestConfigtools.data_set_id}
        """
        )

        config: BaseConfig = load_yaml(config_file_id, BaseConfig)
        print(config)
        self.assertEqual(config.cognite.get_data_set(self.client).external_id, TestConfigtools.data_set_extid)
        self.assertEqual(config.cognite.get_data_set(self.client).name, TestConfigtools.data_set_name)

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
                external-id: {TestConfigtools.data_set_extid}
        """
        )

        config2: BaseConfig = load_yaml(config_file_extid, BaseConfig)
        self.assertEqual(config2.cognite.get_data_set(self.client).id, TestConfigtools.data_set_id)
        self.assertEqual(config2.cognite.get_data_set(self.client).name, TestConfigtools.data_set_name)
