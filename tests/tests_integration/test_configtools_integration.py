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
import random
import unittest
from io import StringIO

from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet
from cognite.client.exceptions import CogniteDuplicatedError
from cognite.extractorutils.configtools import BaseConfig, CogniteConfig, load_yaml


class TestConfigtools(unittest.TestCase):
    client: CogniteClient

    data_set_name: str = f"Extractor Utils Test Data Set"
    data_set_extid: str = f"extractorUtils-testdataset"

    data_set_id: int

    @classmethod
    def setUpClass(cls) -> None:
        cls.client = CogniteClient(client_name="extractor-utils-integration-tests",)

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
            api-key: ${{COGNITE_API_KEY}}
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
            api-key: ${{COGNITE_API_KEY}}
            data-set:
                external-id: {TestConfigtools.data_set_extid}
        """
        )

        config2: BaseConfig = load_yaml(config_file_extid, BaseConfig)
        self.assertEqual(config2.cognite.get_data_set(self.client).id, TestConfigtools.data_set_id)
        self.assertEqual(config2.cognite.get_data_set(self.client).name, TestConfigtools.data_set_name)
