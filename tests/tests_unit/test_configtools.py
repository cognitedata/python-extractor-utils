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

import dataclasses
import logging
import os
import unittest
from dataclasses import dataclass

import yaml

from cognite.client import CogniteClient
from cognite.client.credentials import OAuthClientCredentials
from cognite.extractorutils.configtools import (
    BaseConfig,
    CogniteConfig,
    FileSizeConfig,
    LoggingConfig,
    TimeIntervalConfig,
    load_yaml,
)
from cognite.extractorutils.configtools._util import _to_snake_case
from cognite.extractorutils.configtools.elements import AuthenticatorConfig
from cognite.extractorutils.configtools.loaders import ConfigResolver
from cognite.extractorutils.exceptions import InvalidConfigError


@dataclass
class CastingClass:
    boolean_field: bool
    another_boolean_field: bool
    yet_another_boolean_field: bool
    string_field: str
    another_string_field: str
    yet_another_string_field: str


@dataclass
class SimpleStringConfig:
    string_field: str


class TestConfigtoolsMethods(unittest.TestCase):
    def test_ensure_snake_case(self):
        snake_dict = {
            "test_key": "testValue",
            "another_key": "another-value",
            "last": {"last_one": "val1", "last_two": "val2"},
        }
        hyphen_dict = {
            "test-key": "testValue",
            "another-key": "another-value",
            "last": {"last-one": "val1", "last-two": "val2"},
        }
        camel_dict = {
            "testKey": "testValue",
            "anotherKey": "another-value",
            "last": {"lastOne": "val1", "lastTwo": "val2"},
        }
        pascal_dict = {
            "TestKey": "testValue",
            "AnotherKey": "another-value",
            "Last": {"LastOne": "val1", "LastTwo": "val2"},
        }

        self.assertDictEqual(snake_dict, _to_snake_case(snake_dict, "snake"))
        self.assertDictEqual(snake_dict, _to_snake_case(hyphen_dict, "hyphen"))
        self.assertDictEqual(snake_dict, _to_snake_case(camel_dict, "camel"))
        self.assertDictEqual(snake_dict, _to_snake_case(pascal_dict, "pascal"))

    def test_read_cognite_config(self):
        config_raw = """
        # CDF project (also known as tenant name)
        project: tenant-name

        # How to label uploaded data in CDF
        external-id-prefix: "test_"

        idp-authentication:
            client-id: abc123
            secret: def567
            token-url: https://get-a-token.com/token
            scopes:
              - https://api.cognitedata.com/.default
        """

        config = load_yaml(config_raw, CogniteConfig)

        self.assertIsInstance(config, CogniteConfig)
        self.assertEqual(config.host, "https://api.cognitedata.com")
        self.assertEqual(config.project, "tenant-name")
        self.assertEqual(config.external_id_prefix, "test_")

        client = config.get_cognite_client("test-client")

        self.assertIsInstance(client, CogniteClient)
        self.assertEqual(client.config.base_url, "https://api.cognitedata.com")
        self.assertEqual(client.config.project, "tenant-name")
        self.assertEqual(client.config.client_name, "test-client")

    def test_read_base_config(self):
        config_raw = """
        version: "1"

        logger:
            # Console logging
            console:
                level: INFO

        # Information about CDF tenant
        cognite:
            # CDF server
            host: https://greenfield.cognitedata.com

            # CDF project (also known as tenant name)
            project: tenant-name

            # How to label uploaded data in CDF
            external-id-prefix: "test_"

            idp-authentication:
                client-id: abc123
                secret: def567
                token-url: https://get-a-token.com/token
                scopes:
                  - https://api.cognitedata.com/.default
        """

        config = load_yaml(config_raw, BaseConfig)

        self.assertIsInstance(config, BaseConfig)

        self.assertEqual(config.version, "1")

        self.assertEqual(config.cognite.host, "https://greenfield.cognitedata.com")
        self.assertEqual(config.cognite.project, "tenant-name")
        self.assertEqual(config.cognite.external_id_prefix, "test_")

        self.assertEqual(config.logger.console.level, "INFO")
        self.assertIsNone(config.logger.file)

    def test_read_invalid_missing_fields(self):
        # missing project
        config_raw = """
        # How to label uploaded data in CDF
        external-id-prefix: "test_"
        """

        with self.assertRaises(InvalidConfigError):
            load_yaml(config_raw, CogniteConfig)

    def test_read_invalid_extra_fields(self):
        config_raw = """
        # CDF project (also known as tenant name)
        project: tenant-name

        # How to label uploaded data in CDF
        external-id-prefix: "test_"

        idp-authentication:
            client-id: abc123
            secret: def567
            token-url: https://get-a-token.com/token
            scopes:
              - https://api.cognitedata.com/.default

        # Does not exist:
        no-such-field: value
        """

        with self.assertRaises(InvalidConfigError):
            load_yaml(config_raw, CogniteConfig)

    def test_read_invalid_wrong_type(self):
        config_raw = """
        # CDF project (also known as tenant name)
        project: 1234

        # How to label uploaded data in CDF
        external-id-prefix: "test_"

        idp-authentication:
            client-id: abc123
            secret: def567
            token-url: https://get-a-token.com/token
            scopes:
              - https://api.cognitedata.com/.default
        """

        with self.assertRaises(InvalidConfigError):
            load_yaml(config_raw, CogniteConfig)

    def test_get_cognite_client_from_aad(self):
        config_raw = """
        idp-authentication:
            tenant: foo
            client_id: cid
            secret: scrt
            scopes:
                - scp
            min_ttl: 40
        project: tenant-name
        external-id-prefix: "test_"
        """
        config = load_yaml(config_raw, CogniteConfig)
        cdf = config.get_cognite_client("client_name", token_custom_args={"audience": "lol"})
        self.assertEqual(cdf.config.credentials.token_custom_args, {"audience": "lol"})
        self.assertEqual(type(cdf.config.credentials), OAuthClientCredentials)
        self.assertEqual(cdf.config.credentials.client_id, "cid")
        self.assertEqual(cdf.config.credentials.client_secret, "scrt")
        self.assertEqual(cdf.config.credentials.scopes, ["scp"])
        self.assertIsInstance(cdf, CogniteClient)

    def test_read_boolean_casting(self):
        os.environ["TRUE_FLAG"] = "true"
        os.environ["FALSE_FLAG"] = "FALSE"
        os.environ["STR_VAL"] = "TeST"
        config_raw = """
        boolean-field: ${TRUE_FLAG}
        another-boolean-field: ${FALSE_FLAG}
        yet-another-boolean-field: false
        string-field: "true"
        another-string-field: "test"
        yet-another-string-field: ${STR_VAL}
        """
        config: CastingClass = load_yaml(config_raw, CastingClass)
        self.assertTrue(config.boolean_field)
        self.assertFalse(config.another_boolean_field)
        self.assertFalse(config.yet_another_boolean_field)
        self.assertEqual(config.string_field, "true")
        self.assertEqual(config.another_string_field, "test")
        self.assertEqual(config.yet_another_string_field, "TeST")

    def test_read_invalid_boolean_casting(self):
        os.environ["TRUE_FLAG"] = "true"
        os.environ["FALSE_FLAG"] = "FALSE"
        os.environ["INVALID_FLAG"] = "TEST"
        config = """
        boolean-field: ${FALSE_FLAG}
        another-boolean-field: ${INVALID_FLAG}
        yet-another-boolean-field: false
        string-field: "true"
        another-string-field: "test"
        yet-another-string-field: "test"
        """
        with self.assertRaises(InvalidConfigError):
            load_yaml(config, CastingClass)

    def test_parse_time_interval(self):
        self.assertEqual(TimeIntervalConfig("54").seconds, 54)
        self.assertEqual(TimeIntervalConfig("54s").seconds, 54)
        self.assertEqual(TimeIntervalConfig("120s").seconds, 120)
        self.assertEqual(TimeIntervalConfig("2m").seconds, 120)
        self.assertEqual(TimeIntervalConfig("1h").seconds, 3600)
        self.assertAlmostEqual(TimeIntervalConfig("15m").hours, 0.25)
        self.assertAlmostEqual(TimeIntervalConfig("15m").minutes, 15)
        self.assertAlmostEqual(TimeIntervalConfig("1h").minutes, 60)

    def test_parse_file_size(self):
        self.assertEqual(FileSizeConfig("154584").bytes, 154584)
        self.assertEqual(FileSizeConfig("1kB").bytes, 1000)
        self.assertEqual(FileSizeConfig("25MB").bytes, 25_000_000)
        self.assertEqual(FileSizeConfig("1kib").bytes, 1024)
        self.assertEqual(FileSizeConfig("2.7MiB").bytes, 2831155)
        self.assertEqual(FileSizeConfig("4 KB").bytes, 4000)

        self.assertAlmostEqual(FileSizeConfig("4 KB").kilobytes, 4)
        self.assertAlmostEqual(FileSizeConfig("453 kB").megabytes, 0.453)
        self.assertAlmostEqual(FileSizeConfig("1543 kiB").kilobytes, 1580.032)
        self.assertAlmostEqual(FileSizeConfig("14.5 mb").kilobytes, 14_500)

    def test_multiple_logging_console(self):
        config_file = """
        logger:
            console:
                level: INFO
        cognite:
            project: test
            idp-authentication:
                client-id: abc123
                secret: def567
                token-url: https://get-a-token.com/token
                scopes:
                  - https://api.cognitedata.com/.default
            """

        config: BaseConfig = load_yaml(config_file, BaseConfig)
        logger = logging.getLogger()
        logger.handlers.clear()

        config.logger.setup_logging()

        self.assertEqual(1, len(logger.handlers))

        config.logger.setup_logging()

        self.assertEqual(1, len(logger.handlers))

        logger.handlers.clear()

    def test_multiple_logging_file(self):
        config_file_1 = """
        logger:
            file:
                level: INFO
                path: foo
        cognite:
            project: test
            idp-authentication:
                client-id: abc123
                secret: def567
                token-url: https://get-a-token.com/token
                scopes:
                  - https://api.cognitedata.com/.default

            """
        config_file_2 = """
        logger:
            file:
                level: INFO
                path: bar
        cognite:
            project: test
            idp-authentication:
                client-id: abc123
                secret: def567
                token-url: https://get-a-token.com/token
                scopes:
                  - https://api.cognitedata.com/.default
        """
        config_1: BaseConfig = load_yaml(config_file_1, BaseConfig)
        config_2: BaseConfig = load_yaml(config_file_2, BaseConfig)
        logger = logging.getLogger()
        logger.handlers.clear()

        config_1.logger.setup_logging()
        self.assertEqual(1, len(logger.handlers))

        config_2.logger.setup_logging()
        self.assertEqual(2, len(logger.handlers))

        config_1.logger.setup_logging()
        self.assertEqual(2, len(logger.handlers))

        config_2.logger.setup_logging()
        self.assertEqual(2, len(logger.handlers))

        logger.handlers.clear()

    def test_dump_and_reload_config(self):
        # Verify that dumping and reloading a config file doesn't fail due to _file_hash
        config = BaseConfig(
            type=None,
            cognite=CogniteConfig(
                project="project",
                idp_authentication=AuthenticatorConfig(
                    client_id="abc123",
                    secret="def456",
                    token_url="https://token",
                    scopes=["https://api.cognitedata.com/.default"],
                ),
                data_set=None,
                data_set_external_id=None,
                extraction_pipeline=None,
                data_set_id=None,
            ),
            version=None,
            logger=LoggingConfig(console=None, file=None, metrics=None),
        )
        yaml.emitter.Emitter.process_tag = lambda self, *args, **kwargs: None
        yaml.add_representer(TimeIntervalConfig, lambda dump, data: dump.represent_scalar("!timeinterval", str(data)))

        with open("test_dump_config.yml", "w") as config_file:
            yaml.dump(dataclasses.asdict(config), config_file)
        with open("test_dump_config.yml", "r") as config_file:
            load_yaml(config_file, BaseConfig)

    def test_env_substitution(self):
        os.environ["STRING_VALUE"] = "heyo"

        config_file1 = "string-field: ${STRING_VALUE}"
        config1: SimpleStringConfig = load_yaml(config_file1, SimpleStringConfig)

        self.assertEqual(config1.string_field, "heyo")

        config_file2 = "string-field: ${STRING_VALUE} in context"
        config2 = load_yaml(config_file2, SimpleStringConfig)

        self.assertEqual(config2.string_field, "heyo in context")

        config_file3 = 'string-field: !env "${STRING_VALUE} in context"'
        config3 = load_yaml(config_file3, SimpleStringConfig)

        self.assertEqual(config3.string_field, "heyo in context")

        config_file4 = "string-field: ${STRING_VALUE}without space"
        config4 = load_yaml(config_file4, SimpleStringConfig)

        self.assertEqual(config4.string_field, "heyowithout space")

        config_file5 = "string-field: !env very${STRING_VALUE}crowded"
        config5 = load_yaml(config_file5, SimpleStringConfig)

        self.assertEqual(config5.string_field, "veryheyocrowded")

    def test_env_substitution_remote_check(self):
        os.environ["STRING_VALUE"] = "test"

        resolver = ConfigResolver("some-path.yml", BaseConfig)

        resolver._config_text = """
            type: local
            some_field: !env "wow${STRING_VALUE}wow"
        """
        assert not resolver.is_remote

        resolver._config_text = """
            type: ${STRING_VALUE}
            some_field: !env "wow${STRING_VALUE}wow"
        """

        os.environ["STRING_VALUE"] = "remote"
        assert resolver.is_remote
