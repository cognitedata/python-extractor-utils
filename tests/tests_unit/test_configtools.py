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
import re
from collections.abc import Generator
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml
from faker import Faker

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
from cognite.extractorutils.configtools.elements import (
    AuthenticatorConfig,
    CastableInt,
    IgnorePattern,
    LocalStateStoreConfig,
    PortNumber,
    RegExpFlag,
    StateStoreConfig,
)
from cognite.extractorutils.configtools.loaders import (
    ConfigResolver,
    compile_patterns,
)
from cognite.extractorutils.configtools.validators import matches_pattern, matches_patterns
from cognite.extractorutils.exceptions import InvalidConfigError
from cognite.extractorutils.statestore.watermark import LocalStateStore


@dataclass
class CastingClass:
    boolean_field: bool
    another_boolean_field: bool
    yet_another_boolean_field: bool
    string_field: str
    another_string_field: str
    yet_another_string_field: str
    path_field: Path


@dataclass
class SimpleStringConfig:
    string_field: str


def test_ensure_snake_case() -> None:
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

    assert snake_dict == _to_snake_case(snake_dict, "snake")
    assert snake_dict == _to_snake_case(hyphen_dict, "hyphen")
    assert snake_dict == _to_snake_case(camel_dict, "camel")
    assert snake_dict == _to_snake_case(pascal_dict, "pascal")


def test_read_cognite_config() -> None:
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

    assert isinstance(config, CogniteConfig)
    assert config.host == "https://api.cognitedata.com"
    assert config.project == "tenant-name"
    assert config.external_id_prefix == "test_"

    client = config.get_cognite_client("test-client")

    assert isinstance(client, CogniteClient)
    assert client.config.base_url == "https://api.cognitedata.com"
    assert client.config.project == "tenant-name"
    assert client.config.client_name == "test-client"


def test_read_base_config() -> None:
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

    assert isinstance(config, BaseConfig)

    assert config.version == "1"

    assert config.cognite.host == "https://greenfield.cognitedata.com"
    assert config.cognite.project == "tenant-name"
    assert config.cognite.external_id_prefix == "test_"

    assert config.logger.console.level == "INFO"
    assert config.logger.file is None


def test_read_invalid_missing_fields() -> None:
    # missing project
    config_raw = """
    # How to label uploaded data in CDF
    external-id-prefix: "test_"
    """

    with pytest.raises(InvalidConfigError):
        load_yaml(config_raw, CogniteConfig)


def test_read_invalid_extra_fields() -> None:
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

    with pytest.raises(InvalidConfigError):
        load_yaml(config_raw, CogniteConfig)


def test_read_invalid_wrong_type() -> None:
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

    with pytest.raises(InvalidConfigError):
        load_yaml(config_raw, CogniteConfig)


def test_get_cognite_client_from_aad() -> None:
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
    assert cdf.config.credentials.token_custom_args == {"audience": "lol"}
    assert isinstance(cdf.config.credentials, OAuthClientCredentials)
    assert cdf.config.credentials.client_id == "cid"
    assert cdf.config.credentials.client_secret == "scrt"
    assert cdf.config.credentials.scopes == ["scp"]
    assert isinstance(cdf, CogniteClient)


def test_read_boolean_casting() -> None:
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
    path-field: "./file"
    """
    config: CastingClass = load_yaml(config_raw, CastingClass)
    assert config.boolean_field
    assert config.another_boolean_field is False
    assert config.yet_another_boolean_field is False
    assert config.string_field == "true"
    assert config.another_string_field == "test"
    assert config.yet_another_string_field == "TeST"


def test_read_invalid_boolean_casting() -> None:
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
    path-field: "./file"
    """
    with pytest.raises(InvalidConfigError):
        load_yaml(config, CastingClass)


def test_read_relative_path() -> None:
    config = """
    boolean-field: true
    another-boolean-field: false
    yet-another-boolean-field: false
    string-field: "true"
    another-string-field: "test"
    yet-another-string-field: "test"
    path-field: "./folder/file.txt"
    """
    config: CastingClass = load_yaml(config, CastingClass)
    assert config.path_field.name == "file.txt"


def test_read_absolute_path() -> None:
    config = """
    boolean-field: true
    another-boolean-field: false
    yet-another-boolean-field: false
    string-field: "true"
    another-string-field: "test"
    yet-another-string-field: "test"
    path-field: "c:/folder/file.txt"
    """
    config: CastingClass = load_yaml(config, CastingClass)
    assert config.path_field.name == "file.txt"


def test_parse_time_interval() -> None:
    assert TimeIntervalConfig("54").seconds == 54
    assert TimeIntervalConfig("54s").seconds == 54
    assert TimeIntervalConfig("120s").seconds == 120
    assert TimeIntervalConfig("2m").seconds == 120
    assert TimeIntervalConfig("1h").seconds == 3600
    assert TimeIntervalConfig("15m").hours == pytest.approx(0.25)
    assert TimeIntervalConfig("15m").minutes == pytest.approx(15)
    assert TimeIntervalConfig("1h").minutes == pytest.approx(60)


def test_parse_file_size() -> None:
    assert FileSizeConfig("154584").bytes == 154584
    assert FileSizeConfig("1kB").bytes == 1000
    assert FileSizeConfig("25MB").bytes == 25_000_000
    assert FileSizeConfig("1kib").bytes == 1024
    assert FileSizeConfig("2.7MiB").bytes == 2831155
    assert FileSizeConfig("4 KB").bytes == 4000

    assert FileSizeConfig("4 KB").kilobytes == pytest.approx(4)
    assert FileSizeConfig("453 kB").megabytes == pytest.approx(0.453)
    assert FileSizeConfig("1543 kiB").kilobytes == pytest.approx(1580.032)
    assert FileSizeConfig("14.5 mb").kilobytes == pytest.approx(14_500)


def test_multiple_logging_console() -> None:
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

    assert len(logger.handlers) == 1

    config.logger.setup_logging()

    assert len(logger.handlers) == 1

    logger.handlers.clear()


def test_multiple_logging_file() -> None:
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
    assert len(logger.handlers) == 1

    config_2.logger.setup_logging()
    assert len(logger.handlers) == 2

    config_1.logger.setup_logging()
    assert len(logger.handlers) == 2

    config_2.logger.setup_logging()
    assert len(logger.handlers) == 2

    logger.handlers.clear()


def test_dump_and_reload_config() -> None:
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
    with open("test_dump_config.yml") as config_file:
        load_yaml(config_file, BaseConfig)


def test_env_substitution() -> None:
    os.environ["STRING_VALUE"] = "heyo"

    config_file1 = "string-field: ${STRING_VALUE}"
    config1: SimpleStringConfig = load_yaml(config_file1, SimpleStringConfig)

    assert config1.string_field == "heyo"

    config_file2 = "string-field: ${STRING_VALUE} in context"
    config2 = load_yaml(config_file2, SimpleStringConfig)

    assert config2.string_field == "heyo in context"

    config_file3 = 'string-field: !env "${STRING_VALUE} in context"'
    config3 = load_yaml(config_file3, SimpleStringConfig)

    assert config3.string_field == "heyo in context"

    config_file4 = "string-field: ${STRING_VALUE}without space"
    config4 = load_yaml(config_file4, SimpleStringConfig)

    assert config4.string_field == "heyowithout space"

    config_file5 = "string-field: !env very${STRING_VALUE}crowded"
    config5 = load_yaml(config_file5, SimpleStringConfig)

    assert config5.string_field == "veryheyocrowded"


def test_env_substitution_remote_check() -> None:
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


def test_cognite_validation() -> None:
    conf = CogniteConfig(project="", idp_authentication=AuthenticatorConfig(client_id="", scopes=[]))
    with pytest.raises(InvalidConfigError) as e:
        conf.get_cognite_client("client-name")
    assert e.value.message == "Project is not set"

    conf.project = "${OhNo}"
    with pytest.raises(InvalidConfigError) as e:
        conf.get_cognite_client("client-name")
    assert e.value.message == "Project (${OhNo}) is not valid"

    conf.project = "test"
    with pytest.raises(InvalidConfigError) as e:
        conf.get_cognite_client("client-name")
    assert e.value.message == "No client certificate or secret provided"

    conf.idp_authentication.secret = "test"
    with pytest.raises(InvalidConfigError) as e:
        conf.get_cognite_client("client-name")
    assert e.value.message == "Either token-url or tenant is required for client credentials authentication"

    conf.idp_authentication.token_url = "${OhNo}"
    with pytest.raises(InvalidConfigError) as e:
        conf.get_cognite_client("client-name")
    assert e.value.message == "Token URL (${OhNo}) must be HTTPS"

    conf.idp_authentication.token_url = None
    conf.idp_authentication.authority = "${OhNo}"
    conf.idp_authentication.tenant = "${OhNo}"
    with pytest.raises(InvalidConfigError) as e:
        conf.get_cognite_client("client-name")
    assert e.value.message == "Authority (${OhNo}) must be HTTPS"

    conf.idp_authentication.authority = "https://login.microsoftonline.com"
    with pytest.raises(InvalidConfigError) as e:
        conf.get_cognite_client("client-name")
    assert e.value.message == "Tenant (${OhNo}) is not valid"

    conf.idp_authentication.token_url = "${OhNo}"
    with pytest.raises(InvalidConfigError) as e:
        conf.get_cognite_client("client-name")
    assert e.value.message == "Token URL (${OhNo}) must be HTTPS"

    conf.idp_authentication.tenant = "foo"
    conf.idp_authentication.token_url = None
    conf.get_cognite_client("client-name")

    conf.idp_authentication.tenant = None
    conf.idp_authentication.token_url = "https://login.microsoftonline.com/foo/token"
    conf.get_cognite_client("client-name")


def test_match_pattern() -> None:
    assert matches_pattern("a*c", "abc")


def test_match_patterns() -> None:
    assert matches_patterns(["a*c"], "abc")


def test_compile_patterns() -> None:
    patterns: list[str | IgnorePattern] = [
        "a*c",
        IgnorePattern("d*f", flags=[RegExpFlag.IGNORECASE]),
        IgnorePattern("m*o", options=[RegExpFlag.IC]),
        IgnorePattern("g*i", options=[RegExpFlag.ASCII]),
        IgnorePattern("j*l", [RegExpFlag.A]),
    ]

    compiled = compile_patterns(patterns)

    for c in compiled:
        assert isinstance(c, re.Pattern)


def test_ignore_pattern() -> None:
    a = IgnorePattern("a*b", flags=[RegExpFlag.IC])
    assert a.options == [RegExpFlag.IC]
    assert a.flags is None

    with pytest.raises(ValueError, match=r"'options' is required."):
        IgnorePattern("d*f")

    with pytest.raises(ValueError, match=r"Only one of either 'options' or 'flags' can be specified."):
        IgnorePattern("g*i", [RegExpFlag.IC], [RegExpFlag.IC])


def test_castable_int_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PORT_NUMBER", "8080")

    config = """
    host: 'localhost'
    port: ${PORT_NUMBER}
    connections: 4
    batch-size: ' 1000 '
    """

    @dataclass
    class DbConfigStd:
        host: str
        port: int
        connections: int
        batch_size: int

    @dataclass
    class DbConfigCastable:
        host: str
        port: PortNumber
        connections: CastableInt
        batch_size: CastableInt

    with pytest.raises(InvalidConfigError):
        load_yaml(config, DbConfigStd)

    parsed: DbConfigCastable = load_yaml(config, DbConfigCastable)
    assert parsed.host == "localhost"
    assert parsed.port == 8080
    assert parsed.connections == 4
    assert parsed.batch_size == 1000


@pytest.fixture
def file_name() -> Generator[str, None, None]:
    name = f"{Faker().word()}.json"
    yield name
    if Path(name).exists():
        os.remove(name)


def test_load_local_statestore(file_name: str) -> None:
    raw_config = f"""
    local:
        path: {file_name}
    """

    config = load_yaml(raw_config, StateStoreConfig)

    assert isinstance(config.local, LocalStateStoreConfig)
    assert str(config.local.path) == file_name

    state_store = config.create_state_store()
    assert isinstance(state_store, LocalStateStore)


def test_load_local_directory_fails() -> None:
    raw_config = """
    local:
        path: cognite
    """

    config = load_yaml(raw_config, StateStoreConfig)

    assert isinstance(config.local, LocalStateStoreConfig)

    with pytest.raises(ValueError) as e:
        config.create_state_store()

    assert "is a directory, and not a file" in str(e.value)
