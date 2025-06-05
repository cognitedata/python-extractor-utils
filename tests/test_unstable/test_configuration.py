import os
from io import StringIO

from cognite.client.credentials import OAuthClientCredentials
from cognite.extractorutils.unstable.configuration.loaders import ConfigFormat, load_io
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig, _ClientCredentialsConfig

CONFIG_EXAMPLE_ONLY_REQUIRED = """
project: test-project
base-url: https://baseurl.com

integration:
  external_id: test-pipeline

authentication:
  type: client-credentials
  client-id: testid
  client-secret: very_secret123
  token-url: https://get-a-token.com/token
  scopes:
    - scopea
"""


def test_load_from_io() -> None:
    stream = StringIO(CONFIG_EXAMPLE_ONLY_REQUIRED)
    config = load_io(stream, ConfigFormat.YAML, ConnectionConfig)

    assert config.project == "test-project"
    assert config.base_url == "https://baseurl.com"
    assert config.authentication.type == "client-credentials"
    assert config.authentication.client_secret == "very_secret123"


def test_from_env() -> None:
    # Init a config from env
    config = ConnectionConfig.from_environment()
    assert config.project == os.environ["COGNITE_PROJECT"]
    assert config.base_url == os.environ["COGNITE_BASE_URL"]
    assert isinstance(config.authentication, _ClientCredentialsConfig)

    client = config.get_cognite_client("utils-tests")
    assert client.config.project == os.environ["COGNITE_PROJECT"]
    assert client.config.base_url == os.environ["COGNITE_BASE_URL"]
    assert isinstance(client.config.credentials, OAuthClientCredentials)

    # Check that the produces cogniteclient object is valid
    assert len(client.assets.list(limit=1)) == 1
