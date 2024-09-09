from io import StringIO

from cognite.extractorutils.unstable.configuration.loaders import ConfigFormat, load_io
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig

CONFIG_EXAMPLE_ONLY_REQUIRED = """
project: test-project
base-url: https://baseurl.com

extraction-pipeline: test-pipeline

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
