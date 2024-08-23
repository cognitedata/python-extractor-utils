import os

import pytest

from cognite.client import CogniteClient


@pytest.mark.unstable
def test_is_dev_cluster(set_client: CogniteClient) -> None:
    # Check that the client works
    print(set_client.assets.list())

    # Check that we are running against a dev project/cluster
    assert set_client.config.project == os.environ["COGNITE_DEV_PROJECT"]
    assert "dev" in set_client.config.base_url
