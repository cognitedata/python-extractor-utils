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
import io
import os
import pathlib
import random
import time
from collections.abc import Callable
from typing import BinaryIO

import jsonlines
import pytest
from cognite.client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import (
    CogniteExtractorFile,
    CogniteExtractorFileApply,
)
from cognite.client.exceptions import CogniteNotFoundError
from conftest import ETestType, ParamTest

from cognite.extractorutils.uploader.files import (
    BytesUploadQueue,
    FileUploadQueue,
    IOFileUploadQueue,
)


@pytest.fixture
def set_test_parameters() -> ParamTest:
    test_id = random.randint(0, 2**31)
    test_parameter = ParamTest(test_type=ETestType.FILES)
    test_parameter.external_ids = [
        f"util_integration_file_test_1-{test_id}",
        f"util_integration_file_test_2-{test_id}",
        f"util_integration_file-big-{test_id}",
        f"util_integration_file_test_3-{test_id}",
        f"util_integration_file-big-2-{test_id}",
        f"utils_integration_core_dm_file_test_1-{test_id}",
        f"util_integration_core_dm_file_test_2-{test_id}",
        f"util_integration_core_dm_file-big-{test_id}",
        f"util_integration_core_dm_file_test_3-{test_id}",
        f"util_integration_core_dm_file-big-2-{test_id}",
    ]
    test_parameter.space = "core-dm-test"
    return test_parameter


def await_is_uploaded_status(
    client: CogniteClient,
    external_id: str | None = None,
    instance_id: NodeId | None = None,
) -> None:
    for _ in range(10):
        if external_id is not None:
            retrieved = client.files.retrieve(external_id=external_id)
        elif instance_id is not None:
            retrieved = client.files.retrieve(instance_id=instance_id)
        else:
            raise ValueError("Please provide either external_id or instance_id")
        if retrieved is not None and retrieved.uploaded:
            return
        time.sleep(1)


@pytest.mark.parametrize("functions_runtime", ["true"])
def test_errored_file(set_upload_test: tuple[CogniteClient, ParamTest], functions_runtime: str) -> None:
    LOG_FAILURE_FILE = "integration_test_failure_log.jsonl"
    NO_PERMISSION_FILE = "file_with_no_permission.txt"
    FILE_REASON_MAP_KEY = "file_error_reason_map"
    ERROR_RAISED_ON_FILE_READ = "No permission to read file"

    current_dir = pathlib.Path(__file__).parent.resolve()
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    fully_qualified_failure_logging_path = str(current_dir.joinpath(LOG_FAILURE_FILE))
    queue = IOFileUploadQueue(
        cdf_client=client,
        overwrite_existing=True,
        max_queue_size=2,
        failure_logging_path=fully_qualified_failure_logging_path,
    )

    def load_file_from_path() -> BinaryIO:
        raise Exception(ERROR_RAISED_ON_FILE_READ)

    # Upload a pair of actual files
    assert test_parameter.external_ids is not None
    assert test_parameter.space is not None
    queue.add_io_to_upload_queue(
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[0],
            name=NO_PERMISSION_FILE,
        ),
        read_file=load_file_from_path,
    )

    try:
        queue.upload()

        time.sleep(5)
    except Exception as e:
        failure_logger = queue.get_failure_logger()

        with jsonlines.open(fully_qualified_failure_logging_path, "r") as reader:
            for failure_logger_run in reader:
                assert FILE_REASON_MAP_KEY in failure_logger_run
                assert NO_PERMISSION_FILE in failure_logger_run[FILE_REASON_MAP_KEY]
                assert ERROR_RAISED_ON_FILE_READ in failure_logger_run[FILE_REASON_MAP_KEY][NO_PERMISSION_FILE]
        os.remove(fully_qualified_failure_logging_path)


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_file_upload_queue(set_upload_test: tuple[CogniteClient, ParamTest], functions_runtime: str) -> None:
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = FileUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=2)

    current_dir = pathlib.Path(__file__).parent.resolve()

    # Upload a pair of actual files
    assert test_parameter.external_ids is not None
    assert test_parameter.space is not None
    queue.add_to_upload_queue(
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[0],
            name=test_parameter.external_ids[0],
        ),
        file_name=current_dir.joinpath("test_file_1.txt"),
    )
    queue.add_to_upload_queue(
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[1],
            name=test_parameter.external_ids[1],
        ),
        file_name=current_dir.joinpath("test_file_2.txt"),
    )
    # Upload the Filemetadata of an empty file without trying to upload the "content"
    queue.add_to_upload_queue(
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[3],
            name=test_parameter.external_ids[3],
        ),
        file_name=current_dir.joinpath("empty_file.txt"),
    )

    queue.add_to_upload_queue(
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[5],
            name=test_parameter.external_ids[5],
            space=test_parameter.space,
        ),
        file_name=current_dir.joinpath("test_file_1.txt"),
    )
    queue.add_to_upload_queue(
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[6],
            name=test_parameter.external_ids[6],
            space=test_parameter.space,
        ),
        file_name=current_dir.joinpath("test_file_2.txt"),
    )
    queue.add_to_upload_queue(
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[8],
            name=test_parameter.external_ids[8],
            space=test_parameter.space,
            extracted_data={"testing": "abc", "untested": "ced"},
            directory="mydirectory",
            mime_type="application/json",
        ),
        file_name=current_dir.joinpath("empty_file.txt"),
    )

    queue.upload()

    await_is_uploaded_status(client, test_parameter.external_ids[0])
    await_is_uploaded_status(client, test_parameter.external_ids[1])
    await_is_uploaded_status(client, instance_id=NodeId(test_parameter.space, test_parameter.external_ids[5]))
    await_is_uploaded_status(client, instance_id=NodeId(test_parameter.space, test_parameter.external_ids[6]))

    file1 = client.files.download_bytes(external_id=test_parameter.external_ids[0])
    file2 = client.files.download_bytes(external_id=test_parameter.external_ids[1])
    file3 = client.files.retrieve(external_id=test_parameter.external_ids[3])

    file4 = client.files.download_bytes(instance_id=NodeId(test_parameter.space, test_parameter.external_ids[5]))
    file5 = client.files.download_bytes(instance_id=NodeId(test_parameter.space, test_parameter.external_ids[6]))
    file6 = client.files.retrieve(instance_id=NodeId(test_parameter.space, test_parameter.external_ids[8]))

    assert file1 == b"test content\n"
    assert file2 == b"other test content\n"
    assert file3 is not None and file3.name == test_parameter.external_ids[3]

    assert file4 == b"test content\n"
    assert file5 == b"other test content\n"

    node = client.data_modeling.instances.retrieve_nodes(
        NodeId(test_parameter.space, test_parameter.external_ids[8]),
        node_cls=CogniteExtractorFile,
    )

    assert isinstance(node, CogniteExtractorFile)
    assert file6 is not None and file6.instance_id is not None and file6.instance_id.space == test_parameter.space


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_bytes_upload_queue(set_upload_test: tuple[CogniteClient, ParamTest], functions_runtime: str) -> None:
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)

    assert test_parameter.external_ids is not None
    assert test_parameter.space is not None

    queue.add_to_upload_queue(
        content=b"bytes content",
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[0],
            name=test_parameter.external_ids[0],
        ),
    )
    queue.add_to_upload_queue(
        content=b"other bytes content",
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[1],
            name=test_parameter.external_ids[1],
        ),
    )

    queue.add_to_upload_queue(
        content=b"bytes content",
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[5],
            name=test_parameter.external_ids[5],
            space=test_parameter.space,
        ),
    )
    queue.add_to_upload_queue(
        content=b"other bytes content",
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[6],
            name=test_parameter.external_ids[6],
            space=test_parameter.space,
        ),
    )

    queue.upload()
    await_is_uploaded_status(client, test_parameter.external_ids[0])
    await_is_uploaded_status(client, test_parameter.external_ids[1])
    await_is_uploaded_status(client, instance_id=NodeId(test_parameter.space, test_parameter.external_ids[5]))
    await_is_uploaded_status(client, instance_id=NodeId(test_parameter.space, test_parameter.external_ids[6]))

    file1 = client.files.download_bytes(external_id=test_parameter.external_ids[0])
    file2 = client.files.download_bytes(external_id=test_parameter.external_ids[1])
    file3 = client.files.download_bytes(instance_id=NodeId(test_parameter.space, test_parameter.external_ids[5]))
    file4 = client.files.download_bytes(instance_id=NodeId(test_parameter.space, test_parameter.external_ids[6]))

    assert file1 == b"bytes content"
    assert file2 == b"other bytes content"
    assert file3 == b"bytes content"
    assert file4 == b"other bytes content"


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_big_file_upload_queue(set_upload_test: tuple[CogniteClient, ParamTest], functions_runtime: str) -> None:
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)
    queue.max_file_chunk_size = 6_000_000
    queue.max_single_chunk_file_size = 6_000_000

    content = b"large" * 2_000_000

    assert test_parameter.external_ids is not None
    assert test_parameter.space is not None

    queue.add_to_upload_queue(
        content=content,
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[2],
            name=test_parameter.external_ids[2],
        ),
    )
    queue.add_to_upload_queue(
        content=content,
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[7],
            name=test_parameter.external_ids[7],
            space=test_parameter.space,
        ),
    )

    queue.upload()

    await_is_uploaded_status(client, test_parameter.external_ids[2])
    await_is_uploaded_status(client, instance_id=NodeId(test_parameter.space, test_parameter.external_ids[7]))

    bigfile = client.files.download_bytes(external_id=test_parameter.external_ids[2])
    bigfile2 = client.files.download_bytes(instance_id=NodeId(test_parameter.space, test_parameter.external_ids[7]))

    assert len(bigfile) == 10_000_000
    assert len(bigfile2) == 10_000_000


def test_big_file_stream(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    client, test_parameter = set_upload_test
    queue = IOFileUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)
    queue.max_file_chunk_size = 6_000_000
    queue.max_single_chunk_file_size = 6_000_000

    data = b"large" * 2_000_000

    class BufferedReadWithLength(io.BufferedReader):
        def __init__(
            self,
            raw: io.RawIOBase,
            buffer_size: int,
            length: int,
            on_close: Callable[[], None] | None = None,
        ) -> None:
            super().__init__(raw, buffer_size)
            # Do not remove even if it appears to be unused. :P
            #  Requests uses this to add the content-length header, which is necessary for writing to files in azure clusters
            self.len = length
            self.on_close = on_close

        def close(self) -> None:
            if self.on_close:
                self.on_close()
            return super().close()

    def read_file() -> BufferedReadWithLength:
        return BufferedReadWithLength(io.BytesIO(data), io.DEFAULT_BUFFER_SIZE, len(data))

    assert test_parameter.external_ids is not None
    assert test_parameter.space is not None

    queue.add_io_to_upload_queue(
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[4],
            name=test_parameter.external_ids[4],
        ),
        read_file=read_file,
    )
    queue.add_io_to_upload_queue(
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[9],
            name=test_parameter.external_ids[9],
            space=test_parameter.space,
        ),
        read_file=read_file,
    )

    queue.upload()

    await_is_uploaded_status(client, test_parameter.external_ids[4])
    await_is_uploaded_status(client, instance_id=NodeId(test_parameter.space, test_parameter.external_ids[9]))
    bigfile = client.files.download_bytes(external_id=test_parameter.external_ids[4])
    bigfile2 = client.files.download_bytes(instance_id=NodeId(test_parameter.space, test_parameter.external_ids[9]))

    assert len(bigfile) == 10_000_000
    assert len(bigfile2) == 10_000_000


def test_update_files(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    client, test_parameter = set_upload_test
    queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)

    queue.add_to_upload_queue(
        content=b"bytes content",
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[0],
            name=test_parameter.external_ids[0],
        ),
    )
    queue.add_to_upload_queue(
        content=b"bytes content",
        file_meta=FileMetadata(
            external_id=test_parameter.external_ids[0],
            name=test_parameter.external_ids[0],
            source="some-source",
            directory="/some/directory",
        ),
    )

    queue.upload()
    file = client.files.retrieve(external_id=test_parameter.external_ids[0])
    assert file.source == "some-source"
    assert file.directory == "/some/directory"


_CLUSTERS = [
    pytest.param(
        "extractor-bluefield-testing",
        "https://bluefield.cognitedata.com",
        id="azure-bluefield",
    ),
    pytest.param(
        "extractor-aws-dub-dev-testing",
        "https://aws-dub-dev.cognitedata.com",
        id="aws-dub-dev",
    ),
    pytest.param(
        "extractor-gc-bru-dev-003-testing",
        "https://gc-bru-dev-003.cognitedata.com",
        id="gcs-bru-dev-003",
    ),
]

# Size big enough to trigger multipart upload when paired with the chunk-size
# overrides below (10 MB > 6 MB single-chunk threshold).
_BIG_FILE_BYTES = b"large" * 2_000_000
_MULTIPART_CHUNK_SIZE = 6_000_000


def _build_cluster_client(project: str, base_url: str) -> CogniteClient:
    cognite_token_url = os.environ["COGNITE_TOKEN_URL"]
    cognite_client_id = os.environ["COGNITE_CLIENT_ID"]
    cognite_client_secret = os.environ["COGNITE_CLIENT_SECRET"]
    cognite_project_scopes = os.environ["COGNITE_TOKEN_SCOPES"].split(",")
    client_config = ClientConfig(
        project=project,
        base_url=base_url,
        credentials=OAuthClientCredentials(
            cognite_token_url,
            cognite_client_id,
            cognite_client_secret,
            cognite_project_scopes,
        ),
        client_name="extractor-utils-integration-tests",
    )
    return CogniteClient(client_config)


def _safe_delete_file(client: CogniteClient, external_id: str) -> None:
    with contextlib.suppress(CogniteNotFoundError):
        client.files.delete(external_id=external_id)


@pytest.mark.parametrize(("project", "base_url"), _CLUSTERS)
@pytest.mark.parametrize(
    "mime_type",
    [
        pytest.param("text/plain", id="with-mime"),
        pytest.param(None, id="without-mime"),
    ],
)
def test_some_size_file_upload_across_clusters(project: str, base_url: str, mime_type: str | None) -> None:
    client = _build_cluster_client(project, base_url)
    external_id = f"util_some_size_{'mime' if mime_type else 'nomime'}_{random.randint(0, 2**31)}"
    _safe_delete_file(client, external_id)
    try:
        queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)
        queue.add_to_upload_queue(
            content=b"hello world",
            file_meta=FileMetadata(
                external_id=external_id,
                name=external_id,
                mime_type=mime_type,
            ),
        )
        queue.upload()

        await_is_uploaded_status(client, external_id=external_id)
        retrieved = client.files.retrieve(external_id=external_id)
        assert retrieved is not None
        assert retrieved.uploaded is True
        if mime_type is not None:
            assert retrieved.mime_type == mime_type
        downloaded = client.files.download_bytes(external_id=external_id)
        assert downloaded == b"hello world"
    finally:
        _safe_delete_file(client, external_id)


@pytest.mark.parametrize(("project", "base_url"), _CLUSTERS)
@pytest.mark.parametrize(
    "mime_type",
    [
        pytest.param("text/plain", id="with-mime"),
        pytest.param(None, id="without-mime"),
    ],
)
def test_zero_size_file_upload_across_clusters(project: str, base_url: str, mime_type: str | None) -> None:
    client = _build_cluster_client(project, base_url)
    external_id = f"util_zero_size_{'mime' if mime_type else 'nomime'}_{random.randint(0, 2**31)}"
    current_dir = pathlib.Path(__file__).parent.resolve()
    empty_file = current_dir.joinpath("empty_file.txt")
    _safe_delete_file(client, external_id)
    try:
        queue = FileUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)
        queue.add_to_upload_queue(
            file_meta=FileMetadata(
                external_id=external_id,
                name=external_id,
                mime_type=mime_type,
            ),
            file_name=empty_file,
        )
        queue.upload()

        retrieved = client.files.retrieve(external_id=external_id)
        assert retrieved is not None
        assert retrieved.name == external_id
        if mime_type is not None:
            assert retrieved.mime_type == mime_type
    finally:
        _safe_delete_file(client, external_id)


@pytest.mark.parametrize(("project", "base_url"), _CLUSTERS)
@pytest.mark.parametrize(
    "mime_type",
    [
        pytest.param("application/octet-stream", id="with-mime"),
        pytest.param(None, id="without-mime"),
    ],
)
def test_big_file_multipart_upload_across_clusters(project: str, base_url: str, mime_type: str | None) -> None:
    client = _build_cluster_client(project, base_url)
    external_id = f"util_big_size_{'mime' if mime_type else 'nomime'}_{random.randint(0, 2**31)}"
    _safe_delete_file(client, external_id)
    try:
        queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)
        queue.max_file_chunk_size = _MULTIPART_CHUNK_SIZE
        queue.max_single_chunk_file_size = _MULTIPART_CHUNK_SIZE

        queue.add_to_upload_queue(
            content=_BIG_FILE_BYTES,
            file_meta=FileMetadata(
                external_id=external_id,
                name=external_id,
                mime_type=mime_type,
            ),
        )
        queue.upload()

        await_is_uploaded_status(client, external_id=external_id)
        retrieved = client.files.retrieve(external_id=external_id)
        assert retrieved is not None
        assert retrieved.uploaded is True
        if mime_type is not None:
            assert retrieved.mime_type == mime_type
        downloaded = client.files.download_bytes(external_id=external_id)
        assert len(downloaded) == len(_BIG_FILE_BYTES)
    finally:
        _safe_delete_file(client, external_id)
