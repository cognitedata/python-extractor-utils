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

import io
import os
import pathlib
import random
import time
from typing import Callable, Optional, Tuple

import pytest

from cognite.client import CogniteClient
from cognite.client.data_classes import FileMetadata
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.data_classes.data_modeling.extractor_extensions.v1 import (
    CogniteExtractorFile,
    CogniteExtractorFileApply,
)
from cognite.extractorutils.uploader.files import BytesUploadQueue, FileUploadQueue, IOFileUploadQueue
from tests.conftest import ETestType, ParamTest


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
    client: CogniteClient, external_id: Optional[str] = None, instance_id: Optional[NodeId] = None
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


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_file_upload_queue(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str) -> None:
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = FileUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=2)

    current_dir = pathlib.Path(__file__).parent.resolve()

    # Upload a pair of actual files
    assert test_parameter.external_ids is not None
    assert test_parameter.space is not None
    queue.add_to_upload_queue(
        file_meta=FileMetadata(external_id=test_parameter.external_ids[0], name=test_parameter.external_ids[0]),
        file_name=current_dir.joinpath("test_file_1.txt"),
    )
    queue.add_to_upload_queue(
        file_meta=FileMetadata(external_id=test_parameter.external_ids[1], name=test_parameter.external_ids[1]),
        file_name=current_dir.joinpath("test_file_2.txt"),
    )
    # Upload the Filemetadata of an empty file without trying to upload the "content"
    queue.add_to_upload_queue(
        file_meta=FileMetadata(external_id=test_parameter.external_ids[3], name=test_parameter.external_ids[3]),
        file_name=current_dir.joinpath("empty_file.txt"),
    )

    queue.add_to_upload_queue(
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[5], name=test_parameter.external_ids[5], space=test_parameter.space
        ),
        file_name=current_dir.joinpath("test_file_1.txt"),
    )
    queue.add_to_upload_queue(
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[6], name=test_parameter.external_ids[6], space=test_parameter.space
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
        NodeId(test_parameter.space, test_parameter.external_ids[8]), node_cls=CogniteExtractorFile
    )
    assert isinstance(node, CogniteExtractorFile)
    assert file6 is not None and file6.instance_id is not None and file6.instance_id.space == test_parameter.space


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_bytes_upload_queue(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str) -> None:
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)

    assert test_parameter.external_ids is not None
    assert test_parameter.space is not None

    queue.add_to_upload_queue(
        content=b"bytes content",
        file_meta=FileMetadata(external_id=test_parameter.external_ids[0], name=test_parameter.external_ids[0]),
    )
    queue.add_to_upload_queue(
        content=b"other bytes content",
        file_meta=FileMetadata(external_id=test_parameter.external_ids[1], name=test_parameter.external_ids[1]),
    )

    queue.add_to_upload_queue(
        content=b"bytes content",
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[5], name=test_parameter.external_ids[5], space=test_parameter.space
        ),
    )
    queue.add_to_upload_queue(
        content=b"other bytes content",
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[6], name=test_parameter.external_ids[6], space=test_parameter.space
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
def test_big_file_upload_queue(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str) -> None:
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
        file_meta=FileMetadata(external_id=test_parameter.external_ids[2], name=test_parameter.external_ids[2]),
    )
    queue.add_to_upload_queue(
        content=content,
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[7], name=test_parameter.external_ids[7], space=test_parameter.space
        ),
    )

    queue.upload()

    await_is_uploaded_status(client, test_parameter.external_ids[2])
    await_is_uploaded_status(client, instance_id=NodeId(test_parameter.space, test_parameter.external_ids[7]))

    bigfile = client.files.download_bytes(external_id=test_parameter.external_ids[2])
    bigfile2 = client.files.download_bytes(instance_id=NodeId(test_parameter.space, test_parameter.external_ids[7]))

    assert len(bigfile) == 10_000_000
    assert len(bigfile2) == 10_000_000


def test_big_file_stream(set_upload_test: Tuple[CogniteClient, ParamTest]) -> None:
    client, test_parameter = set_upload_test
    queue = IOFileUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)
    queue.max_file_chunk_size = 6_000_000
    queue.max_single_chunk_file_size = 6_000_000

    data = b"large" * 2_000_000

    class BufferedReadWithLength(io.BufferedReader):
        def __init__(
            self, raw: io.RawIOBase, buffer_size: int, len: int, on_close: Optional[Callable[[], None]] = None
        ) -> None:
            super().__init__(raw, buffer_size)
            # Do not remove even if it appears to be unused. :P
            #  Requests uses this to add the content-length header, which is necessary for writing to files in azure clusters
            self.len = len
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
        file_meta=FileMetadata(external_id=test_parameter.external_ids[4], name=test_parameter.external_ids[4]),
        read_file=read_file,
    )
    queue.add_io_to_upload_queue(
        file_meta=CogniteExtractorFileApply(
            external_id=test_parameter.external_ids[9], name=test_parameter.external_ids[9], space=test_parameter.space
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


def test_update_files(set_upload_test: Tuple[CogniteClient, ParamTest]) -> None:
    client, test_parameter = set_upload_test
    queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)

    queue.add_to_upload_queue(
        content=b"bytes content",
        file_meta=FileMetadata(external_id=test_parameter.external_ids[0], name=test_parameter.external_ids[0]),
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
