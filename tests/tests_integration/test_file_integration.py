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
    ]
    return test_parameter


def await_is_uploaded_status(client: CogniteClient, external_id: str):
    for _ in range(10):
        if client.files.retrieve(external_id=external_id).uploaded:
            return
        time.sleep(1)


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_file_upload_queue(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str):
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = FileUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=2)

    current_dir = pathlib.Path(__file__).parent.resolve()

    # Upload a pair of actual files
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

    queue.upload()

    await_is_uploaded_status(client, test_parameter.external_ids[0])
    await_is_uploaded_status(client, test_parameter.external_ids[1])
    file1 = client.files.download_bytes(external_id=test_parameter.external_ids[0])
    file2 = client.files.download_bytes(external_id=test_parameter.external_ids[1])
    file3 = client.files.retrieve(external_id=test_parameter.external_ids[3])

    assert file1 == b"test content\n"
    assert file2 == b"other test content\n"
    assert file3.name == test_parameter.external_ids[3]


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_bytes_upload_queue(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str):
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)

    queue.add_to_upload_queue(
        content=b"bytes content",
        metadata=FileMetadata(external_id=test_parameter.external_ids[0], name=test_parameter.external_ids[0]),
    )
    queue.add_to_upload_queue(
        content=b"other bytes content",
        metadata=FileMetadata(external_id=test_parameter.external_ids[1], name=test_parameter.external_ids[1]),
    )

    queue.upload()
    await_is_uploaded_status(client, test_parameter.external_ids[0])
    await_is_uploaded_status(client, test_parameter.external_ids[1])
    file1 = client.files.download_bytes(external_id=test_parameter.external_ids[0])
    file2 = client.files.download_bytes(external_id=test_parameter.external_ids[1])

    assert file1 == b"bytes content"
    assert file2 == b"other bytes content"


@pytest.mark.parametrize("functions_runtime", ["true", "false"])
def test_big_file_upload_queue(set_upload_test: Tuple[CogniteClient, ParamTest], functions_runtime: str):
    os.environ["COGNITE_FUNCTION_RUNTIME"] = functions_runtime
    client, test_parameter = set_upload_test
    queue = BytesUploadQueue(cdf_client=client, overwrite_existing=True, max_queue_size=1)
    queue.max_file_chunk_size = 6_000_000
    queue.max_single_chunk_file_size = 6_000_000

    content = b"large" * 2_000_000

    queue.add_to_upload_queue(
        content=content,
        metadata=FileMetadata(external_id=test_parameter.external_ids[2], name=test_parameter.external_ids[2]),
    )

    queue.upload()

    await_is_uploaded_status(client, test_parameter.external_ids[2])
    bigfile = client.files.download_bytes(external_id=test_parameter.external_ids[2])

    assert len(bigfile) == 10_000_000


def test_big_file_stream(set_upload_test: Tuple[CogniteClient, ParamTest]):
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

    def read_file():
        return BufferedReadWithLength(io.BytesIO(data), io.DEFAULT_BUFFER_SIZE, len(data))

    queue.add_io_to_upload_queue(
        meta_or_apply=FileMetadata(external_id=test_parameter.external_ids[4], name=test_parameter.external_ids[4]),
        read_file=read_file,
    )

    queue.upload()

    await_is_uploaded_status(client, test_parameter.external_ids[4])
    bigfile = client.files.download_bytes(external_id=test_parameter.external_ids[4])

    assert len(bigfile) == 10_000_000
