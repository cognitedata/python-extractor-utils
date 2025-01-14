import random
from time import sleep

import pytest
from conftest import NUM_EDGES, NUM_NODES, ETestType, ParamTest
from faker import Faker

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import (
    EdgeApply,
    NodeApply,
    NodeOrEdgeData,
    ViewId,
)
from cognite.extractorutils.uploader.data_modeling import InstanceUploadQueue

fake = Faker()


@pytest.fixture
def set_test_parameters() -> ParamTest:
    test_id = random.randint(0, 2**31)
    return ParamTest(
        test_type=ETestType.DATA_MODELING,
        external_ids=[f"utils-test-{test_id}-node-{i}" for i in range(NUM_NODES)]
        + [f"utils-test-{test_id}-edge-{i}" for i in range(NUM_EDGES)]
        + [f"utils-test-{test_id}-movie"],
    )


def test_dm_upload_queue(set_upload_test: tuple[CogniteClient, ParamTest]) -> None:
    client, test_params = set_upload_test
    queue = InstanceUploadQueue(client)

    for i in range(NUM_NODES):
        queue.add_to_upload_queue(
            node_data=[
                NodeApply(
                    space="ExtractorUtilsTests",
                    external_id=test_params.external_ids[i],
                    sources=[
                        NodeOrEdgeData(
                            source=ViewId(space="ExtractorUtilsTests", external_id="Actor", version="65ce919fcaad7f"),
                            properties={"name": fake.name(), "age": fake.random_int(23, 71)},
                        )
                    ],
                )
            ]
        )

    queue.add_to_upload_queue(
        node_data=[
            NodeApply(
                space="ExtractorUtilsTests",
                external_id=test_params.external_ids[-1],
                sources=[
                    NodeOrEdgeData(
                        source=ViewId(space="ExtractorUtilsTests", external_id="Movie", version="1bf67849d6a6f5"),
                        properties={"name": "Test Movie", "imdbRating": 8.2},
                    )
                ],
            )
        ]
    )

    for i in range(NUM_EDGES):
        queue.add_to_upload_queue(
            edge_data=[
                EdgeApply(
                    space="ExtractorUtilsTests",
                    external_id=test_params.external_ids[NUM_NODES + i],
                    start_node=("ExtractorUtilsTests", test_params.external_ids[i]),
                    end_node=("ExtractorUtilsTests", test_params.external_ids[-1]),
                    type=("ExtractorUtilsTests", "acts-in"),
                )
            ]
        )

    queue.upload()

    sleep(10)

    items = client.data_modeling.instances.retrieve(
        nodes=[("ExtractorUtilsTests", i) for i in test_params.external_ids[0:NUM_NODES]],
        edges=[("ExtractorUtilsTests", i) for i in test_params.external_ids[NUM_NODES : NUM_NODES + NUM_EDGES]],
    )

    assert len(items.nodes) == NUM_NODES
    assert len(items.edges) == NUM_EDGES
