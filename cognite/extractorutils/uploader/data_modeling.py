from types import TracebackType
from typing import Any, Callable, Type

from cognite.client import CogniteClient
from cognite.client.data_classes.data_modeling import EdgeApply, NodeApply
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.uploader._base import (
    RETRIES,
    RETRY_BACKOFF_FACTOR,
    RETRY_DELAY,
    RETRY_MAX_DELAY,
    AbstractUploadQueue,
)
from cognite.extractorutils.util import cognite_exceptions, retry


class InstanceUploadQueue(AbstractUploadQueue):
    def __init__(
        self,
        cdf_client: CogniteClient,
        post_upload_function: Callable[[list[Any]], None] | None = None,
        max_queue_size: int | None = None,
        max_upload_interval: int | None = None,
        trigger_log_level: str = "DEBUG",
        thread_name: str | None = None,
        cancellation_token: CancellationToken | None = None,
        auto_create_start_nodes: bool = True,
        auto_create_end_nodes: bool = True,
        auto_create_direct_relations: bool = True,
    ):
        super().__init__(
            cdf_client,
            post_upload_function,
            max_queue_size,
            max_upload_interval,
            trigger_log_level,
            thread_name,
            cancellation_token,
        )

        self.auto_create_start_nodes = auto_create_start_nodes
        self.auto_create_end_nodes = auto_create_end_nodes
        self.auto_create_direct_relations = auto_create_direct_relations

        self.node_queue: list[NodeApply] = []
        self.edge_queue: list[EdgeApply] = []

    def add_to_upload_queue(
        self,
        *,
        node_data: list[NodeApply] | None = None,
        edge_data: list[EdgeApply] | None = None,
    ) -> None:
        if node_data:
            with self.lock:
                self.node_queue.extend(node_data)
                self.upload_queue_size += len(node_data)

        if edge_data:
            with self.lock:
                self.edge_queue.extend(edge_data)
                self.upload_queue_size += len(edge_data)

        with self.lock:
            self._check_triggers()

    def upload(self) -> None:
        @retry(
            exceptions=cognite_exceptions(),
            cancellation_token=self.cancellation_token,
            tries=RETRIES,
            delay=RETRY_DELAY,
            max_delay=RETRY_MAX_DELAY,
            backoff=RETRY_BACKOFF_FACTOR,
        )
        def upload_batch() -> None:
            self.cdf_client.data_modeling.instances.apply(
                nodes=self.node_queue,
                edges=self.edge_queue,
                auto_create_start_nodes=self.auto_create_start_nodes,
                auto_create_end_nodes=self.auto_create_end_nodes,
                auto_create_direct_relations=self.auto_create_direct_relations,
            )
            self.node_queue.clear()
            self.edge_queue.clear()
            self.upload_queue_size = 0

        with self.lock:
            upload_batch()

    def __enter__(self) -> "InstanceUploadQueue":
        """
        Wraps around start method, for use as context manager

        Returns:
            self
        """
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """
        Wraps around stop method, for use as context manager

        Args:
            exc_type: Exception type
            exc_val: Exception value
            exc_tb: Traceback
        """
        self.stop()
