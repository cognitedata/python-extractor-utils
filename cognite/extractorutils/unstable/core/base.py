import logging
from multiprocessing import Queue
from threading import RLock, Thread
from types import TracebackType
from typing import Generic, Literal, Optional, Type, TypeVar, Union

from typing_extensions import Self

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig, ExtractorConfig
from cognite.extractorutils.unstable.core._messaging import RuntimeMessage

ConfigType = TypeVar("ConfigType", bound=ExtractorConfig)
ConfigRevision = Union[Literal["local"], int]


class Extractor(Generic[ConfigType]):
    NAME: str
    EXTERNAL_ID: str
    DESCRIPTION: str
    VERSION: str

    CONFIG_TYPE: Type[ConfigType]

    def __init__(
        self,
        connection_config: ConnectionConfig,
        application_config: ConfigType,
        current_config_revision: ConfigRevision,
    ) -> None:
        self.cancellation_token = CancellationToken()
        self.cancellation_token.cancel_on_interrupt()

        self.connection_config = connection_config
        self.application_config = application_config
        self.current_config_revision = current_config_revision

        self.cognite_client = self.connection_config.get_cognite_client(f"{self.EXTERNAL_ID}-{self.VERSION}")

        self._checkin_lock = RLock()
        self._runtime_messages: Optional[Queue[RuntimeMessage]] = None

        self.logger = logging.getLogger(f"{self.EXTERNAL_ID}.main")

    def _set_runtime_message_queue(self, queue: Queue) -> None:
        self._runtime_messages = queue

    def _run_checkin(self) -> None:
        def checkin() -> None:
            body = {"externalId": self.connection_config.extraction_pipeline}

            with self._checkin_lock:
                res = self.cognite_client.post(
                    f"/api/v1/projects/{self.cognite_client.config.project}/odin/checkin",
                    json=body,
                    headers={"cdf-version": "alpha"},
                )
                new_config_revision = res.json().get("lastConfigRevision")

                if new_config_revision and new_config_revision != self.current_config_revision:
                    self.restart()

        while not self.cancellation_token.is_cancelled:
            try:
                checkin()
            except Exception:
                self.logger.exception("Error during checkin")
            self.cancellation_token.wait(10)

    def restart(self) -> None:
        if self._runtime_messages:
            self._runtime_messages.put(RuntimeMessage.RESTART)
        self.cancellation_token.cancel()

    @classmethod
    def init_from_runtime(
        cls,
        connection_config: ConnectionConfig,
        application_config: ConfigType,
        current_config_revision: ConfigRevision,
    ) -> Self:
        return cls(connection_config, application_config, current_config_revision)

    def start(self) -> None:
        self.cognite_client.post(
            f"/api/v1/projects/{self.cognite_client.config.project}/odin/extractorinfo",
            json={
                "externalId": self.connection_config.extraction_pipeline,
                "activeConfigRevision": self.current_config_revision,
                "extractor": {
                    "version": self.VERSION,
                    "externalId": self.EXTERNAL_ID,
                },
            },
            headers={"cdf-version": "alpha"},
        )
        Thread(target=self._run_checkin, name="ExtractorCheckin", daemon=True).start()

    def stop(self) -> None:
        self.cancellation_token.cancel()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.stop()
        return exc_val is None

    def run(self) -> None:
        raise NotImplementedError()
