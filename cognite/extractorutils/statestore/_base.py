import logging
import threading
from abc import ABC, abstractmethod
from typing import Optional

from cognite.extractorutils._inner_util import _resolve_log_level
from cognite.extractorutils.threading import CancellationToken

RETRY_BACKOFF_FACTOR = 1.5
RETRY_MAX_DELAY = 60
RETRY_DELAY = 1
RETRIES = 10


class _BaseStateStore(ABC):
    def __init__(
        self,
        save_interval: Optional[int] = None,
        trigger_log_level: str = "DEBUG",
        thread_name: Optional[str] = None,
        cancellation_token: Optional[CancellationToken] = None,
    ) -> None:
        self._initialized = False

        self.logger = logging.getLogger(__name__)
        self.trigger_log_level = _resolve_log_level(trigger_log_level)
        self.save_interval = save_interval

        self.thread = threading.Thread(target=self._run, daemon=cancellation_token is None, name=thread_name)
        self.lock = threading.RLock()
        self.cancellation_token = cancellation_token.create_child_token() if cancellation_token else CancellationToken()

    def start(self, initialize: bool = True) -> None:
        """
        Start saving state periodically if save_interval is set.
        This calls the synchronize method every save_interval seconds.
        """
        if initialize and not self._initialized:
            self.initialize()
        if self.save_interval is not None:
            self.thread.start()

    def stop(self, ensure_synchronize: bool = True) -> None:
        """
        Stop synchronize thread if running, and ensure state is saved if ensure_synchronize is True.

        Args:
            ensure_synchronize (bool): (Optional). Call synchronize one last time after shutting down thread.
        """
        self.cancellation_token.cancel()
        if ensure_synchronize:
            self.synchronize()

    def _run(self) -> None:
        """
        Internal run method for synchronize thread
        """
        self.initialize()
        while not self.cancellation_token.wait(timeout=self.save_interval):
            try:
                self.logger.log(self.trigger_log_level, "Triggering scheduled state store synchronization")
                self.synchronize()
            except Exception as e:
                self.logger.error("Unexpected error while synchronizing state store: %s.", str(e))

        # trigger stop event explicitly to drain the queue
        self.stop(ensure_synchronize=True)

    @abstractmethod
    def initialize(self, force: bool = False) -> None:
        """
        Get states from remote store
        """
        pass

    @abstractmethod
    def synchronize(self) -> None:
        """
        Upload states to remote store
        """
        pass
