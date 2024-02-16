import logging
import signal
from threading import Condition
from typing import Any, Optional


class CancellationToken:
    """
    Abstraction for a hierarchical cancellation token.

    Using this you can create hierarchies of cancellation tokens, to cancel a part of the extractor
    without cancelling the whole process. Use ``create_child_token`` to create a token that will be
    cancelled if the parent is cancelled, but can be canceled alone without affecting the parent token.
    """

    def __init__(self, condition: Optional[Condition] = None) -> None:
        self._cv: Condition = condition or Condition()
        self._is_cancelled_int: bool = False
        self._parent: Optional["CancellationToken"] = None

    def __repr__(self) -> str:
        cls = self.__class__
        status = "cancelled" if self.is_cancelled else "not cancelled"
        return f"<{cls.__module__}.{cls.__qualname__} at {id(self):#x}: {status}>"

    @property
    def is_cancelled(self) -> bool:
        """
        ``True`` if the token has been cancelled, or if some parent token has been cancelled.
        """
        return self._is_cancelled_int or self._parent is not None and self._parent.is_cancelled

    def is_set(self) -> bool:
        """
        Deprecated, use ``is_cancelled`` instead.

        ``True`` if the token has been cancelled, or if some parent token has been cancelled.
        """
        return self.is_cancelled

    def cancel(self) -> None:
        """
        Cancel the token, notifying any waiting threads.
        """
        # No point in cancelling if a parent token is already canceled.
        if self.is_cancelled:
            return

        with self._cv:
            self._is_cancelled_int = True
            self._cv.notify_all()

    def set(self) -> None:
        """
        Deprecated, use ``cancel`` instead. This will be removed in the next major release.

        Cancel the token, notifying any waiting threads.
        """
        self.cancel()

    def wait(self, timeout: Optional[float] = None) -> bool:
        while not self.is_cancelled:
            with self._cv:
                did_not_time_out = self._cv.wait(timeout)
                if not did_not_time_out:
                    return False
        return True

    def create_child_token(self) -> "CancellationToken":
        child = CancellationToken(self._cv)
        child._parent = self
        return child

    def cancel_on_interrupt(self) -> None:
        """
        Register an interrupt handler to capture SIGINT (Ctrl-C) and cancel this token,
        instead of throwing a KeyboardInterrupt exception.
        """

        def sigint_handler(sig_num: int, frame: Any) -> None:
            logger = logging.getLogger(__name__)
            logger.warning("Interrupt signal received, stopping extractor gracefully")
            self.cancel()
            logger.info("Waiting for threads to complete. Send another interrupt to force quit.")
            signal.signal(signal.SIGINT, signal.default_int_handler)

        try:
            signal.signal(signal.SIGINT, sigint_handler)
        except ValueError as e:
            logging.getLogger(__name__).warning(f"Could not register handler for interrupt signals: {str(e)}")
