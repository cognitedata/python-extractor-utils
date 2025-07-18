"""
Module that provides additional threading utilities.
"""

import logging
import signal
from threading import Condition
from time import time
from types import FrameType


class CancellationToken:
    """
    Abstraction for a hierarchical cancellation token.

    Using this you can create hierarchies of cancellation tokens, to cancel a part of the extractor
    without cancelling the whole process. Use ``create_child_token`` to create a token that will be
    cancelled if the parent is cancelled, but can be canceled alone without affecting the parent token.
    """

    def __init__(self, condition: Condition | None = None) -> None:
        self._cv: Condition = condition or Condition()
        self._is_cancelled_int: bool = False
        self._parent: CancellationToken | None = None

    def __repr__(self) -> str:
        """
        Return a string representation of the CancellationToken instance.
        """
        cls = self.__class__
        status = "cancelled" if self.is_cancelled else "not cancelled"
        return f"<{cls.__module__}.{cls.__qualname__} at {id(self):#x}: {status}>"

    @property
    def is_cancelled(self) -> bool:
        """
        ``True`` if the token has been cancelled, or if some parent token has been cancelled.
        """
        return self._is_cancelled_int or (self._parent is not None and self._parent.is_cancelled)

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

    def wait(self, timeout: float | None = None) -> bool:
        """
        Wait for the token to be cancelled, or until the timeout expires.

        This can also be used as a drop-in replacement for sleep if you want to wait for a certain amount of time. A
        call to sleep will not be interrupted by a cancellation, but a call to wait will return immediately if the token
        is cancelled.

        Args:
            timeout: The maximum time to wait in seconds. If None, wait indefinitely.

        Returns:
            ``True`` if the token was cancelled, ``False`` if the timeout expired before cancellation.
        """
        endtime = None
        if timeout is not None:
            endtime = time() + timeout

        while not self.is_cancelled:
            with self._cv:
                if endtime is not None:
                    remaining_time = endtime - time()
                    if remaining_time <= 0.0:
                        return True
                    timed_out = not self._cv.wait(remaining_time)
                    if timed_out:
                        return False
                else:
                    self._cv.wait()
        return True

    def create_child_token(self) -> "CancellationToken":
        """
        Create a child cancellation token of this token.

        The child token will be cancelled if this token is cancelled, but can also be cancelled independently.
        """
        child = CancellationToken(self._cv)
        child._parent = self
        return child

    def cancel_on_interrupt(self) -> None:
        """
        Register an interrupt handler to capture SIGINT (Ctrl-C) and cancel this token.

        This will set the cancellation token instead of throwing a KeyboardInterrupt exception.
        """

        def sigint_handler(sig_num: int, frame: FrameType | None) -> None:
            logger = logging.getLogger(__name__)
            logger.warning("Interrupt signal received, stopping extractor gracefully")
            self.cancel()
            logger.info("Waiting for threads to complete. Send another interrupt to force quit.")
            signal.signal(signal.SIGINT, signal.default_int_handler)

        try:
            signal.signal(signal.SIGINT, sigint_handler)
        except ValueError as e:
            logging.getLogger(__name__).warning(f"Could not register handler for interrupt signals: {e!s}")
