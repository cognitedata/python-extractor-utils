"""
This module provides a task scheduler.

It is inspired by the ``APScheduler`` library and is designed to manage the scheduling of tasks within the extractor
framework. It differs from ``APScheduler`` in a few key ways:
- It is designed to be used within the extractor framework, allowing for better integration with the extractor's
  lifecycle and error handling. For example, it respects the extractor's cancellation token and will gracefully shut
  down upon cancellation.
- It has a simpler interface, focusing on the core functionality needed for scheduling tasks without the additional
  complexity of a full-featured scheduler like ``APScheduler``.
- It is fully typed, providing better type safety and autocompletion in IDEs.
"""

from ._scheduler import TaskScheduler

__all__ = ["TaskScheduler"]
