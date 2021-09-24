#  Copyright 2021 Cognite AS
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

"""
Module containing tools for throttling extractors to avoid overloading a source system
"""

import logging
from threading import Event
from time import time
from typing import Generator


def throttled_loop(target_time: int, cancelation_token: Event) -> Generator[None, None, None]:
    """
    A loop generator that automatically sleeps until each iteration has taken the desired amount of time. Useful for
    when you want to avoid overloading a source system with requests.

    Example:
        This example will throttle printing to only print every 10th second:

        .. code-block:: python

            for _ in throttled_loop(10, stop_event):
                print("Hello every 10 seconds!")

    Args:
        target_time: How long (in seconds) an iteration should take om total
        cancelation_token: An Event object that will act as the stop event. When set, the loop will stop.

    Returns:
        A generator that will only yield when the target iteration time is met
    """
    logger = logging.getLogger(__name__)

    while not cancelation_token.is_set():
        start_time = time()
        yield
        iteration_time = time() - start_time
        if iteration_time > target_time:
            logger.warning("Iteration time longer than target time, will not sleep")

        else:
            logger.debug(f"Iteration took {iteration_time:.1f} s, sleeping {target_time - iteration_time:.1f} s")
            cancelation_token.wait(target_time - iteration_time)
