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

"""
The ``util`` package contains miscellaneous functions and classes that can some times be useful while developing
extractors.
"""
import logging
import signal
from threading import Event
from typing import Any, Dict, Iterable, Union

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.exceptions import CogniteNotFoundError


def ensure_time_series(cdf_client: CogniteClient, time_series: Iterable[TimeSeries]) -> None:
    """
    Ensure that all the given time series exists in CDF.

    Searches through the tenant after the external IDs of the time series given, and creates those that are missing.

    Args:
        cdf_client: Tenant to create time series in
        time_series: Time series to create
    """
    try:
        external_ids = [ts.external_id for ts in time_series]

        # Not doing anything with the result, only want to make sure they exist. This will throw an exception if not.
        cdf_client.time_series.retrieve_multiple(external_ids=external_ids)

    except CogniteNotFoundError as e:
        # Create the missing time series
        external_ids = [obj["externalId"] for obj in e.not_found]

        create_these = [ts for ts in time_series if ts.external_id in external_ids]
        cdf_client.time_series.create(create_these)


def set_event_on_interrupt(stop_event: Event) -> None:
    """
    Set given event on SIGINT (Ctrl-C) instead of throwing a KeyboardInterrupt exception.

    Args:
        stop_event: Event to set
    """

    def sigint_handler(sig, frame):
        logger = logging.getLogger(__name__)
        logger.warning("Interrupt signal received, stopping extractor gracefully")
        stop_event.set()
        logger.info("Waiting for threads to complete")

    signal.signal(signal.SIGINT, sigint_handler)


class EitherId:
    """
    Class representing an ID in CDF, which can either be an external or internal ID. An EitherId can only hold one ID
    type, not both.

    Args:
        id: Internal ID
        externalId or external_id: external ID

    Raises:
        TypeError: If none of both of id types are set.
    """

    def __init__(self, **kwargs):
        internal_id = kwargs.get("id")
        external_id = kwargs.get("externalId") or kwargs.get("external_id")

        if internal_id is None and external_id is None:
            raise TypeError("Either id or external_id must be set")

        if internal_id is not None and external_id is not None:
            raise TypeError("Only one of id and external_id can be set")

        self.internal_id = internal_id
        self.external_id = external_id

    def type(self) -> str:
        """
        Get the type of the ID

        Returns:
            'id' if the EitherId represents an internal ID, 'externalId' if the EitherId represents an external ID
        """
        return "id" if self.internal_id is not None else "externalId"

    def content(self) -> Union[int, str]:
        """
        Get the value of the ID

        Returns:
            The ID
        """
        return self.internal_id or self.external_id

    def __eq__(self, other: Any) -> bool:
        """
        Compare with another object. Only returns true if other is an EitherId with the same type and content

        Args:
            other: Another object

        Returns:
            True if the other object is equal to this
        """
        if not isinstance(other, EitherId):
            return False

        return self.internal_id == other.internal_id and self.external_id == other.external_id

    def __hash__(self) -> int:
        """
        Returns a hash of the internal or external ID

        Returns:
            Hash code of ID
        """
        return hash((self.internal_id, self.external_id))

    def __str__(self) -> str:
        """
        Get a string representation of the EitherId on the format "type: content".

        Returns:
            A string rep of the EitherId
        """
        return "{}: {}".format(self.type(), self.content())

    def __repr__(self) -> str:
        """
        Get a string representation of the EitherId on the format "type: content".

        Returns:
            A string rep of the EitherId
        """
        return self.__str__()
