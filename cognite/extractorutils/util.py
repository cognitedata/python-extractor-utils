from typing import Iterable

from cognite.client import CogniteClient
from cognite.client.data_classes import TimeSeries
from cognite.client.exceptions import CogniteNotFoundError


def ensure_time_series(cdf_client: CogniteClient, time_series: Iterable[TimeSeries]):
    """
    Ensure that all the given time series exists in CDF.

    Searches through the tenant after the external IDs of the time series given, and creates those that are missing.

    Args:
        cdf_client (CogniteClient): Tenant to create time series in
        time_series (iterable): Time series to create
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
