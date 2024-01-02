#  Copyright 2023 Cognite AS
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
Module containing upload queue classes. The UploadQueue classes chunks together items and uploads them together to CDF,
both to minimize the load on the API, and also to speed up uploading as requests can be slow.

Each upload queue comes with some configurable conditions that, when met, automatically triggers an upload.

**Note:** You cannot assume that an element is uploaded when it is added to the queue, since the upload may be
delayed. To ensure that everything is uploaded you should set the `post_upload_function` callback to verify. For
example, for a time series queue you might want to check the latest time stamp, as such (assuming incremental time
stamps and using timestamp-value tuples as data point format):

You can create an upload queue manually like this:

.. code-block:: python

    queue = TimeSeriesUploadQueue(cdf_client=my_cognite_client)

and then call ``queue.upload()`` to upload all data in the queue to CDF. However you could set some upload conditions
and have the queue perform the uploads automatically, for example:

.. code-block:: python

    client = CogniteClient()
    upload_queue = TimeSeriesUploadQueue(cdf_client=client, max_upload_interval=10)

    upload_queue.start()

    while not stop:
        timestamp, value = source.query()
        upload_queue.add_to_upload_queue((timestamp, value), external_id="my-timeseries")

    upload_queue.stop()

The ``max_upload_interval`` specifies the maximum time (in seconds) between each API call. The upload method will be
called on ``stop()`` as well so no datapoints are lost. You can also use the queue as a context:

.. code-block:: python

    client = CogniteClient()

    with TimeSeriesUploadQueue(cdf_client=client, max_upload_interval=1) as upload_queue:
        while not stop:
            timestamp, value = source.query()

            upload_queue.add_to_upload_queue((timestamp, value), external_id="my-timeseries")

This will call the ``start()`` and ``stop()`` methods automatically.

You can also trigger uploads after a given amount of data is added, by using the ``max_queue_size`` keyword argument
instead. If both are used, the condition being met first will trigger the upload.
"""

from .assets import AssetUploadQueue
from .events import EventUploadQueue
from .files import BytesUploadQueue, FileUploadQueue, IOFileUploadQueue
from .raw import RawUploadQueue
from .time_series import (
    DataPoint,
    DataPointList,
    SequenceUploadQueue,
    TimeSeriesUploadQueue,
    default_time_series_factory,
)
