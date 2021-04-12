.. quickstart:

Quickstart
==========


Installation
------------

To install this package:

.. code-block:: bash

   pip install cognite-extractor-utils

If the Cognite SDK is not already installed, the installation will automatically fetch and install it as well.


``cognite.extractorutils.uploader``
-----------------------------------

API requests can be expensive to perform, even with a well-performing backend. The solution is to batch toghether more
data into a single API request.

The uploader module contains upload queues. These will hold data until a configured condition is met, triggering an
upload. For example, instead of

.. code-block:: python

    client = CogniteClient()

    while not stop:
        timestamp, value = source.query()

        client.datapoints.insert((timestamp, value), external_id="my-timeseries")


which would do very many API requests to CDF, you could do

.. code-block:: python

    client = CogniteClient()
    upload_queue = TimeSeriesUploadQueue(cdf_client=client, max_upload_interval=1)

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

Similar queues exists for RAW rows and Events too.



``cognite.extractorutils.statestore``
-------------------------------------

A state store object is a dictionary from an external ID to a low and high watermark, used to keep track of the progress
of a front- or backfill between runs.

A state is a tuple, typically containing two timestamps (range of extracted data).

You can choose the back-end for your state store with which class you're instatiating:

.. code-block:: python

    # A state store using a JSON file as remote storage:
    states = LocalStateStore("state.json")
    states.initialize()

    # A state store using a RAW table as remote storage:
    states = RawStateStore(
        cdf_client = CogniteClient(),
        database = "extractor_states",
        table = "my_extractor_deployment"
    )
    states.initialize()


The ``initialize()`` method loads all the states from the configured remote store.

You can now use this state store to get states:

.. code-block:: python

    low, high = states.get_state(external_id = "my-id")

You can set states:

.. code-block:: python

    states.set_state(external_id = "another-id", high=100)

and similar for ``low``. The ``set_state(...)`` method will always overwrite the current state. Some times you might
want to only set state *if larger* than the previous state, in that case consider ``expand_state(...)``:

.. code-block:: python

    # High watermark of another-id is already 100, nothing happens in this call:
    states.expand_state(external_id = "another-id", high=50)

    # This will set high to 150 as it is larger than the previous state
    states.expand_state(external_id = "another-id", high=150)

To store the state to the remote store, use the ``synchronize()`` method:

.. code-block:: python

    states.synchronize()


Integrating with upload queues
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can set a state store to automatically update on upload triggers from an upload queue by using the
``post_upload_function`` in the upload queue:

.. code-block:: python

    states = LocalStateStore("state.json")
    states.initialize()

    uploader = TimeSeriesUploadQueue(
        cdf_client = CogniteClient(),
        max_upload_interval = 10
        post_upload_function = states.post_upload_handler()
    )

    # The state store is now updated automatically!

    states.synchronize()


``cognite.extractorutils.metrics``
----------------------------------

The metrics module contains a general, pre-built metrics collection, as well as tools to routinely
push metrics to a remote server.

The ``BaseMetrics`` class forms the basis for a metrics collection for an extractor, containing some general metrics
that all extractors should report (such as e.g. CPU and memory usage of the extractor). To create your own set of
metrics, subclass this class and populate it with extractor-specific metrics, as such:

.. code-block:: python

    class MyMetrics(BaseMetrics):
        def __init__(self):
            super().__init__(extractor_name="my_extractor", extractor_version=__version__)

            self.a_counter = Counter("my_extractor_example_counter", "An example counter")
            ...

The metrics module also contains some Pusher classes that are used to routinely send metrics to a
remote server, these can be automatically created with the ``start_pushers`` method described in
``configtools``.


``cognite.extractorutils.configtools``
--------------------------------------

The configtools contains base classes for configuration, and a YAML loader to automatically serialize these dataclasses
from a config file.

Configs are described as ``dataclass``\es, and use the ``BaseConfig`` class as a superclass to get a few things built-in:
config version, Cognite project and logging. Use type hints to specify types, use the ``Optional`` type to specify that
a config parameter is optional, and give the attribute a value to give it a default.

For example, a config class for an extractor may look like the following:

.. code-block:: python

    class ExtractorConfig:
        parallelism: int = 10

        state_store: Optional[StateStoreConfig]
        ...

    @dataclass
    class SourceConfig:
        host: str
        username: str
        password: str
        ...


    @dataclass
    class MyConfig(BaseConfig):
        extractor: ExtractorConfig
        source: SourceConfig


You can then load a YAML file into this dataclass with the `load_yaml` function:

.. code-block:: python

    with open("config.yaml") as infile:
        config = load_yaml(infile, MyConfig)


The config object
^^^^^^^^^^^^^^^^^

The config object can additionally do several things, such as:

Creating a ``CogniteClient`` based on the config:

.. code-block:: python

    client = config.cognite.get_cognite_client("my-client")

Setup the logging according to the config:

.. code-block:: python

    config.logger.setup_logging()

Start and stop threads to automatically push all the prometheus metrics in the default prometheus registry to the
configured push-gateways:

.. code-block:: python

    config.metrics.start_pushers(client)

    # Extractor code

    config.metrics.stop_pushers()

Get a state store object as configured:

.. code-block:: python

    states = config.extractor.state_store.create_state_store()

