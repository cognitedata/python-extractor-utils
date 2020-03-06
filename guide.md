# configtools

The configtools contains base classes for configuration, and a YAML loader to automatically
serialize these dataclasses from a config file.

Configs are described as `dataclass`es, and use the `BaseConfig` class as a superclass to get a few
things built-in: config version, Cognite project and logging. Use type hints to specify types, use
the `Optional` type to specify that a config parameter is optional, and give the attribute a value
to give it a default.

For example, a config class for an extractor may look like the following:

``` python
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
```

You can then load a YAML file into this dataclass with the `load_yaml` function:

``` python
with open("config.yaml") as infile:
    config = load_yaml(infile, MyConfig)
```

The config object can do several things, like creating `CogniteClient`s based on the config:

``` python
client = config.cognite.get_cognite_client("my-client")
```

or setup the logging according to the config:

``` python
config.logger.setup_logging()
```

or start threads to automatically push all the prometheus metrics in the default prometheus registry
to the configured push-gateways:

``` python
config.metrics.start_pushers(client)

# Extractor code

config.metrics.stop_pushers()
```



# uploader

The uploader module contains upload queues. These are ment to batch together API requests to CDF to
make fewer requests. For example, instead of

``` python
client = CogniteClient()

while not stop:
    timestamp, value = source.query()

    client.datapoints.insert((timestamp, value), external_id="my-timeseries")
```

you could do

``` python
client = CogniteClient()
upload_queue = TimeSeriesUploadQueue(cdf_client=client, max_upload_interval=1)

upload_queue.start()

while not stop:
    timestamp, value = source.query()

    upload_queue.add_to_upload_queue((timestamp, value), external_id="my-timeseries")

upload_queue.stop()
```

The `max_upload_interval` specifies the maximum time (in seconds) between each API call. The upload
method will be called on `stop()` as well so no datapoints are lost. You can also use the queue as
a context:

``` python
client = CogniteClient()

with TimeSeriesUploadQueue(cdf_client=client, max_upload_interval=1) as upload_queue:
    while not stop:
        timestamp, value = source.query()

        upload_queue.add_to_upload_queue((timestamp, value), external_id="my-timeseries")
```

This will call the `start()` and `stop()` methods automatically.

Similar queues exists for RAW rows and Events too.


# statestore

A state store object is a dictionary from an external ID to a high and low watermark, used to keep
track of the progress of a front- or backfill between runs.

Currently, two types of state stores are supported: a local JSON file, or a table in RAW.

If you have the `StateStoreConfig` in your config file, you can create a state store object
automatically based on the config:

``` python
# The client arg is only used for RAW state stores
state_store = config.extractor.state_store.create_state_store(client)
```

The `initialize` method gets the states from the remote state store, the `synchronize` method sends
the current states to the remote state store.


# metrics

The metrics module contains a general, pre-built metrics collection, as well as tools to routinely
push metrics to a remote server.

The `BaseMetrics` class contains some barebone metrics, and can be used as a super class:

``` python
class MyMetrics(BaseMetrics):
    def __init__(self):
        super(MyMetrics, self).__init__("db_extractor")

        # Define other specific metrics here
```

The `BaseMetrics` class with send process metrics (such as CPU and memory usage of the extractor),
and startup and finish times (finish time must be set manually).

The metrics module also contains some Pusher classes that are used to routinely send metrics to a
remote server, these can be automatically created with the `start_pushers` method described in
__configtools__.
