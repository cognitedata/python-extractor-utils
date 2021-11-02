.. configs:

Defining a config schema
========================

We will start off by defining how we want our users to configure our extractor. ``cogex`` has already created a
``config.py`` file with a config class based on the :ref:`BaseConfig <Base Classes>` class from extractor-utils. We
need to extend this with a list of files the extractor should read from.

Config schemas are defined using `data classes <https://docs.python.org/3/library/dataclasses.html>`_ and `type hints
<https://docs.python.org/3/library/typing.html>`_. For each file we want to read we need to know

*  The *path* to the file. We can use ``str`` for this.
*  Which column in the CSV file to use as the *key* in RAW. We can use ``str`` for this too.
*  Which *RAW database and table* to write to. For this, we will use the pre-built :meth:`RawDestinationConfig <cognite.extractorutils.configtools.RawDestinationConfig>` from ``cognite.extractorutils.configtools``.

Our final config class for CSV files looks like the following:

.. code-block:: python

    @dataclass
    class FileConfig:
        path: str
        key_column: str
        destination: RawDestinationConfig

We now need to update the auto-generated ``Config`` class with a list of files to extract from:

.. code-block:: python

    @dataclass
    class Config(BaseConfig):
        extractor: ExtractorConfig = ExtractorConfig()
        files: List[FileConfig]

This means that our users can now configure the extractor like so:

.. code-block:: yaml

    # This comes from BaseConfig:
    cognite:
      project: publicdata
      api-key: ${COGNITE_API_KEY}

    logging:
      console:
        level: INFO
      file:
        path: "debug.log"
        level: DEBUG

    # This is from our additions:
    files:
      - path: "pumps.csv"
        key-column: serial_number
        destination:
          database: csv_assets
          table: pumps

      - path: "valves.csv"
        key-column: id
        destination:
          database: csv_assets
          table: valves
