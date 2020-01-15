
Package reference
=================

.. toctree::


``configtools`` - Utilities for reading, parsing and validating config files
----------------------------------------------------------------------------

The ``configtools`` module exists of tools for loading and verifying config files for extractors.

Extractor configurasions are conventionally written in *hyphen-cased YAML*. These are typically loaded and serialized as *dataclasses* in Python.


Config loader
^^^^^^^^^^^^^
.. autofunction:: cognite.extractorutils.configtools.load_yaml


Base classes
^^^^^^^^^^^^
The ``configtools`` module contains several prebuilt config classes for many common parameters. The class ``BaseConfig`` is intended as a starting point for a custom configuration schema, containing parameters for config version, CDF connection and logging.

**Example:**

.. code-block:: python

    @dataclass
    class ExtractorConfig:
        ...

    @dataclass
    class SourceConfig:
        ...


    @dataclass
    class MyConfig(BaseConfig):
        extractor: ExtractorConfig
        source: SourceConfig


.. autoclass:: cognite.extractorutils.configtools.BaseConfig
    :undoc-members:
.. autoclass:: cognite.extractorutils.configtools.BaseWithMetricsConfig
    :undoc-members:
.. autoclass:: cognite.extractorutils.configtools.CogniteConfig
    :undoc-members:
.. autoclass:: cognite.extractorutils.configtools.LoggingConfig
    :undoc-members:
.. autoclass:: cognite.extractorutils.configtools.MetricsConfig
    :undoc-members:


Exceptions
^^^^^^^^^^

.. autoexception:: cognite.extractorutils.configtools.InvalidConfigError


``metrics`` - Automatic pushers of performance metrics
------------------------------------------------------

.. automodule:: cognite.extractorutils.metrics
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:


``statestore`` - Storing extractor state between runs locally or remotely
-------------------------------------------------------------------------

.. automodule:: cognite.extractorutils.statestore
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:


``uploader`` - Batching upload queues with automatic upload triggers
--------------------------------------------------------------------

.. automodule:: cognite.extractorutils.uploader
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:


``util`` - Miscellaneous utilities
----------------------------------

.. automodule:: cognite.extractorutils.util
    :members:
    :undoc-members:
    :inherited-members:
    :show-inheritance:

