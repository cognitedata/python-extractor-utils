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
Module containing tools for loading and verifying config files, and a YAML loader to automatically serialize these
dataclasses from a config file.

Configs are described as ``dataclass``\es, and use the ``BaseConfig`` class as a superclass to get a few things
built-in: config version, Cognite project and logging. Use type hints to specify types, use the ``Optional`` type to
specify that a config parameter is optional, and give the attribute a value to give it a default.

For example, a config class for an extractor may look like the following:

.. code-block:: python

    @dataclass
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
        config: MyConfig = load_yaml(infile, MyConfig)

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

However, all of these things will be automatically done for you if you are using the base Extractor class.
"""

from cognite.extractorutils.exceptions import InvalidConfigError

from .elements import (
    AuthenticatorConfig,
    BaseConfig,
    CastableInt,
    CertificateConfig,
    CogniteConfig,
    ConfigType,
    ConnectionConfig,
    EitherIdConfig,
    FileSizeConfig,
    LocalStateStoreConfig,
    LoggingConfig,
    MetricsConfig,
    PortNumber,
    RawDestinationConfig,
    RawStateStoreConfig,
    StateStoreConfig,
    TimeIntervalConfig,
)
from .loaders import ConfigResolver, KeyVaultAuthenticationMethod, KeyVaultLoader, load_yaml, load_yaml_dict
