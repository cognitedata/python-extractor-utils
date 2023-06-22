<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite Python `extractor-utils`
================================
[![Build Status](https://github.com/cognitedata/python-extractor-utils/workflows/release/badge.svg)](https://github.com/cognitedata/python-extractor-utils/actions)
[![Documentation Status](https://readthedocs.com/projects/cognite-extractor-utils/badge/?version=latest&token=a9bab88214cbf624706005f6a71bbd77964efc910f8e527c7b3d75edc016397c)](https://cognite-extractor-utils.readthedocs-hosted.com/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/cognitedata/python-extractor-utils/branch/master/graph/badge.svg?token=7AmVCpAh7I)](https://codecov.io/gh/cognitedata/python-extractor-utils)
[![PyPI version](https://badge.fury.io/py/cognite-extractor-utils.svg)](https://pypi.org/project/cognite-extractor-utils)
![PyPI - Python Version](https://img.shields.io/pypi/pyversions/cognite-extractor-utils)
[![License](https://img.shields.io/github/license/cognitedata/python-extractor-utils)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

The `extractor-utils` package is an extension of the Cognite Python SDK intended to simplify the development of data
extractors or other integrations for Cognite Data Fusion.

Documentation is hosted [here](https://cognite-extractor-utils.readthedocs-hosted.com/en/latest/), including a
[quickstart tutorial](https://cognite-extractor-utils.readthedocs-hosted.com/en/latest/quickstart.html).

The changelog is found [here](./CHANGELOG.md).

## Overview

The best way to start a new extractor project is to use the `cogex` CLI. You can install that from PyPI:

``` bash
pip install cognite-extractor-manager
```

To initialize a new extractor project, run

``` bash
cogex init
```

in the directory you want your extractor project in. The `cogex` CLI will prompt you for some information about your
project, and then set up a poetry environment, git repository, commit hooks with type and style checks and load a
template.


### Extensions

Some source systems have a lot in common, such as RESTful APIs or systems exposing as MQTT. We therefore have extensions
to `extractor-utils` tailroed to these protocols. These can be found in separate packages:

 * [REST extension](https://github.com/cognitedata/python-extractor-utils-rest)
 * [MQTT extension](https://github.com/cognitedata/python-extractor-utils-mqtt)


## Contributing

The package is open source under the [Apache 2.0 license](./LICENSE), and contribtuions are welcome.

This project adheres to the [Contributor Covenant v2.0](https://www.contributor-covenant.org/version/2/0/code_of_conduct/)
as a code of conduct.


### Development environment

We use [poetry](https://python-poetry.org) to manage dependencies and to administrate virtual environments. To develop
`extractor-utils`, follow the following steps to set up your local environment:

 1. [Install poetry](https://python-poetry.org/docs/#installation) if you haven't already.

 2. Clone repository:
    ```
    $ git clone git@github.com:cognitedata/python-extractor-utils.git
    ```
 3. Move into the newly created local repository:
    ```
    $ cd python-extractor-utils
    ```
 4. Create virtual environment and install dependencies:
    ```
    $ poetry install
    ```


### Code requirements

All code must pass [black](https://github.com/ambv/black) and [isort](https://github.com/timothycrosley/isort) style
checks to be merged. It is recommended to install pre-commit hooks to ensure this locally before commiting code:

```
$ poetry run pre-commit install
```

Each public method, class and module should have docstrings. Docstrings are written in the [Google
style](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings). Please include unit and/or
integration tests for submitted code, and remember to update the [changelog](./CHANGELOG.md).
