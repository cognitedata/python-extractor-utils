<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Cognite Python `extractor-utils`
================================
[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/python-extractor-utils/master)](https://jenkins.cognite.ai/job/github-builds/job/python-extractor-utils/job/master/)
[![codecov](https://codecov.io/gh/cognitedata/python-extractor-utils/branch/master/graph/badge.svg?token=7AmVCpAh7I)](https://codecov.io/gh/cognitedata/python-extractor-utils)
[![PyPI version](https://badge.fury.io/py/cognite-extractor-utils.svg)](https://pypi.org/project/cognite-extractor-utils)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

A package containing convenient utilities for developing extractors in Python.

__Important:__ Note that for the time being, the package is in an unstable `0.x` version, and while breaking changes are
discouraged they may occur. It is therefore reccomended to lock requirements down to the minor version in your
`setup.py` / `Pipfile` / `pyproject.toml` until version `1.0.0` is released.



### Contributing

We use [poetry](https://python-poetry.org) to manage dependencies and to administrate virtual environments. To develop
`extractor-utils`, follow the following steps to set up your local environment:

 1. Install poetry: (add `--user` if desirable)
    ```
    $ pip install poetry
    ```
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

All code must pass [black](https://github.com/ambv/black) and [isort](https://github.com/timothycrosley/isort) style
checks to be merged. It is recommended to install pre-commit hooks to ensure this locally before commiting code:

```
$ poetry run pre-commit install
```

This project adheres to the [Contributor Covenant v2.0](https://www.contributor-covenant.org/version/2/0/code_of_conduct/)
as a code of conduct.


### Library overview

The package contains the following modules:

 * `authentication` containing functionality for authenticating to CDF with Azure AD instead of API keys
 * `configtools` containing utilities for automating config validation
 * `metrics` containing classes that spawn threads pushing Prometheus metrics to a Prometheus push gateway or to CDF as
   time series
 * `statestore` containing  classes for storing states from extractions between runs, either remotely or locally (todo)
 * `uploader` containing upload queues for batching together items to be uploaded to CDF
 * `utils` containing some supplementing functions and classes

For complete reference documentation, build the sphinx project found in `docs`.


