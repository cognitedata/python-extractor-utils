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

A package containing convenient utilities for developing extractors in Python.

Documentation is hosted [here](https://cognite-extractor-utils.readthedocs-hosted.com/en/latest/).


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

