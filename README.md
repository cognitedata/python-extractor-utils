<a href="https://cognite.com/">
    <img src="https://github.com/cognitedata/cognite-python-docs/blob/master/img/cognite_logo.png" alt="Cognite logo" title="Cognite" align="right" height="80" />
</a>

Python extractor-utils
=======================
[![build](https://webhooks.dev.cognite.ai/build/buildStatus/icon?job=github-builds/python-extractor-utils/master)](https://jenkins.cognite.ai/job/github-builds/job/python-extractor-utils/job/master/)
[![codecov](https://codecov.io/gh/cognitedata/python-extractor-utils/branch/master/graph/badge.svg?token=7AmVCpAh7I)](https://codecov.io/gh/cognitedata/python-extractor-utils)
[![PyPI version](https://badge.fury.io/py/cognite-extractor-utils.svg)](https://pypi.org/project/cognite-extractor-utils)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

A package containing convenient utilities for developing extractors in Python.

The package contains the following modules:

 * `configtools` containing utilities for automating config validation
 * `metrics` containing classes that spawn threads pushing Prometheus metrics to a Prometheus push gateway or to CDF as time series
 * `statestore` containing  classes for storing states from extractions between runs, either remotely or locally (todo)
 * `uploader` containing upload queues for batching together items to be uploaded to CDF
 * `utils` containing some supplementing functions and classes

For complete documentation, build the sphinx project found in `docs`.

Note that for the time being, the package is in an unstable `0.x` version, and while breaking changes are discouraged they may occur. It is therefore reccomended to lock requirements down to the minor version in your `setup.py` / `Pipfile` / `pyproject.toml` until version `1.0.0` is released.
