#! /bin/bash
set -e
pipenv run python -m pytest -v --show-capture=all --cov=cognite.configtools
