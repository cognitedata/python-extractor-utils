#! /bin/bash
set -e
pipenv run python -m pytest -v --show-capture=all --cov-report=term-missing --cov=cognite.extractorutils
