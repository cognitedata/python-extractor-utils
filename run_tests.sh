#! /bin/bash
set -e
poetry run python -m pytest -v --show-capture=all --cov-report=term-missing --cov=cognite.extractorutils
