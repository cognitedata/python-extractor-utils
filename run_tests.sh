#! /bin/bash
set -e
uv run pytest -v --show-capture=all --cov-report=term-missing --cov=cognite.extractorutils
