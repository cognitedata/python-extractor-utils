#! /bin/bash
set -e
pipenv run python -m pytest --show-capture=all --cov=cognite_dict_validator
