import re

from setuptools import setup

version = re.search('^__version__\s*=\s*"(.*)"', open("cognite/extractorutils/__init__.py").read(), re.M).group(1)

setup(
    name="cognite-extractor-utils",
    # Version number:
    version=version,
    # Application author details:
    author="Mathias Lohne",
    author_email="mathias.lohne@cognite.com",
    packages=["cognite.extractorutils"],
    description="Utilities for use in extractors.",
    install_requires=["cognite-sdk==1.0.*", "typing", "google-cloud-pubsub==0.41.*", "prometheus-client==0.7.*"],
    python_requires=">=3.5",
)
