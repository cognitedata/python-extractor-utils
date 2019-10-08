import re

from setuptools import setup

version = re.search('^__version__\s*=\s*"(.*)"', open("cognite/extractorutils/__init__.py").read(), re.M).group(1)

setup(
    name="cognite-extractor-utils",
    version=version,
    description="Utilities for easier development of extractors for CDF",
    author="Mathias Lohne",
    author_email="mathias.lohne@cognite.com",
    packages=["cognite.extractorutils"],
    install_requires=["cognite-sdk>=1.0.0", "typing", "google-cloud-pubsub", "prometheus-client"],
    python_requires=">=3.5",
)
