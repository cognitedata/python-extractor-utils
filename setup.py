import re

from setuptools import setup

version = re.search('^__version__\s*=\s*"(.*)"', open("cognite/extractors/configtools/__init__.py").read(), re.M).group(
    1
)

setup(
    name="cognite-configtools",
    # Version number:
    version=version,
    # Application author details:
    author="Mathias Lohne",
    author_email="mathias.lohne@cognite.com",
    packages=["cognite.extractors.configtools"],
    description="Utilities to read and verify config files.",
    install_requires=["cognite-sdk"],
    python_requires=">=3.5",
)
