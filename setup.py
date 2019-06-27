from setuptools import setup

from cognite.extractors.configtools import __version__ as version

setup(
    name="cognite-configtools",

    # Version number:
    version=version,

    # Application author details:
    author="Mathias Lohne",
    author_email="mathias.lohne@cognite.com",

    packages=["cognite.extractors.configtools"],
    description="Utilities to read and verify config files.",
)
