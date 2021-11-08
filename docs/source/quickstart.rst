.. quickstart:

Quickstart
==========

This quickstart guide will take you through the steps of developing your very first extractor based on the Cognite
extractor-utils framework.

If you want to see other examples of extractors, see our `example extractor repository <https://github.com/cognitedata/python-extractor-example>`_


Starting a new extractor project
--------------------------------

The easiest way to set up a new extractor project is to use ``cogex``. To install it, run

.. code-block:: bash

    pip install cognite-extractor-manager

in a shell. To initialize a new extractor project, run

.. code-block:: bash

    cogex init

in the directory you want your extractor project in.

Running ``cogex init`` will first prompt you for some information about your extractor, and then set up a poetry
environment, git repository, commit hooks with type and style checks and load a template.

.. You can read more about ``cogex`` `in the project repository <https://github.com/cognitedata/python-extractor-manager>`_.



Developing your first extractor
-------------------------------

In this tutorial, we will go through the steps of creating a simple extractor that reads CSV files locally on your
machine and uploads them to CDF RAW.


Chapters

.. toctree::

    configs
    read_csv
    uploader
    package
