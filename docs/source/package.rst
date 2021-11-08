.. configs:

Packaging the final extractor
=============================

The final step is to ship our extractor. Some times we can depend on a Python installation the environment we will run
our extractor in, in other cases out extractor needs to be fully self-contained. How we ship our extractor will differ
slightly between those scenarios.

When developing the extractor, running it is fairly easy:

.. code-block:: bash

    potery run <extractor_name>

Sometimes we could just send our project to the production environment like this, for example by cloning the git repo
(which would also make updating to future versions very easy). However, when shipping the extractor to a production
environment, sending the entire development environment is not always an option. We may therefore need a way to pacakge
our extractor.


Python is available at production environment
---------------------------------------------

If we have a working and compatable version of Python available at our production environment, we can ship our extractor
as a `Python wheel file <https://www.python.org/dev/peps/pep-0427/>`_. Wheels are the standard way of shipping Python
packages.

To make a wheel of our extractor, run

.. code-block:: bash

    poetry build

from the directory of the extractor project. In the newly created ``dist`` folder, you will find a ``.whl`` file similar
to ``csv_extractor-1.0.0-py3-none-any.whl``. This is the file you would send to your production environment. There, you
run

.. code-block:: bash

    pip install csv_extractor-1.0.0-py3-none-any.whl

Our extractor is then runnable from the shell directly:

.. code-block:: bash

    csv_extractor <config-file>

Replace ``csv_extractor`` with the name of your specific extractor, and ``<config-file>`` with the (absolute or
relative) path to a config file.


Python is not available at production environment
-------------------------------------------------

If Python is not available, we have to make a completely self-contained executable of the extractor. We can use
`PyInstaller <https://www.pyinstaller.org/>`_ for this, however ``cogex`` contains functionality that wraps around
PyInstaller with suitable defaults and configuration to ease the process further. To build a self-contained executable
of the extractor, run

.. code-block:: bash

    cogex build

from the directory of the extractor project. In the newly created ``dist`` folder, you will find an executable for your
system. Note that PyInstaller only builds executables for the OS you are building from. So if you want a Windows
executable you have to run ``cogex build`` from Windows, likewise for Linux or Mac OS.

The resulting executable will contain your extractor, all dependencies [#]_ and the Python runtime.


.. [#] Note that this could be in violation of some licenses, particularly GPL or it's derivatives. Make sure that the
   licenses of your dependencies allows this type of linking.
