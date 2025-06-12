"""
Module containing state stores for extractors.

The ``statestore`` module contains classes for keeping track of the extraction state of individual items, facilitating
incremental load and speeding up startup times.

At the beginning of a run the extractor typically calls the ``initialize`` method, which loads the states from the
remote store (which can either be a local JSON file or a table in CDF RAW), and during and/or at the end of a run, the
``synchronize`` method is called, which saves the current states to the remote store.

You can choose the back-end for your state store with which class you're instantiating:

.. code-block:: python

    # A state store using a JSON file as remote storage:
    states = LocalStateStore("state.json")
    states.initialize()

    # A state store using a RAW table as remote storage:
    states = RawStateStore(
        cdf_client = CogniteClient(),
        database = "extractor_states",
        table = "my_extractor_deployment"
    )
    states.initialize()

You can now use this state store to get states:

.. code-block:: python

    low, high = states.get_state(external_id = "my-id")

You can set states:

.. code-block:: python

    states.set_state(external_id = "another-id", high=100)

and similar for ``low``. The ``set_state(...)`` method will always overwrite the current state. Some times you might
want to only set state *if larger* than the previous state, in that case consider ``expand_state(...)``:

.. code-block:: python

    # High watermark of another-id is already 100, nothing happens in this call:
    states.expand_state(external_id = "another-id", high=50)

    # This will set high to 150 as it is larger than the previous state
    states.expand_state(external_id = "another-id", high=150)

To store the state to the remote store, use the ``synchronize()`` method:

.. code-block:: python

    states.synchronize()

You can set a state store to automatically update on upload triggers from an upload queue by using the
``post_upload_function`` in the upload queue:

.. code-block:: python

    states = LocalStateStore("state.json")
    states.initialize()

    uploader = TimeSeriesUploadQueue(
        cdf_client = CogniteClient(),
        max_upload_interval = 10
        post_upload_function = states.post_upload_handler()
    )

    # The state store is now updated automatically!

    states.synchronize()
"""

from .hashing import AbstractHashStateStore, LocalHashStateStore, RawHashStateStore
from .watermark import AbstractStateStore, LocalStateStore, NoStateStore, RawStateStore

__all__ = [
    "AbstractHashStateStore",
    "AbstractStateStore",
    "LocalHashStateStore",
    "LocalStateStore",
    "NoStateStore",
    "RawHashStateStore",
    "RawStateStore",
]
