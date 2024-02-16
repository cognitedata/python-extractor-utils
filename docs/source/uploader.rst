.. uploader:

Uploading data to CDF
=====================

In the previous chapter we introduced an upload queue, in this chapter we will look more closely at these. We will also
look at using the base :meth:`Extractor <cognite.extractorutils.Extractor>` class which will, among other things,
automate the reporting of extractor runs to `the extraction pipelines feature
<https://docs.cognite.com/cdf/integration/guides/interfaces/about_integrations.html>`_ in CDF.


Using an upload queue
---------------------

We begin by looking at the upload queue. Since this extractor will write to CDF RAW, we will use the
:meth:`RawUploadQueue <cognite.extractorutils.uploader.RawUploadQueue>`. Similar queues exists for time series data
points, events, sequence rows and files.

The reason for using an upload queue is to batch together data into larger requests to CDF. This will increase
performance, some times quite dramatically since network latencies can often be a bottleneck for extractors. We can add
elements (in our case, RAW rows) to the queue, and when we trigger an upload, all the data in the queue will be merged
into as big requests as possible. If there is too much data in the queue for a single request, the queue will
automatically split it into several requests and perform them in parallel, further increasing performance.

Instead of triggering these uploads ourselves, the reccomended way to use an upload queue is to use it as a context
manager (with a ``with`` statement). The advantage is that we can then set one or more criteria for when uploads should
happen (such as every *x* seconds or when the queue has *y* elements in it), and don't think about it again. If the
queue is not empty when we exit the context, a final upload will be made to make sure no data is left behind.

To create a :meth:`RawUploadQueue <cognite.extractorutils.uploader.RawUploadQueue>`, we write therefore start with

.. code-block:: python

    with RawUploadQueue(cdf_client=cognite, max_queue_size=100_000) as queue:

This will make an upload queue that uploads every time we reach 100 000 rows. We could let that value be configurable if
we were worried about memory usage. The reason we chose 100 000 in this example is that one request to RAW can handle up
to 10 000 rows, so this would allow us to batch up and send 10 request in parallel. If data freshness is a concern, we
could set the ``max_upload_interval`` as well which would define a maximum time (in seconds) between uploads.


Tying it all together
---------------------

To finish our extractor, we will enter the ``extractor.py`` file created by ``cogex``, and fill in the ``run_extractor``
function. This function is called from the ``__main__.py`` file, and is provided with a set of arguments:

*  ``cognite`` is an initiated ``CogniteClient`` that is set up to to use the CDF project that the user configured in
   their config file.
*  ``states`` is a state store object, we will not cover these in this tutorial, but in short it allows us to keep track
   of extraction state between runs to avoid duplicate work
*  ``config`` is the config file the user have provided, which has been loaded and stored as an instance of the Config
   class we made in the :ref:`Read CSV files` chapter.
*  ``stop_event`` is an instance of the `CancellationToken class`_
   It will be set whenever the extractor is asked to stop (for example by a user
   sending an interrupt signal), or you can set it yourself when a stopping condition is met. In our case, we will check
   the status of this stop event in our main loop before submitting a file for extraction. This way, when a user e.g.
   hits CTRL+C to stop the extractor, it will finish what it was doing, upload the final data to CDF, and then stop
   gracefully.

In the ``run_extractor`` function we will first create an upload queue, and then loop through all the files from the
config and hand it to the ``extract_file`` function from the :ref:`Read CSV files` chapter. Our function then looks like
this:

.. code-block:: python

    def run_extractor(cognite: CogniteClient, states: AbstractStateStore, config: Config, stop_event: Event):
        with RawUploadQueue(cdf_client=cognite, max_queue_size=100_000) as queue:
            for file in config.files:
                if stop_event.is_set():
                    break

                extract_file(file, queue)


Extraction pipeline runs
------------------------

Extraction pipelines are a way of monitoring the health of our extractor from CDF itself. You can read more about it
`here <https://docs.cognite.com/cdf/integration/guides/interfaces/about_integrations.html>`_.

Our extractor is already set up to use extraction pipelines, this is because of the use of the
:meth:`Extractor <cognite.extractorutils.Extractor>` base class in ``__main__.py``:

.. code-block:: python

    def main() -> None:
        with Extractor(
            name="csv_extractor",
            description="An extractor that takes CSV files and uploads their content to RAW",
            config_class=Config,
            run_handle=run_extractor,
            version=__version__,
        ) as extractor:
            extractor.run()

Since we are using the :meth:`Extractor <cognite.extractorutils.Extractor>` class as a context manager, it will detect
if an unhandled exception is thrown in the ``run_extractor`` function. If such an unhandled exception occurs, it will
report a new failed run. If the context manager exits cleanly, it will report a new successful run.

To enable reporting of runs, the user would simply have to include an ``extraction-pipeline`` field in the ``cognite``
section of the config file, containing either an ``external-id`` or (internal) ``id``.

.. code-block:: yaml

    cognite:
      project: publicdata
      api-key: ${COGNITE_API_KEY}

      extraction-pipeline:
        external-id: abc123
