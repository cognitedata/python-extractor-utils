.. remoteconfigs:

Remote configuration
====================

If you are using the base :meth:`Extractor <cognite.extractorutils.Extractor>` class, you can automatically fetch configuration from the extraction pipelines API in CDF.
To use this, all you need to do is create a minimal configuration file like this:

.. code-block:: yaml

    type: remote

    cognite:
        # Read these from environment variables
        host: ${COGNITE_BASE_URL}
        project: ${COGNITE_PROJECT}

        idp-authentication:
            token-url: ${COGNITE_TOKEN_URL}
            client-id: ${COGNITE_CLIENT_ID}
            secret: ${COGNITE_CLIENT_SECRET}
            scopes:
                - ${COGNITE_BASE_URL}/.default

        extraction-pipeline:
            external-id: my-extraction-pipeline

containing only the ``cognite`` section, and the ``type: remote`` flag, and the configuration will be loaded dynamically from extraction pipelines.

You can, for example, use the `upload config github action <https://github.com/cognitedata/upload-config-action>`_ to publish configuration files from a github repository.
Remote configuration files are combined with local configuration and loaded into the extractor on startup.

Detecting config changes
------------------------

When using the base class, you have the option to automatically detect new
config revisions, and do one of several predefined actions (keep in mind that
this is not exclusive to remote configs, if the extractor is running with a
local configuration that changes, it will do the same action). You specify which
with an ``reload_config_action`` enum. The enum can be one of the following values:

* ``DO_NOTHING`` which is the default
* ``REPLACE_ATTRIBUTE`` which will replace the ``config`` attribute on the object
  (keep in mind that if you are using the ``run_handle`` instead of subclassing,
  this will have no effect). Be also aware that anything that is set up based
  on the config (upload queues, cognite client objects, loggers, connections to
  source, etc) will not change in this case.
* ``SHUTDOWN`` will set the ``cancellation_token`` event, and wait for the extractor
  to shut down. It is then intended that the service layer running the
  extractor (ie, windows services, systemd, docker, etc) will be configured to
  always restart the service if it shuts down. This is the recomended approach
  for reloading configs, as it is always guaranteed that everything will be
  re-initialized according to the new configuration.
* ``CALLBACK`` is similar to ``REPLACE_ATTRIBUTE`` with one difference. After
  replacing the ``config`` attribute on the extractor object, it will call the
  ``reload_config_callback`` method, which you will have to override in your
  subclass. This method should then do any necessary cleanup or
  re-initialization needed for your particular extractor.

To enable detection of config changes, set the ``reload_config_action`` argument
to the ``Extractor`` constructor to your chosen action:

.. code-block:: python

    # Using run handle:
    with Extractor(
        name="my_extractor",
        description="Short description of my extractor",
        config_class=MyConfig,
        version="1.2.3",
        run_handle=run_extractor,
        reload_config_action=ReloadConfigAction.SHUTDOWN,
    ) as extractor:
        extractor.run()

    # Using subclass:
    class MyExtractor(Extractor):
        def __init__(self):
            super().__init__(
                name="my_extractor",
                description="Short description of my extractor",
                config_class=MyConfig,
                version="1.2.3",
                reload_config_action=ReloadConfigAction.SHUTDOWN,
            )

The extractor will then periodically check if the config file has changed. The
default interval is 5 minutes, you can change this by setting the
``reload_config_interval`` attribute. As with any other interval in
extractor-utils, the unit is seconds.
