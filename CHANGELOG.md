# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows
- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## [4.2.0]

### Added

 * Add support for certificate authentication

### Changed
 
 * Change minimum cognite-sdk version to 5.8

## [4.1.0]

### Changed

 * Update cognite-sdk version to 5.1.1

## [4.0.1]

### Fixed

 * Fixed a bug in the error handling for reporting heartbeats.

## [4.0.0]

### Added

 * Allow indexing state stores. You can now use the indexing notation to access
   values in a state store like you would do e.g. in a dictionary. Examples:

   ``` python
   states = LocalStateStore(...)

   # You can now set states like so:
   states["id"] = (None, 5)

   # Getting current states:
   low, high = states["another_id"]

   # You can also check if an entry has a stored state with the 'in' operator:
   if "new_id" not in states:
       # do something you only do the first time you process an item
   ```

### Fixed

 * Fixed a typo throughout the library, `cancelation_token` is now called
   `cancellation_token` everywhere. If you ever e.g. specify a cancellation
   token in a keyword argument, make sure to update the spelling.

### Changed

 * Allow any interval to be configured as a string in addition to integers, so
   e.g. upload intervals can be configured as

   ```yaml
   upload-interval: 2m
   ```

   with `s`/`m`/`h`/`d` being valid units, and `s` being implied when units are
   missing (to preserve backwards compatibility with old config files). If your
   extractor reads from these config fields you need to update to read the
   `seconds` (or the computed `minutes`, `hours` or `days` ) attribute instead
   of the fields directly, for example:

   ```python
   RawUploadQueue(
       cognite_client,
       max_upload_interval=config.upload_interval,
   )
   ```

   must be changed to

   ```python
   RawUploadQueue(
       cognite_client,
       max_upload_interval=config.upload_interval.seconds,
   )
   ```

### Migration guide

In order to update from version 3 to version 4, you need to:

 * Change `cancelation_token` to `cancellation_token` every time you pass one as
   a keyword argument (or read from an attribute)
 * Access the `seconds` attribute from any time values you read from the default
   config (such as `upload_interval`s or `timeout`s)
 * Consider updating any time value you have in your own config parameters to be
   of `TimeIntervalConfig` instead of `int` to allow users the option of
   configuring time values in a human-readable format.

A type checker, like mypy, will be able to detect any breaking changes this
update introduces. We highly reccomend scanning your code with a type checker.

Updating from version 3 to 4 should not introduce breaking changes to your
extractor.

## [3.2.0]

### Added

 * Decorator which adds extraction pipeline functionality

## [3.1.4]

### Fixed
 
 * Correctly catch errors when reloading remote configuration files

## [3.1.1]

### Fixed

 * Support running extractor utils inside Cognite Functions

## [3.1.0]

### Added

 * Support for exposing prometheus metrics on a local port instead of pushing to pushgateway

## [3.0.2]

### Fixed

 * Remove old version warning from the cognite SDK.

## [3.0.1]

### Fixed
 
 * Correctly do not request the experimental SDK  when using remote configuration files

## [3.0.0]

### Added

 * `SequenceUploadQueue`'s `create_missing` funtionality can now be used to set
   Name and Description values on newly created sequences.
 * Remote configuration files is now fully released and supported without using the experimental SDK.

### Fixed

 * Dataset and linked asset information are correctly set on created sequences.
 * Type hint for sequence column definitions updated to be more consistent.

## [3.0.0-beta4]

### Added

 * Option to set data set id when creating missing time series in the time series upload queue.

## [3.0.0-beta2]

### Changed

 * Update cognite-sdk to version 4.0.1, which removes the support for reserved environment variables such as `COGNITE_API_KEY` and `COGNITE_CLIENT_ID`.

## [3.0.0-beta]

### Removed

 * Python 3.7 support

Preview release of remote configuration of extractors. This allows users of your
extractors to configure their extractors from CDF.

### Migration guide

In this section we will go through how you can start using remote configuration
in your extractors.


#### Loading configs on startup

If you have based your extractor on the `Extractor` base class, remote
configuration is already implemented for your extractor without any need for
further changes to your code.

To include the feature, you must update `cognite-extractor-utils` to
`2.3.0-beta1`, and add a dependency to `cognite-sdk-experimental` version
`>=0.76.0`.

Otherwise, you must use the new `ConfigResolver` class in the `configtools`
module:

``` python
# With automatic CLI (ie, read config from command line args):
resolver = ConfigResolver.from_cli(
    name="my extractor",
    description="Short description of my extractor",
    version="1.2.3",
    config_type=MyConfig,
)
config: MyConfig = resolver.config


# With path to a yaml file:
resolver = ConfigResolver(
    config_path="/path/to/config.yaml",
    config_type=MyConfig
)
config: MyConfig = resolver.config
```

The resolver will automatically fetch configuration from CDF if remote
configuration is used, otherwise it will return the same as `load_yaml`.

#### Detecting config changes (only when using the base class)

When using the base class, you have the option to automatically detect new
config revisions, and do one of several predefined actions (keep in mind that
this is not exclusive to remote configs, if the extractor is running with a
local configuration that changes, it will do the same action). You specify which
with an `reload_config_action` enum. The enum can be one of the following values:

 * `DO_NOTHING` which is the __default__
 * `REPLACE_ATTRIBUTE` which will replace the `config` attribute on the object
   (keep in mind that if you are using the `run_handle` instead of subclassing,
   this will have no effect). Be also aware that anything that is set up based
   on the config (upload queues, cognite client objects, loggers, connections to
   source, etc) will __not__ change in this case.
 * `SHUTDOWN` will set the `cancelation_token` event, and wait for the extractor
   to shut down. It is then intended that the service layer running the
   extractor (ie, windows services, systemd, docker, etc) will be configured to
   always restart the service if it shuts down. This is the recomended approach
   for reloading configs, as it is always guaranteed that everything will be
   re-initialized according to the new configuration.
 * `CALLBACK` is similar to `REPLACE_ATTRIBUTE` with one difference. After
   replacing the `config` attribute on the extractor object, it will call the
   `reload_config_callback` method, which you will have to override in your
   subclass. This method should then do any necessary cleanup or
   re-initialization needed for your particular extractor.

To enable detection of config changes, set the `reload_config_action` argument
to the `Extractor` constructor to your chosen action:

```python
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
```

The extractor will then periodically check if the config file has changed. The
default interval is 5 minutes, you can change this by setting the
`reload_config_interval` attribute. As with any other interval in
extractor-utils, the unit is seconds.

#### Using remote configuration

When using remote configuration, you will still need to configure the extractor
with some minimal parameters - namely a CDF project, credentials for that
project, and an extraction pipeline ID to fetch configs from. There is also a
new global config field named `type` which is either `remote` or `local` (which
is the default).

An example for this minimal config follows:

``` yaml
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

```

The config file stored in CDF should omit all of these fields, as they will be
overwritten by the `ConfigResolver` to be the values given here. In other words,
for most extractor deployments, you should be able to leave out the `cognite`
field in the config stored in CDF.


## [2.2.0] - 2022-04-01

### Added

 * `.env` files will now be loaded if present at runtime
 * Check that a configured extraction pipeline actually exists, and report an
   appropriate error if not.

### Fixed

 * A few type hints in retry module were more restrictive than needed (such as 
   requiring `int` when `float` would work).
 * Gracefully handle wrongful data in state stores. If JSON parsing fails, use
   an empty state store as default.

### Changed

 * Exception messages for `InvalidConfigError`s have been improved, and when
   using the extractor base class it will print them in a formatted way instead
   of dumping a stack trace on invalid configs.


## [2.1.3] - 2022-03-07

### Fixed

 * Use Optional with defaults in code instead of dataclass defaults in
   `UploaderExtractorConfig`, as this allows non-default config sections in
   subclasses.


## [2.1.2] - 2022-03-07

### Fixed

 * Include defaults for queue sizes in `UploaderExtractor`


## [2.1.1] - 2022-03-04

### Changed

 * Allow wider ranges for certain dependencies
   - Allow `>=3.7.4, <5` for typing-extensions
   - Allow `>=5.3.0, <7` for PyYAML


## [2.1.0] - 2022-02-25

### Added
 
 * `uploader_extractor` and `uploader_types` modules used to create extractors
   writing to events, timeseries, or raw, by calling a common method.
   This is primarily used for the utils extensions, but can be useful for
   very simple extractors in general. 

### Fixed

 * Fixed a signature bug in `SequenceUploadQueue`'s `__enter__` method
   preventing it to be used as a context.


## [2.0.3] - 2022-02-08

### Fixed

 * Fixed an issue where the base class would not always load a default
   `LocalStateStore` if requested

## [2.0.2] - 2022-02-07

### Added

 * A `get_current_state_store` class method on the Extractor class which returns
   the most recent state store loaded


## [2.0.1] - 2022-02-01

### Fixed

 * Fixed retries to not block the GIL and respect the cancellation token
 
### Added

 * A `get_current_config` class method on the Extractor class which returns the
   most recent config file read


## [2.0.0] - 2022-01-24

### Added

 * Option to not handle `SIGINT`s gracefully
 * Configurable heartbeat interval

### Changed

 * Use `cognite-sdk-core` as base instead of `cognite-sdk`
 * Update Arrow to `>1.0.0`

### Migration guide

To update your projects to 2.0.0 there are two things to consider:

 * If you are specifying a version of the Cognite SDK to use in your dependency
   list, you must now specify `cognite-sdk-core` instead. Otherwise you might
   install both versions. E.g. If your `pyproject.toml` looks like this:

   ``` toml
   cognite-sdk = "^2.32.0"
   cognite-extractor-utils = "^1.3.3"
   ```

   You must also change the `cognite-sdk` dependency when updating
   `extractor-utils`:

   ``` toml
   cognite-sdk-core = "^2.32.0"
   cognite-extractor-utils = "^2.0.0"
   ```

 * If your extractor is using the included
   [`Arrow`](https://github.com/arrow-py/arrow) package, there are a few
   breaking changes (most notibly that the `timestamp` attribute is renamed to
   `int_timestamp`). Consult their [migration
   guide](https://github.com/arrow-py/arrow/issues/832) to make sure you are in
   compliance.


## [1.6.2] - 2022-01-22

### Changed

 * Allow environment variable substitution in bool type config fields without
   breaking generic environment variable substitution


## [1.6.1] - 2022-01-21

### Changed

 * Reverts 1.6.0 as it broke generic environment variable substitution


## [1.6.0] - 2022-01-21

### Fixed

 * Allow environment variable substitution in bool type config fields


## [1.5.4] - 2021-11-17

### Fixed

 *  Make Cognite SDK timeout configurable


## [1.5.3] - 2021-10-12

### Fixed

 * Add a base class for extractors to remove a lot of boilerplate code necesarry
   for startup/shutdown, initialization etc.


## [1.5.2] - 2021-09-27

### Fixed

 * Allow using `Enum`s in config classes


## [1.5.1] - 2021-09-24

### Added

 * An `ensure_assets` function similar to `ensure_timeseries`
 * A `BytesUploadQueue` that takes byte arrays directly instead of file paths
 * A `throttled_loop` generator that iterates at most every `n` seconds
 * An `EitherIdConfig` to configure e.g. data sets with either an id or external
   id in the same field.

### Changed

 * Never call `/login/status` as the endpoint is deprecated

### Fixed

 * Inlcude missing classes and modules in docs

### Deprecated

 * Using `dataset-id` or `dataset-external-id` fields, use the new
   `EitherIdConfig` instead.


## [1.5.0] - 2021-08-12

### Added

 * Add a base class for extractors to remove a lot of boilerplate code necesarry
   for startup/shutdown, initialization etc.


## [1.4.2] - 2021-06-01

### Fixed

 * Fix an issue in `SequenceUploadQueue.add_to_upload_queue` when adding
   multiple rows to the same id


## [1.4.1] - 2021-05-03

### Changed

 * Changed bucket sizes for observed times in uploader metrics to be more suited
   for expected values.


## [1.4.0] - 2021-04-13

### Added

 * Option to provide additional custum args to token fetching (via the
   `token_custom_args` arg to the `CogniteClient` constructor)


### Changed

 * Use token fetching from Cognite SDK instead of our own implementation


### Deprecated

 * `Authenticator` class


## [1.3.4] - 2021-03-31

### Added

 * Add config parameter to enable metrics of log messages. It reveals how many
   logging events happened per logger and log-level.


## [1.3.3] - 2021-03-30

### Changed

 * Reduce cardinality (by reducing label count) on autogenerated metrics. E.g.
   don't label `TIMESERIES_UPLOADER_POINTS_WRITTEN` with which time series it's
   writing to.


## [1.3.2] - 2021-03-29

### Added

 * Add option to specify external IDs for data sets


## [1.3.1] - 2021-03-22

### Fixed

 * Fix labels for generated Prometheus


## [1.3.0] - 2021-03-15

### Fixed

 * Add a cancellation token to all data uploaders and metrics pushers
 * Add various automatic Prometheus metrics for data uploaders


## [1.2.4] - 2021-03-02

### Fixed

 * Fix URL creation in OAuth flow


## [1.2.3] - 2021-01-07

### Fixed

 * Fix a pool issue in FileUploadQueue


## [1.2.2] - 2020-12-07

### Added

 * General OIDC token support


## [1.2.1] - 2020-10-20

### Added

 * Add option to authenticate to CDF with AAD


## [1.2.0] - 2020-09-16

### Added

 * Add upload queues for sequences and files

### Fixed

 * TimeSeriesUploadQueue can now auto-create string time series


## [1.1.1] - 2020-08-27

### Added

 * Add option to create missing time series from upload queue

### Fix

 * Fixed an issue in EitherId where the `repr` method didn't return a string (as
   would be expected).


## [1.1.0] - 2020-08-25

### Added

 * Add py.typed file so mypy knows that package is typed


## [1.0.2] - 2020-06-02

### Fixed

 * Don't require auth for prometheus push gateways as push gateways can be
   configured to allow unauthorized access.
 * Fixed a bug where the state store config would not allow raw state stores
   without explicit `null` on local


## [1.0.1] - 2020-05-25

### Added

 * An outside_state method to test if a new proposed state in state stores is
   covered or not
 * A general SIGINT handler
 * Several minor additions to configtools: Defaults in StateStoreConfig,
   optional dataset ID in CogniteConfig, option to have version as int or None
 * Add a metrics factory that caches instances

### Fixed

 * A concurrency issue with TimeSeriesUploadQueue where uploads could fail if
   points were added at the very start of the upload call
 * Fix documentation build


## [1.0.0] - 2020-05-15

Release the first stable version. Open source the library under the Apache 2.0
license
