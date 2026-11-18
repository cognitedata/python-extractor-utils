# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`cognite-extractor-utils` is a Python utility library for building data extractors that load data into Cognite Data Fusion (CDF). It provides the scaffolding (config loading, state tracking, upload queuing, metrics, lifecycle management) so extractor authors only implement the domain-specific extraction logic.

## Commands

```bash
# Install dependencies
uv sync

# Run all tests with coverage
uv run pytest -v --show-capture=all --cov-report=term-missing --cov=cognite.extractorutils
# or
./run_tests.sh

# Run a single test file
uv run pytest tests/tests_unit/test_configtools.py -v

# Run a single test by name
uv run pytest tests/tests_unit/test_configtools.py::test_name -v

# Run only unstable module tests
uv run pytest tests/test_unstable/ -v

# Type checking
uv run mypy --non-interactive --install-types cognite

# Linting and formatting (pre-commit)
uv run pre-commit run --all
```

Integration tests (`tests/tests_integration/`) require CDF credentials via environment variables (`COGNITE_CLIENT_ID`, `COGNITE_PROJECT`, `COGNITE_BASE_URL`, `COGNITE_TOKEN_URL`, `COGNITE_CLIENT_SECRET`, `COGNITE_TOKEN_SCOPES`).

## Architecture

There are **two distinct extractor patterns** in this library: the stable API (`cognite/extractorutils/`) and the unstable/next-gen API (`cognite/extractorutils/unstable/`). They share some lower-level primitives (threading, statestore, metrics) but have separate config systems and extractor base classes.

### Stable Extractor Lifecycle (`cognite/extractorutils/base.py`)

`Extractor[ConfigT]` is a generic context manager. Extractor authors pass a `run_handle` function or subclass, provide a `ConfigT` dataclass, and implement their extraction loop inside the `with` block. On `__enter__` it parses CLI args, loads config via `dacite`, constructs `CogniteClient`, initializes the state store, and starts metrics. On `__exit__` it flushes state, stops metrics, and reports the pipeline run status.

`UploaderExtractor` in `uploader_extractor.py` is **deprecated** — use the base `Extractor` and instantiate upload queues manually instead.

### Stable Configuration (`cognite/extractorutils/configtools/`)

Config is YAML-loaded into typed Python **dataclasses** via `dacite`. The base config hierarchy is:

```
BaseConfig
  ├── CogniteConfig       — CDF project, auth (OIDC via AuthenticatorConfig), base URL
  ├── LoggingConfig       — log level, structured logging, file rotation
  ├── MetricsConfig       — Prometheus push gateway or CDF time series
  └── StateStoreConfig    — local JSON file or CDF RAW table
```

Config values support environment variable substitution (`${ENV_VAR}`) and the `!env` YAML tag for quoted strings. Azure Key Vault secrets are referenced with the `!keyvault secret-name` YAML tag (requires an `azure-keyvault` section in config). `ConfigResolver` additionally supports remote config fetched from CDF Files or RAW.

### Unstable Extractor Lifecycle (`cognite/extractorutils/unstable/core/`)

The unstable pattern separates connection config from application config and runs the extractor in a **subprocess** managed by `Runtime`. The pattern:

1. **`Extractor[ConfigT]`** (`core/base.py`) — subclass this; define class attributes `NAME`, `EXTERNAL_ID`, `DESCRIPTION`, `VERSION`, `CONFIG_TYPE`; implement `__init_tasks__` to register `ScheduledTask` / `IntervalTask` objects.
2. **`Runtime`** (`core/runtime.py`) — wraps the extractor class, provides CLI parsing, loads `ConnectionConfig` from a separate file, spawns the extractor in a child process, and handles restarts on failure.

```python
class MyExtractor(Extractor[MyConfig]):
    NAME = "My Extractor"
    EXTERNAL_ID = "my-extractor"
    DESCRIPTION = "..."
    VERSION = "1.0.0"
    CONFIG_TYPE = MyConfig

    def __init_tasks__(self) -> None:
        self.add_task(ScheduledTask(name="poll", schedule=self.application_config.schedule, target=self._poll))
```

### Unstable Configuration (`cognite/extractorutils/unstable/configuration/`)

Config uses **Pydantic** `ConfigModel` (not dataclasses). `ConfigModel` auto-generates kebab-case YAML aliases. `ConnectionConfig` handles CDF connectivity and is loaded from a separate file; `ExtractorConfig` holds application-specific config and is subclassed per extractor.

### Upload Queues (`cognite/extractorutils/uploader/`)

Each queue buffers items and flushes on a configurable size threshold or time interval in a background thread. They accept a `post_upload_function` callback for state synchronization after a successful upload. Queue types: `TimeSeriesUploadQueue`, `AssetUploadQueue`, `EventUploadQueue`, `FileUploadQueue`, `RawUploadQueue`.

### State Stores (`cognite/extractorutils/statestore/`)

Track high/low watermarks per `external_id` for incremental extraction. Two backends:
- `LocalStateStore` — JSON file on disk
- `RawStateStore` — CDF RAW table

Hash-based variants (`LocalHashStateStore`, `RawHashStateStore`) track previously seen items by hash for deduplication instead of watermarks.

### Metrics (`cognite/extractorutils/metrics.py`)

`BaseMetrics` wraps Prometheus client. Subclass it to add custom counters/gauges. The `Pusher` classes send metrics to a Prometheus push gateway or as CDF time series on a background thread.

### Threading (`cognite/extractorutils/threading.py`)

`CancellationToken` is a hierarchical cancellation primitive. The root token is wired to `SIGINT`/`SIGTERM`. Child tokens allow partial cancellation of subsystems. Upload queues and metrics pushers use tokens for clean shutdown.

## Code Conventions

- **Type hints required** on all functions (`disallow_untyped_defs = true` in mypy config)
- **Google-style docstrings** on all public classes and methods
- **Line length**: 120 characters
- **Ruff rules enforced**: A, E, F, I, T20, S, B, UP, DTZ, W, LOG, RUF, SIM, C4, PERF, FURB, D, ANN — run pre-commit before committing
- Tests use `unittest.mock` for mocking; integration tests use real CDF clients
- `conftest.py` cleans up Prometheus registries between tests via an `autouse` fixture to prevent metric re-registration errors
- Stable tests are in `tests/tests_unit/` and `tests/tests_integration/`; unstable module tests are in `tests/test_unstable/`
