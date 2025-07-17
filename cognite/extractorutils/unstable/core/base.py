"""
This module provides the base class for extractors.

It includes functionality for task management, logging, error handling, and configuration management.

Extractors should subclass the `Extractor` class and implement the `__init_tasks__` method to define their tasks.
The subclass should also define several class attributes:
- ``NAME``: A human-readable name for the extractor.
- ``EXTERNAL_ID``: A unique identifier for the extractor, used when reporting to CDF Integrations.
- ``DESCRIPTION``: A brief description of the extractor.
- ``VERSION``: The version of the extractor, used when reporting to CDF Integrations. This should follow semantic
   versioning.
- ``CONFIG_TYPE``: The type of the application configuration for the extractor, which should be a subclass of
  ``ExtractorConfig``. This should be the same class as the one used for the generic type parameter of the
  ``Extractor`` class.


.. code-block:: python

    class MyConfig(ExtractorConfig):
        parameter: str
        another_parameter: int
        schedule: ScheduleConfig

    class MyExtractor(Extractor[MyConfig]):
        NAME = "My Extractor"
        EXTERNAL_ID = "my-extractor"
        DESCRIPTION = "An example extractor"
        VERSION = "1.0.0"

        CONFIG_TYPE = MyConfig

        def __init_tasks__(self) -> None:
            self.add_task(
                ScheduledTask(
                    name="my_task",
                    description="An example task",
                    schedule=self.application_config.schedule,
                    target=self.my_task_function,
                )
            )

        def my_task_function(self, task_context: TaskContext) -> None:
            task_context.logger.info("Running my task")
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from logging.handlers import TimedRotatingFileHandler
from multiprocessing import Queue
from threading import RLock, Thread
from types import TracebackType
from typing import Any, Generic, Literal, TypeVar

from humps import pascalize
from typing_extensions import Self, assert_never

from cognite.client import CogniteClient
from cognite.extractorutils._inner_util import _resolve_log_level
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import (
    ConnectionConfig,
    ExtractorConfig,
    LogConsoleHandlerConfig,
    LogFileHandlerConfig,
)
from cognite.extractorutils.unstable.core._dto import Error as DtoError
from cognite.extractorutils.unstable.core._dto import TaskUpdate
from cognite.extractorutils.unstable.core._messaging import RuntimeMessage
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.unstable.core.logger import CogniteLogger
from cognite.extractorutils.unstable.core.restart_policy import WHEN_CONTINUOUS_TASKS_CRASHES, RestartPolicy
from cognite.extractorutils.unstable.core.tasks import ContinuousTask, ScheduledTask, StartupTask, Task, TaskContext
from cognite.extractorutils.unstable.scheduling import TaskScheduler
from cognite.extractorutils.util import now

__all__ = ["ConfigRevision", "ConfigType", "Extractor"]

ConfigType = TypeVar("ConfigType", bound=ExtractorConfig)
ConfigRevision = Literal["local"] | int


_T = TypeVar("_T", bound=ExtractorConfig)


class _NoOpCogniteClient:
    """A mock CogniteClient that performs no actions, for use in dry-run mode."""

    class _MockResponse:
        def __init__(self, url: str, current_config_revision: int | str, external_id: str) -> None:
            self._url = url
            self.current_config_revision = current_config_revision
            self.external_id = external_id

        def json(self) -> dict:
            if "integrations/checkin" in self._url or "/integrations/extractorinfo" in self._url:
                if self.current_config_revision == "local":
                    return {"externalId": self.external_id}
                elif isinstance(self.current_config_revision, int):
                    return {"externalId": self.external_id, "lastConfigRevision": self.current_config_revision}
            return {}

    def __init__(
        self,
        config: ConnectionConfig | None,
        current_config_revision: int | str,
        client_name: str,
        logger: logging.Logger,
    ) -> None:
        class MockSDKConfig:
            def __init__(self, project: str) -> None:
                self.project = project

        project_name = config.project if config else "dry-run-no-config"
        self.external_id = config.integration.external_id if config and config.integration else "dry-run-no-integration"
        self.config = MockSDKConfig(project_name)
        self.current_config_revision = current_config_revision
        # self._logger = logging.getLogger(__name__)
        self._logger = logger
        self._logger.info(f"CogniteClient is in no-op mode (dry-run). Client name: {client_name}")

    def post(self, url: str, json: dict, **kwargs: dict[str, Any]) -> _MockResponse:
        response = self._MockResponse(url, self.current_config_revision, self.external_id)
        self._logger.info(f"[DRY-RUN] SKIPPED POST to {url} with payload: {json}.")
        self._logger.info(f"[DRY-RUN] Response: {response.json()}")
        return response

    def get(self, url: str, **kwargs: dict[str, Any]) -> _MockResponse:
        response = self._MockResponse(url, self.current_config_revision, self.external_id)
        self._logger.info(f"[DRY-RUN] SKIPPED GET from {url}.")
        self._logger.info(f"[DRY-RUN] Response: {response.json()}")
        return response


class FullConfig(Generic[_T]):
    """
    A class that holds the full configuration for an extractor.

    This includes the connection configuration, application configuration, and which revision of the application
    configuration is currently active.
    """

    def __init__(
        self,
        application_config: _T,
        current_config_revision: ConfigRevision,
        connection_config: ConnectionConfig | None,
        log_level_override: str | None = None,
        is_dry_run: bool = False,
    ) -> None:
        self.application_config = application_config
        self.current_config_revision = current_config_revision
        self.connection_config = connection_config
        self.log_level_override = log_level_override
        self.is_dry_run = is_dry_run


class Extractor(Generic[ConfigType], CogniteLogger):
    """
    Base class for all extractors.

    This class provides the basic functionality for running an extractor, including task management, logging,
    error handling, and configuration management.

    It designed to be subclassed by specific extractors, which should implement the `__init_tasks__` method
    to define their tasks.
    """

    NAME: str
    EXTERNAL_ID: str
    DESCRIPTION: str
    VERSION: str

    CONFIG_TYPE: type[ConfigType]

    RESTART_POLICY: RestartPolicy = WHEN_CONTINUOUS_TASKS_CRASHES
    SUPPORTS_DRY_RUN: bool = False
    cognite_client: _NoOpCogniteClient | CogniteClient

    def __init__(self, config: FullConfig[ConfigType]) -> None:
        self.is_dry_run = config.is_dry_run
        if self.is_dry_run and not self.SUPPORTS_DRY_RUN:
            raise NotImplementedError(f"Extractor '{self.NAME}' does not support dry-run mode.")

        self._logger = logging.getLogger(f"{self.EXTERNAL_ID}.main")

        self.cancellation_token = CancellationToken()
        self.cancellation_token.cancel_on_interrupt()

        self.connection_config = config.connection_config
        self.application_config = config.application_config
        self.current_config_revision = config.current_config_revision
        self.log_level_override = config.log_level_override

        if self.is_dry_run:
            self.cognite_client = _NoOpCogniteClient(
                self.connection_config, self.current_config_revision, f"{self.EXTERNAL_ID}-{self.VERSION}", self._logger
            )
        elif self.connection_config:
            self.cognite_client = self.connection_config.get_cognite_client(f"{self.EXTERNAL_ID}-{self.VERSION}")
        else:
            raise ValueError("Connection config is missing and not in dry-run mode.")

        self._checkin_lock = RLock()
        self._runtime_messages: Queue[RuntimeMessage] | None = None

        self._scheduler = TaskScheduler(self.cancellation_token.create_child_token())

        self._tasks: list[Task] = []
        self._task_updates: list[TaskUpdate] = []
        self._errors: dict[str, Error] = {}

        self.__init_tasks__()

    def _setup_logging(self) -> None:
        if self.log_level_override:
            level_to_set = _resolve_log_level(self.log_level_override)
            # Use the override level if provided
            min_level = level_to_set
            max_level = level_to_set
        else:
            # Otherwise, use the levels from the config file
            min_level = min([_resolve_log_level(h.level.value) for h in self.application_config.log_handlers])
            max_level = max([_resolve_log_level(h.level.value) for h in self.application_config.log_handlers])

        root = logging.getLogger()
        root.setLevel(min_level)

        # The oathlib logs too much on debug level, including secrets
        logging.getLogger("requests_oauthlib.oauth2_session").setLevel(max(max_level, logging.INFO))

        fmt = logging.Formatter(
            "%(asctime)s.%(msecs)03d UTC [%(levelname)-8s] %(process)d %(threadName)s - %(message)s",
            "%Y-%m-%d %H:%M:%S",
        )
        # Set logging to UTC
        fmt.converter = time.gmtime

        # Remove any previous logging handlers
        for handler in root.handlers:
            root.removeHandler(handler)

        # Define new handlers
        for handler_config in self.application_config.log_handlers:
            match handler_config:
                case LogConsoleHandlerConfig() as console_handler:
                    sh = logging.StreamHandler()
                    sh.setFormatter(fmt)
                    level_for_handler = _resolve_log_level(
                        self.log_level_override if self.log_level_override else console_handler.level.value
                    )
                    sh.setLevel(level_for_handler)

                    root.addHandler(sh)

                case LogFileHandlerConfig() as file_handler:
                    fh = TimedRotatingFileHandler(
                        filename=file_handler.path,
                        when="midnight",
                        utc=True,
                        backupCount=file_handler.retention,
                    )
                    level_for_handler = _resolve_log_level(
                        self.log_level_override if self.log_level_override else file_handler.level.value
                    )
                    fh.setLevel(level_for_handler)
                    fh.setFormatter(fmt)

                    root.addHandler(fh)

    def __init_tasks__(self) -> None:
        """
        This method should be overridden by subclasses to define their tasks.

        It is called automatically when the extractor is initialized.

        Subclasses should call ``self.add_task(...)`` to add tasks to the extractor.
        """
        pass

    def _set_runtime_message_queue(self, queue: Queue) -> None:
        self._runtime_messages = queue

    def _checkin(self) -> None:
        if not self.connection_config:
            return

        with self._checkin_lock:
            task_updates = [t.model_dump() for t in self._task_updates]
            self._task_updates.clear()

            error_updates = [
                DtoError(
                    external_id=e.external_id,
                    level=e.level.value,
                    description=e.description,
                    details=e.details,
                    start_time=e.start_time,
                    end_time=e.end_time,
                    task=e._task_name if e._task_name is not None else None,
                ).model_dump()
                for e in self._errors.values()
            ]
            self._errors.clear()

        res = self.cognite_client.post(
            f"/api/v1/projects/{self.cognite_client.config.project}/integrations/checkin",
            json={
                "externalId": self.connection_config.integration.external_id,
                "taskEvents": task_updates,
                "errors": error_updates,
            },
            headers={"cdf-version": "alpha"},
        )
        new_config_revision = res.json().get("lastConfigRevision")

        if (
            new_config_revision
            and self.current_config_revision != "local"
            and new_config_revision > self.current_config_revision
        ):
            self.restart()

    def _run_checkin(self) -> None:
        while not self.cancellation_token.is_cancelled:
            try:
                self._logger.debug("Running checkin")
                self._checkin()
            except Exception:
                self._logger.exception("Error during checkin")
            self.cancellation_token.wait(10)

    def _report_error(self, error: Error) -> None:
        with self._checkin_lock:
            self._errors[error.external_id] = error

    def _new_error(
        self,
        level: ErrorLevel,
        description: str,
        *,
        details: str | None = None,
        task_name: str | None = None,
    ) -> Error:
        return Error(
            level=level,
            description=description,
            details=details,
            extractor=self,
            task_name=task_name,
        )

    def restart(self) -> None:
        """
        Trigger a restart of the extractor.
        """
        self._logger.info("Restarting extractor")
        if self._runtime_messages:
            self._runtime_messages.put(RuntimeMessage.RESTART)
        self.cancellation_token.cancel()

    @classmethod
    def _init_from_runtime(cls, config: FullConfig[ConfigType]) -> Self:
        return cls(config)

    def add_task(self, task: Task) -> None:
        """
        Add a task to the extractor.

        This method wraps the task's target function to include error handling and task tracking.

        Args:
            task: The task to add. It should be an instance of ``StartupTask``, ``ContinuousTask``, or ``ScheduledTask``
        """
        # Store this for later, since we'll override it with the wrapped version
        target = task.target

        def run_task(task_context: TaskContext) -> None:
            """
            A wrapped version of the task's target, with tracking and error handling.
            """
            # Record a task start
            with self._checkin_lock:
                self._task_updates.append(
                    TaskUpdate(type="started", name=task.name, timestamp=now()),
                )

            try:
                # Run task
                target(task_context)

            except Exception as e:
                # Task crashed, record it as a fatal error
                task_context.exception(
                    f"Task {task.name} crashed unexpectedly",
                    e,
                    level=ErrorLevel.fatal,
                )

                if self.__class__.RESTART_POLICY(task, e):
                    self.restart()

            finally:
                # Record task end
                with self._checkin_lock:
                    self._task_updates.append(
                        TaskUpdate(type="ended", name=task.name, timestamp=now()),
                    )

        task.target = run_task
        self._tasks.append(task)

        match task:
            case ScheduledTask() as t:
                self._scheduler.schedule_task(
                    name=t.name,
                    schedule=t.schedule,
                    task=lambda: t.target(
                        TaskContext(
                            task=task,
                            extractor=self,
                        )
                    ),
                )

    def _report_extractor_info(self) -> None:
        if not self.connection_config:
            return

        self.cognite_client.post(
            f"/api/v1/projects/{self.cognite_client.config.project}/integrations/extractorinfo",
            json={
                "externalId": self.connection_config.integration.external_id,
                "activeConfigRevision": self.current_config_revision,
                "extractor": {
                    "version": self.VERSION,
                    "externalId": self.EXTERNAL_ID,
                },
                "tasks": [
                    {
                        "name": t.name,
                        "type": "continuous" if isinstance(t, ContinuousTask) else "batch",
                        "action": bool(isinstance(t, ScheduledTask)),
                        "description": t.description,
                    }
                    for t in self._tasks
                ],
            },
            headers={"cdf-version": "alpha"},
        )

    def start(self) -> None:
        """
        Start the extractor.

        Instead of calling this method directly, it is recommended to use the context manager interface by using the
        ``with`` statement, which ensures proper cleanup on exit.
        """
        self._setup_logging()
        self._report_extractor_info()
        Thread(target=self._run_checkin, name="ExtractorCheckin", daemon=True).start()

    def stop(self) -> None:
        """
        Stop the extractor.

        Instead of calling this method directly, it is recommended to use the context manager interface by using the
        ``with`` statement, which ensures proper cleanup on exit.
        """
        self.cancellation_token.cancel()

    def __enter__(self) -> Self:
        """
        Start the extractor in a context manager.
        """
        self.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """
        Stop the extractor when exiting the context manager.
        """
        self.stop()
        with self._checkin_lock:
            self._checkin()

        self._logger.info("Shutting down extractor")
        return exc_val is None

    def run(self) -> None:
        """
        Run the extractor. This method starts the extractor and runs all tasks that have been added.

        This method assumes ``self.start()`` has been called first. The recommended way to use this method is
        to use the context manager interface, which ensures that the extractor is started and stopped properly.

        .. code-block:: python

            with extractor:
                extractor.run()
        """
        has_scheduled = False

        startup: list[StartupTask] = []
        continuous: list[ContinuousTask] = []

        for task in self._tasks:
            match task:
                case ScheduledTask():
                    has_scheduled = True

                case StartupTask() as t:
                    startup.append(t)

                case ContinuousTask() as t:
                    continuous.append(t)

                case _:
                    assert_never(task)

        self._logger.info("Starting extractor")
        if startup:
            with ThreadPoolExecutor() as pool:
                for task in startup:
                    pool.submit(
                        partial(
                            task.target,
                            TaskContext(
                                task=task,
                                extractor=self,
                            ),
                        )
                    )
        self._logger.info("Startup done")

        for task in continuous:
            Thread(
                name=pascalize(task.name),
                target=task.target,
                args=(TaskContext(task=task, extractor=self),),
            ).start()

        if has_scheduled:
            self._scheduler.run()

        else:
            self.cancellation_token.wait()
