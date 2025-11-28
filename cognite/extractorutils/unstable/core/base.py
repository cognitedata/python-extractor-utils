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
from datetime import datetime, timezone
from functools import partial
from multiprocessing import Queue
from multiprocessing.synchronize import Event as MpEvent
from threading import RLock, Thread
from types import TracebackType
from typing import Generic, TypeVar

from humps import pascalize
from typing_extensions import Self, assert_never

from cognite.extractorutils._inner_util import _resolve_log_level
from cognite.extractorutils.metrics import BaseMetrics
from cognite.extractorutils.statestore import (
    AbstractStateStore,
    LocalStateStore,
    NoStateStore,
)
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import (
    ConfigRevision,
    ConfigType,
    ConnectionConfig,
    ExtractorConfig,
    LogConsoleHandlerConfig,
    LogFileHandlerConfig,
)
from cognite.extractorutils.unstable.core._dto import (
    CogniteModel,
    ExtractorInfo,
    StartupRequest,
    TaskType,
)
from cognite.extractorutils.unstable.core._dto import (
    Task as DtoTask,
)
from cognite.extractorutils.unstable.core._messaging import RuntimeMessage
from cognite.extractorutils.unstable.core.checkin_worker import CheckinWorker
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.unstable.core.logger import CogniteLogger, RobustFileHandler
from cognite.extractorutils.unstable.core.restart_policy import WHEN_CONTINUOUS_TASKS_CRASHES, RestartPolicy
from cognite.extractorutils.unstable.core.tasks import ContinuousTask, ScheduledTask, StartupTask, Task, TaskContext
from cognite.extractorutils.unstable.scheduling import TaskScheduler
from cognite.extractorutils.util import now

__all__ = [
    "CogniteModel",
    "ConfigRevision",
    "ConfigType",
    "Extractor",
]


_T = TypeVar("_T", bound=ExtractorConfig)


class FullConfig(Generic[_T]):
    """
    A class that holds the full configuration for an extractor.

    This includes the connection configuration, application configuration, and which revision of the application
    configuration is currently active.
    """

    def __init__(
        self,
        connection_config: ConnectionConfig,
        application_config: _T,
        current_config_revision: ConfigRevision,
        log_level_override: str | None = None,
    ) -> None:
        self.connection_config = connection_config
        self.application_config = application_config
        self.current_config_revision: ConfigRevision = current_config_revision
        self.log_level_override = log_level_override


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

    RETRY_STARTUP: bool = True
    RESTART_POLICY: RestartPolicy = WHEN_CONTINUOUS_TASKS_CRASHES
    USE_DEFAULT_STATE_STORE: bool = True
    _statestore_singleton: AbstractStateStore | None = None

    cancellation_token: CancellationToken

    def __init__(
        self, config: FullConfig[ConfigType], checkin_worker: CheckinWorker, metrics: BaseMetrics | None = None
    ) -> None:
        self._logger = logging.getLogger(f"{self.EXTERNAL_ID}.main")
        self._checkin_worker = checkin_worker

        self.cancellation_token = CancellationToken()
        self.cancellation_token.cancel_on_interrupt()

        self.connection_config = config.connection_config
        self.application_config = config.application_config
        self.metrics_config = config.application_config.metrics
        self.current_config_revision: ConfigRevision = config.current_config_revision
        self.log_level_override = config.log_level_override

        self.cognite_client = self.connection_config.get_cognite_client(f"{self.EXTERNAL_ID}-{self.VERSION}")

        self.state_store: AbstractStateStore

        self._checkin_lock = RLock()
        self._runtime_messages: Queue[RuntimeMessage] | None = None

        self._scheduler = TaskScheduler(self.cancellation_token.create_child_token())

        self._tasks: list[Task] = []
        self._start_time: datetime
        self._metrics: BaseMetrics | None = metrics

        self.metrics_push_manager = (
            self.metrics_config.create_manager(self.cognite_client, cancellation_token=self.cancellation_token)
            if self.metrics_config
            else None
        )

        self.__init_tasks__()

    def _setup_cancellation_watcher(self, cancel_event: MpEvent) -> None:
        """Starts a daemon thread to watch the inter-process event."""

        def watcher() -> None:
            """Blocks until the event is set, then cancels the local token."""
            cancel_event.wait()
            if not self.cancellation_token.is_cancelled:
                self._logger.info("Cancellation signal received from runtime. Shutting down gracefully.")
                self.cancellation_token.cancel()

        thread = Thread(target=watcher, name="RuntimeCancellationWatcher", daemon=True)
        thread.start()

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
                    level_for_handler = _resolve_log_level(self.log_level_override or console_handler.level.value)
                    sh.setLevel(level_for_handler)

                    root.addHandler(sh)

                case LogFileHandlerConfig() as file_handler:
                    level_for_handler = _resolve_log_level(self.log_level_override or file_handler.level.value)
                    try:
                        fh = RobustFileHandler(
                            filename=file_handler.path,
                            when="midnight",
                            utc=True,
                            backupCount=file_handler.retention,
                            create_dirs=True,
                        )
                        fh.setLevel(level_for_handler)
                        fh.setFormatter(fmt)

                        root.addHandler(fh)
                    except (OSError, PermissionError) as e:
                        if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
                            sh = logging.StreamHandler()
                            sh.setFormatter(fmt)
                            sh.setLevel(level_for_handler)
                            root.addHandler(sh)
                        self._logger.warning(
                            f"Could not create or write to log file {file_handler.path}: {e}. "
                            "Defaulted to console logging."
                        )

    def _load_state_store(self) -> None:
        """
        Searches through the config object for a StateStoreConfig.

        If found, it will use that configuration to generate a state store, if no such config is found it will either
        create a LocalStateStore or a NoStateStore depending on whether the ``use_default_state_store`` argument to the
        constructor was true or false.

        Either way, the state_store attribute is guaranteed to be set after calling this method.
        """
        state_store_config = self.application_config.state_store

        if state_store_config:
            self.state_store = state_store_config.create_state_store(
                cdf_client=self.cognite_client,
                default_to_local=self.USE_DEFAULT_STATE_STORE,
                cancellation_token=self.cancellation_token,
            )
        elif self.USE_DEFAULT_STATE_STORE:
            self.state_store = LocalStateStore("states.json", cancellation_token=self.cancellation_token)
        else:
            self.state_store = NoStateStore()

        try:
            self.state_store.initialize()
        except ValueError:
            self._logger.exception("Could not load state store, using an empty state store as default")
            self.state_store = NoStateStore()

        Extractor._statestore_singleton = self.state_store

    @classmethod
    def get_current_statestore(cls) -> AbstractStateStore:
        """
        Get the current state store singleton.

        Returns:
            The current state store singleton

        Raises:
            ValueError: If no state store singleton has been created, meaning no state store has been loaded.
        """
        if Extractor._statestore_singleton is None:
            raise ValueError("No state store singleton created. Have a state store been loaded?")
        return Extractor._statestore_singleton

    def __init_tasks__(self) -> None:
        """
        This method should be overridden by subclasses to define their tasks.

        It is called automatically when the extractor is initialized.

        Subclasses should call ``self.add_task(...)`` to add tasks to the extractor.
        """
        pass

    def _set_runtime_message_queue(self, queue: Queue) -> None:
        self._runtime_messages = queue

    def _attach_runtime_controls(self, *, cancel_event: MpEvent, message_queue: Queue) -> None:
        self._set_runtime_message_queue(message_queue)
        self._setup_cancellation_watcher(cancel_event)

    def _get_startup_request(self) -> StartupRequest:
        return StartupRequest(
            external_id=self.connection_config.integration.external_id,
            active_config_revision=self.current_config_revision,
            extractor=ExtractorInfo(version=self.VERSION, external_id=self.EXTERNAL_ID),
            tasks=[
                DtoTask(
                    type=TaskType.continuous if isinstance(t, ContinuousTask) else TaskType.batch,
                    action=isinstance(t, ScheduledTask),
                    description=t.description,
                    name=t.name,
                )
                for t in self._tasks
            ]
            if len(self._tasks) > 0
            else None,
            timestamp=int(self._start_time.timestamp() * 1000),
        )

    def _run_checkin(self) -> None:
        self._checkin_worker.run_periodic_checkin(self.cancellation_token, self._get_startup_request())

    def _report_error(self, error: Error) -> None:
        self._checkin_worker.report_error(error)

    def _try_report_error(self, error: Error) -> None:
        self._checkin_worker.try_report_error(error)

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
    def _init_from_runtime(
        cls, config: FullConfig[ConfigType], checkin_worker: CheckinWorker, metrics: BaseMetrics
    ) -> Self:
        return cls(config, checkin_worker, metrics)

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
            self._checkin_worker.report_task_start(name=task.name, timestamp=now())

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
                self._checkin_worker.report_task_end(name=task.name, timestamp=now())

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

    def start(self) -> None:
        """
        Start the extractor.

        Instead of calling this method directly, it is recommended to use the context manager interface by using the
        ``with`` statement, which ensures proper cleanup on exit.
        """
        self._setup_logging()
        self._start_time = datetime.now(tz=timezone.utc)

        self._load_state_store()
        self.state_store.start()

        Thread(target=self._run_checkin, name="ExtractorCheckin", daemon=True).start()
        if self.metrics_push_manager:
            self.metrics_push_manager.start()

    def stop(self) -> None:
        """
        Stop the extractor.

        Instead of calling this method directly, it is recommended to use the context manager interface by using the
        ``with`` statement, which ensures proper cleanup on exit.
        """
        if self.metrics_push_manager:
            self.metrics_push_manager.stop()
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

        if self.state_store:
            self.state_store.synchronize()

        self._checkin_worker.flush(self.cancellation_token)
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
