import logging
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar, Token
from multiprocessing import Queue
from threading import RLock, Thread
from traceback import format_exception
from types import TracebackType
from typing import Generic, Literal, Optional, Type, TypeVar, Union

from humps import pascalize
from typing_extensions import Self, assert_never

from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConnectionConfig, ExtractorConfig
from cognite.extractorutils.unstable.core._dto import Error as DtoError
from cognite.extractorutils.unstable.core._dto import TaskUpdate
from cognite.extractorutils.unstable.core._messaging import RuntimeMessage
from cognite.extractorutils.unstable.core.errors import Error, ErrorLevel
from cognite.extractorutils.unstable.core.tasks import ContinuousTask, ScheduledTask, StartupTask, Task
from cognite.extractorutils.unstable.scheduling import TaskScheduler
from cognite.extractorutils.util import now

ConfigType = TypeVar("ConfigType", bound=ExtractorConfig)
ConfigRevision = Union[Literal["local"], int]


class Extractor(Generic[ConfigType]):
    NAME: str
    EXTERNAL_ID: str
    DESCRIPTION: str
    VERSION: str

    CONFIG_TYPE: Type[ConfigType]

    def __init__(
        self,
        connection_config: ConnectionConfig,
        application_config: ConfigType,
        current_config_revision: ConfigRevision,
    ) -> None:
        self.cancellation_token = CancellationToken()
        self.cancellation_token.cancel_on_interrupt()

        self.connection_config = connection_config
        self.application_config = application_config
        self.current_config_revision = current_config_revision

        self.cognite_client = self.connection_config.get_cognite_client(f"{self.EXTERNAL_ID}-{self.VERSION}")

        self._checkin_lock = RLock()
        self._runtime_messages: Optional[Queue[RuntimeMessage]] = None

        self._scheduler = TaskScheduler(self.cancellation_token.create_child_token())

        self._tasks: list[Task] = []
        self._task_updates: list[TaskUpdate] = []
        self._errors: dict[str, Error] = {}

        self.logger = logging.getLogger(f"{self.EXTERNAL_ID}.main")

        self._current_task: ContextVar[str | None] = ContextVar("current_task", default=None)

        self.__init_tasks__()

    def __init_tasks__(self) -> None:
        pass

    def _set_runtime_message_queue(self, queue: Queue) -> None:
        self._runtime_messages = queue

    def _checkin(self) -> None:
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
            f"/api/v1/projects/{self.cognite_client.config.project}/odin/checkin",
            json={
                "externalId": self.connection_config.extraction_pipeline,
                "taskEvents": task_updates,
                "errors": error_updates,
            },
            headers={"cdf-version": "alpha"},
        )
        new_config_revision = res.json().get("lastConfigRevision")

        if new_config_revision and new_config_revision != self.current_config_revision:
            self.restart()

    def _run_checkin(self) -> None:
        while not self.cancellation_token.is_cancelled:
            try:
                self.logger.debug("Running checkin")
                self._checkin()
            except Exception:
                self.logger.exception("Error during checkin")
            self.cancellation_token.wait(10)

    def _report_error(self, error: Error) -> None:
        with self._checkin_lock:
            self._errors[error.external_id] = error

    def error(
        self,
        level: ErrorLevel,
        description: str,
        details: str | None = None,
        *,
        force_global: bool = False,
    ) -> Error:
        task_name = self._current_task.get()

        return Error(
            level=level,
            description=description,
            details=details,
            extractor=self,
            task_name=None if force_global else task_name,
        )

    def restart(self) -> None:
        if self._runtime_messages:
            self._runtime_messages.put(RuntimeMessage.RESTART)
        self.cancellation_token.cancel()

    @classmethod
    def _init_from_runtime(
        cls,
        connection_config: ConnectionConfig,
        application_config: ConfigType,
        current_config_revision: ConfigRevision,
    ) -> Self:
        return cls(connection_config, application_config, current_config_revision)

    def add_task(self, task: Task) -> None:
        # Store this for later, since we'll override it with the wrapped version
        target = task.target

        def wrapped() -> None:
            """
            A wrapped version of the task's target, with tracking and error handling
            """
            # Record a task start
            with self._checkin_lock:
                self._task_updates.append(
                    TaskUpdate(type="started", name=task.name, timestamp=now()),
                )

            context_token: Token[str | None] | None = None

            try:
                # Set the current task context var, used to track that we're in a task for error reporting
                context_token = self._current_task.set(task.name)

                # Run task
                target()

            except Exception as e:
                # Task crashed, record it as a fatal error
                self.error(
                    ErrorLevel.fatal,
                    description="Task crashed unexpectedly",
                    details="".join(format_exception(e)),
                ).instant()

                raise e

            finally:
                # Unset the current task
                if context_token is not None:
                    self._current_task.reset(context_token)

                # Record task end
                with self._checkin_lock:
                    self._task_updates.append(
                        TaskUpdate(type="ended", name=task.name, timestamp=now()),
                    )

        task.target = wrapped
        self._tasks.append(task)

        match task:
            case ScheduledTask() as t:
                self._scheduler.schedule_task(name=t.name, schedule=t.schedule, task=t.target)

    def _report_extractor_info(self) -> None:
        self.cognite_client.post(
            f"/api/v1/projects/{self.cognite_client.config.project}/odin/extractorinfo",
            json={
                "externalId": self.connection_config.extraction_pipeline,
                "activeConfigRevision": self.current_config_revision,
                "extractor": {
                    "version": self.VERSION,
                    "externalId": self.EXTERNAL_ID,
                },
                "tasks": [
                    {
                        "name": t.name,
                        "type": "continuous" if isinstance(t, ContinuousTask) else "batch",
                    }
                    for t in self._tasks
                ],
            },
            headers={"cdf-version": "alpha"},
        )

    def start(self) -> None:
        self._report_extractor_info()
        Thread(target=self._run_checkin, name="ExtractorCheckin", daemon=True).start()

    def stop(self) -> None:
        self.cancellation_token.cancel()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        self.stop()
        with self._checkin_lock:
            self._checkin()

        return exc_val is None

    def run(self) -> None:
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

        self.logger.info("Starting extractor")
        if startup:
            with ThreadPoolExecutor() as pool:
                for task in startup:
                    pool.submit(task.target)
        self.logger.info("Startup done")

        for task in continuous:
            Thread(name=pascalize(task.name), target=task.target).start()

        if has_scheduled:
            self._scheduler.run()

        else:
            self.cancellation_token.wait()
