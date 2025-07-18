from collections.abc import Callable
from logging import Logger
from secrets import SystemRandom
from threading import RLock
from time import sleep

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError, CogniteConnectionError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConfigRevision
from cognite.extractorutils.unstable.core._dto import (
    CheckinRequest,
    CheckinResponse,
    JSONType,
    StartupRequest,
    TaskUpdate,
)
from cognite.extractorutils.unstable.core._dto import (
    Error as DtoError,
)
from cognite.extractorutils.unstable.core.errors import Error

DEFAULT_SLEEP_INTERVAL = STARTUP_BACKOFF_SECONDS = 5  # 30.0
MAX_ERRORS_PER_CHECKIN = MAX_TASK_UPDATES_PER_CHECKIN = 1000
rng = SystemRandom()


class CheckinWorker:
    _start_lock = RLock()
    _flush_lock = RLock()

    def __init__(
        self,
        cognite_client: CogniteClient,
        integration: str,
        logger: Logger,
        on_revision_change: Callable[[], None],
        on_fatal_error: Callable[[Exception], None],
        active_revision: ConfigRevision,
        retry_startup: bool = False,
    ) -> None:
        self._cognite_client: CogniteClient = cognite_client
        self._integration: str = integration
        self._logger: Logger = logger
        self._on_revision_change: Callable[[], None] = on_revision_change
        self._on_fatal_error: Callable[[Exception], None] = on_fatal_error
        self._is_running: bool = False
        self._retry_startup: bool = retry_startup
        self._has_reported_startup: bool = False
        self._active_revision: ConfigRevision = active_revision
        self._errors: dict[str, DtoError] = {}
        self._task_updates: list[TaskUpdate] = []

    def run_periodic_checkin(
        self, cancellation_token: CancellationToken, startup_request: StartupRequest, interval: float | None = None
    ) -> None:
        with self._start_lock:
            if self._is_running:
                raise RuntimeError("Attempting to start a check-in worker that was already running")
            self._is_running = True

        with self._flush_lock:
            while not cancellation_token.is_cancelled:
                try:
                    self._report_startup(startup_request)
                    self._has_reported_startup = True
                    break
                except Exception as e:
                    if not self._retry_startup:
                        raise RuntimeError("Could not report startup") from e

                    a = STARTUP_BACKOFF_SECONDS / 2
                    b = STARTUP_BACKOFF_SECONDS * 3 / 2
                    next_retry = a + (b - a) * rng.random()
                    self._logger.warning("Failed to report startup, retrying in %.2f seconds", next_retry)
                    sleep(next_retry)

        report_interval = interval or DEFAULT_SLEEP_INTERVAL

        while not cancellation_token.is_cancelled:
            self._logger.debug("Running periodic check-in with interval %.2f seconds", report_interval)
            self.flush(cancellation_token)
            self._logger.debug("Check-in worker finished check-in, sleeping for %.2f seconds", report_interval)
            sleep(report_interval)

    def _report_startup(self, startup_request: StartupRequest) -> None:
        try:
            response = self._cognite_client.post(
                f"/api/v1/projects/{self._cognite_client.config.project}/integrations/startup",
                json=startup_request.model_dump(mode="json"),
                headers={"cdf-version": "alpha"},
            )

            self._handle_checkin_response(response.json())
        except CogniteConnectionError as e:
            if e.__cause__ is not None:
                self._logger.error(str(e.__cause__))
            self._logger.critical("Could not connect to CDF. Please check your configuration.")
            self._on_fatal_error(e)

        except CogniteAuthError as e:
            self._logger.error(str(e))
            self._logger.critical("Could not get an access token. Please check your configuration.")
            self._on_fatal_error(e)

        except CogniteAPIError as e:
            if e.code == 401:
                self._logger.critical(
                    "Got a 401 error from CDF. Please check your configuration. "
                    "Make sure the credentials and project is correct."
                )
                self._on_fatal_error(e)

            elif e.message:
                self._logger.critical(str(e.message))

            else:
                self._logger.critical(f"Error while connecting to CDF {e!s}")

    def _handle_checkin_response(self, response: JSONType) -> None:
        checkin_response = CheckinResponse.model_validate(response)
        self._logger.debug("Received check-in response: %s", checkin_response)

        if isinstance(self._active_revision, int) and checkin_response.last_config_revision is not None:
            if self._active_revision == "local":
                self._logger.warning(
                    "Remote config revision changed "
                    f"{self._active_revision} -> {checkin_response.last_config_revision}. "
                    "The extractor is currently using local configuration and will need to be manually restarted "
                    "and configured to use remote config for the new config to take effect.",
                )
            elif self._active_revision < checkin_response.last_config_revision:
                self._logger.info(
                    f"Remote config changed from {self._active_revision} to {checkin_response.last_config_revision}."
                )
                self._on_revision_change()

    def flush(self, cancellation_token: CancellationToken) -> None:
        with self._flush_lock:
            self._logger.debug(
                "Going to report check-in with %d errors and %d task updates.",
                len(self._errors),
                len(self._task_updates),
            )
            self.report_checkin(cancellation_token)

    def report_checkin(self, cancellation_token: CancellationToken) -> None:
        with self._start_lock:
            if not self._has_reported_startup:
                new_errors = [error for error in self._errors.values() if error.task is None]
                if len(new_errors) == 0:
                    self._logger.info("No startup request has been reported yet, skipping check-in.")
                    return
                self._logger.warning(
                    "Check-in worker has not reported startup yet, only reporting errors not associated with a task."
                )
                for error in new_errors:
                    del self._errors[error.external_id]
                task_updates: list[TaskUpdate] = []
            else:
                new_errors = list(self._errors.values())
                self._errors.clear()
                task_updates = self._task_updates[:]
                self._task_updates.clear()

            new_errors.sort(key=lambda e: e.end_time or e.start_time)
            task_updates.sort(key=lambda t: t.timestamp)

        while not cancellation_token.is_cancelled:
            if len(new_errors) <= MAX_ERRORS_PER_CHECKIN and len(task_updates) <= MAX_TASK_UPDATES_PER_CHECKIN:
                self._logger.debug("Writing check-in with no batching needed.")
                self._logger.debug("Writing %d errors and %d task updates.", len(new_errors), len(task_updates))
                errors_to_write = new_errors
                new_errors = []
                task_updates_to_write = task_updates
                task_updates = []
                self.try_write_checkin(
                    CheckinRequest(
                        external_id=self._integration, errors=errors_to_write, task_events=task_updates_to_write
                    ),
                )
                break

            errsIdx = 0
            tasksIdx = 0

            while (
                (errsIdx < len(new_errors) or tasksIdx < len(task_updates))
                and errsIdx < MAX_ERRORS_PER_CHECKIN
                and tasksIdx < MAX_TASK_UPDATES_PER_CHECKIN
            ):
                err = new_errors[errsIdx]
                errTime = err.end_time or err.start_time
                taskTime = task_updates[tasksIdx].timestamp

                if errTime <= taskTime:
                    errsIdx += 1
                if taskTime <= errTime:
                    tasksIdx += 1

            errors_to_write = new_errors[:errsIdx]
            task_updates_to_write = task_updates[:tasksIdx]

            if errsIdx > 0:
                new_errors = new_errors[errsIdx:]
            if tasksIdx > 0:
                task_updates = task_updates[tasksIdx:]
            self.try_write_checkin(
                CheckinRequest(external_id=self._integration, errors=errors_to_write, task_events=task_updates_to_write)
            )
            if errsIdx == 0 and tasksIdx == 0:
                self._logger.debug("No new errors or task updates to report, stopping checkin.")
                break

        self._logger.debug("Check-in worker finished writing check-in.")

        if cancellation_token.is_cancelled:
            self._logger.debug("Extractor was stopped during check-in, requeuing remaining errors and task updates.")
            self._requeue_checkin(new_errors, task_updates)

    def try_write_checkin(self, checkin_request: CheckinRequest) -> None:
        try:
            response = self._cognite_client.post(
                f"/api/v1/projects/{self._cognite_client.config.project}/integrations/checkin",
                json=checkin_request.model_dump(mode="json"),
                headers={"cdf-version": "alpha"},
            )
            self._handle_checkin_response(response.json())
        except CogniteConnectionError as e:
            if e.__cause__ is not None:
                self._logger.error(str(e.__cause__))
            self._logger.critical("Could not connect to CDF. Please check your configuration.")
            raise

        except CogniteAuthError as e:
            self._logger.error(str(e))
            self._logger.critical("Could not get an access token. Please check your configuration.")
            raise

        except CogniteAPIError as e:
            if e.code == 401:
                self._logger.critical(
                    "Got a 401 error from CDF. Please check your configuration. "
                    "Make sure the credentials and project is correct."
                )
                raise

            elif e.message:
                self._logger.critical(str(e.message))

            else:
                self._logger.critical(f"Error while connecting to CDF {e!s}")

            self._requeue_checkin(checkin_request.errors, checkin_request.task_events)

    def report_error(self, error: Error) -> None:
        """
        Report an error to the CDF API.

        This method is used to report errors that occur during the execution of the extractor.
        It will automatically requeue the error if the check-in fails.
        """
        with self._start_lock:
            if error.external_id not in self._errors:
                self._errors[error.external_id] = error.into_dto()
            else:
                self._logger.warning(f"Error {error.external_id} already reported, skipping re-reporting.")

    def report_task_start(self, name: str, timestamp: int) -> None:
        """
        Report a task start to the CDF API.

        This method is used to report start related to tasks that are running in the extractor.
        It will automatically requeue the task update if the check-in fails.
        """
        with self._start_lock:
            self._task_updates.append(TaskUpdate(type="started", name=name, timestamp=timestamp))

    def report_task_end(self, name: str, timestamp: int) -> None:
        """
        Report a task end to the CDF API.

        This method is used to report end related to tasks that are running in the extractor.
        It will automatically requeue the task update if the check-in fails.
        """
        with self._start_lock:
            self._task_updates.append(TaskUpdate(type="ended", name=name, timestamp=timestamp))

    def _requeue_checkin(self, errors: list[DtoError] | None, task_updates: list[TaskUpdate] | None) -> None:
        with self._start_lock:
            for error in errors or []:
                if error.external_id not in self._errors:
                    self._errors[error.external_id] = error
            self._task_updates.extend(task_updates or [])
