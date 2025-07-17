from logging import Logger
from secrets import SystemRandom
from threading import RLock
from time import sleep

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError, CogniteConnectionError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.core._dto import (
    CheckinRequest,
    CheckinResponse,
    Error,
    JSONType,
    StartupRequest,
    TaskUpdate,
)

DEFAULT_SLEEP_INTERVAL = STARTUP_BACKOFF_SECONDS = 30.0
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
        # on_revision_change: Callable[[], None],
        # runtime_messages: Queue[RuntimeMessage] | None = None,
        retry_startup: bool = False,
        active_revision: int | None = None,
    ) -> None:
        self._cognite_client: CogniteClient = cognite_client
        self._integration: str = integration
        self._logger: Logger = logger
        self._is_running: bool = False
        self._retry_startup: bool = retry_startup
        self._has_reported_startup: bool = False
        # self._runtime_messages: Queue[RuntimeMessage] | None = runtime_messages
        self._active_revision: int | None = active_revision
        self._errors: dict[str, Error] = {}
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
            self.flush(cancellation_token)
            sleep(report_interval)

    def _report_startup(self, startup_request: StartupRequest) -> None:
        try:
            response = self._cognite_client.post(
                f"/api/v1/projects/{self._cognite_client.config.project}/integrations/extractorinfo",
                json=startup_request.model_dump(mode="json"),
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
            # Error response from the CDF API
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

    def _handle_checkin_response(self, response: JSONType) -> None:
        checkin_response = CheckinResponse.model_validate(response)

        if self._active_revision is not None and checkin_response.last_config_revision is not None:
            if self._active_revision < checkin_response.last_config_revision:
                self._logger.info(
                    f"Remote config changed from {self._active_revision} to {checkin_response.last_config_revision}."
                )
                # self.restart_extractor(cancellation_token)
            elif self._active_revision is None:
                self._logger.warning("")

    # def restart_extractor(self, cancellation_token: CancellationToken) -> None:
    #     self._logger.info("Restarting extractor")
    #     if self._runtime_messages:
    #         self._runtime_messages.put(RuntimeMessage.RESTART)
    #     cancellation_token.cancel()

    def flush(self, cancellation_token: CancellationToken) -> None:
        with self._flush_lock:
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
                task_updates = self._task_updates

            new_errors.sort(key=lambda e: e.end_time or e.start_time)
            task_updates.sort(key=lambda t: t.timestamp)

        while not cancellation_token.is_cancelled:
            if len(new_errors) <= MAX_ERRORS_PER_CHECKIN and len(task_updates) <= MAX_TASK_UPDATES_PER_CHECKIN:
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
        else:
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
            # Error response from the CDF API
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

    def _requeue_checkin(self, errors: list[Error] | None, task_updates: list[TaskUpdate] | None) -> None:
        with self._start_lock:
            for error in errors or []:
                if error.external_id not in self._errors:
                    self._errors[error.external_id] = error
            self._task_updates.extend(task_updates or [])
