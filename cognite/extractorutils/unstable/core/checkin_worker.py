"""
Check-in worker for reporting errors and task updates to the CDF Integrations API.

The logic in this file is based off of the implementation in the dotnet extractorutils package.

It manages the reporting of startup, errors, and task updates to the Integrations API.

It ensures that startup (on none extractor related errors) are reported first,
followed by (all) errors and task updates.
"""

import sys
from collections.abc import Callable
from logging import Logger
from secrets import SystemRandom
from threading import RLock
from time import sleep

from requests import Response

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError, CogniteConnectionError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.configuration.models import ConfigRevision
from cognite.extractorutils.unstable.core._dto import (
    CheckinRequest,
    CheckinResponse,
    JSONType,
    MessageType,
    StartupRequest,
    TaskUpdate,
)
from cognite.extractorutils.unstable.core._dto import (
    Error as DtoError,
)
from cognite.extractorutils.unstable.core.errors import Error
from cognite.extractorutils.util import now

DEFAULT_SLEEP_INTERVAL = STARTUP_BACKOFF_SECONDS = 30.0
MAX_ERRORS_PER_CHECKIN = MAX_TASK_UPDATES_PER_CHECKIN = 1000
rng = SystemRandom()


class CheckinWorker:
    """
    A worker to manage how we report check-ins to the Integrations API.

    This will help us:
    1. Ensure that we don't report any check-ins before the start up is reported.
    1. Manage how we batch errors and task updates.
    2. Manage how we handle retries and backoff.
    """

    _lock = RLock()
    _flush_lock = RLock()

    def __init__(
        self,
        cognite_client: CogniteClient,
        integration: str,
        logger: Logger,
    ) -> None:
        """
        Initialize the CheckinWorker.

        Arguments:
            cognite_client (CogniteClient): Cognite client to use for API requests.
            integration (str): The external ID of the integration.
            logger (Logger): Logger to use for logging.
            on_revision_change (Callable[[int], None]): A trigger function to call when
                                                        the configuration revision changes.
            on_fatal_error: Callable[[Exception], None]: A trigger function to call when a fatal error occurs
                                                         such as a wrong CDF credentials.
            active_revision (ConfigRevision): The initial config revision when the worker is initialized.
            retry_startup (bool): Whether to retry reporting startup if it fails. Defaults to False.
        """
        self._cognite_client: CogniteClient = cognite_client
        self._integration: str = integration
        self._logger: Logger = logger
        self._on_revision_change: Callable[[int], None] | None = None
        self._on_fatal_error: Callable[[Exception], None] | None = None
        self._is_running: bool = False
        self._retry_startup: bool = False
        self._has_reported_startup: bool = False
        self._active_revision: ConfigRevision = "local"
        self._errors: dict[str, Error] = {}
        self._task_updates: list[TaskUpdate] = []

    @property
    def active_revision(self) -> ConfigRevision:
        """Get the active configuration revision."""
        return self._active_revision

    @active_revision.setter
    def active_revision(self, value: ConfigRevision) -> None:
        with self._lock:
            self._active_revision = value

    def set_on_revision_change_handler(self, on_revision_change: Callable[[int], None]) -> None:
        """
        Set the handler for when the configuration revision changes.

        This handler will be called with the new configuration revision when it changes.

        Arguments:
            on_revision_change (Callable[[int], None]): A callback to call when the configuration revision.
        """
        self._on_revision_change = on_revision_change

    def set_on_fatal_error_handler(self, on_fatal_error: Callable[[Exception], None]) -> None:
        """
        Set the handler for when a fatal error occurs.

        This handler will be called with the exception when a fatal error occurs, such as a wrong CDF credentials.

        Arguments:
            on_fatal_error (Callable[[Exception], None]): A callback to call when a fatal error occurs.
        """
        self._on_fatal_error = on_fatal_error

    def set_retry_startup(self, retry_startup: bool) -> None:
        """
        Set whether to retry reporting startup if it fails.

        Arguments:
            retry_startup (bool): Whether to retry reporting startup if it fails.
        """
        self._retry_startup = retry_startup

    def run_periodic_checkin(
        self, cancellation_token: CancellationToken, startup_request: StartupRequest, interval: float | None = None
    ) -> None:
        """
        Run periodic check ins with the Integrations API.

        This method will start a process that periodically reports check-ins to the Integrations API.
        It will also ensure that we report the start up first or just report errors that are not associated with a task

        Arguments:
            cancellation_token: A token to cancel the periodic check-in.
            startup_request: The start up request.
            interval: The interval in seconds between each check-in. If None, defaults to DEFAULT_SLEEP_INTERVAL.
        """
        with self._lock:
            if self._is_running:
                raise RuntimeError("Attempting to start a check-in worker that was already running")
            self._is_running = True

        self._run_startup_report(cancellation_token, startup_request, interval)

        report_interval = interval or DEFAULT_SLEEP_INTERVAL

        while not cancellation_token.is_cancelled:
            self._logger.debug("Running periodic check-in with interval %.2f seconds", report_interval)
            self.flush(cancellation_token)
            self._logger.debug(f"Check-in worker finished check-in, sleeping for {report_interval:.2f} seconds")
            cancellation_token.wait(report_interval)

    def _run_startup_report(
        self, cancellation_token: CancellationToken, startup_request: StartupRequest, interval: float | None = None
    ) -> None:
        with self._flush_lock:
            while not cancellation_token.is_cancelled:
                should_retry = self._report_startup(startup_request)
                if not should_retry:
                    self._has_reported_startup = True
                    break
                elif not self._retry_startup:
                    raise RuntimeError("Could not report startup")

                interval = interval or STARTUP_BACKOFF_SECONDS
                next_retry = interval / 2 + interval * rng.random()
                self._logger.info("Failed to report startup, retrying in %.2f seconds", next_retry)
                sleep(next_retry)

    def _report_startup(self, startup_request: StartupRequest) -> bool:
        return self._wrap_checkin_like_request(
            lambda: self._cognite_client.post(
                f"/api/v1/projects/{self._cognite_client.config.project}/integrations/startup",
                json=startup_request.model_dump(mode="json", by_alias=True),
                headers={"cdf-version": "alpha"},
            )
        )

    def _handle_checkin_response(self, response: JSONType) -> None:
        checkin_response = CheckinResponse.model_validate(response)
        self._logger.debug("Received check-in response: %s", checkin_response)

        if checkin_response.last_config_revision is not None:
            if self._active_revision == "local":
                self._logger.warning(
                    "Remote config revision changed "
                    f"{self._active_revision} -> {checkin_response.last_config_revision}. "
                    "The extractor is currently using local configuration and will need to be manually restarted "
                    "and configured to use remote config for the new config to take effect.",
                )
            elif self._active_revision < checkin_response.last_config_revision:
                self._active_revision = checkin_response.last_config_revision
                if self._on_revision_change is not None:
                    self._logger.info(
                        "Remote config revision changed %s -> %s. The extractor will now use the new configuration.",
                        self._active_revision,
                        checkin_response.last_config_revision,
                    )
                    self._on_revision_change(checkin_response.last_config_revision)

    def flush(self, cancellation_token: CancellationToken) -> None:
        """
        Flush available check-ins.

        Arguments:
        cancellation_token: A token to cancel the check-in reporting.
        """
        with self._flush_lock:
            self._logger.debug(
                "Going to report check-in with %d errors and %d task updates.",
                len(self._errors),
                len(self._task_updates),
            )
            self.report_checkin(cancellation_token)

    def report_checkin(self, cancellation_token: CancellationToken) -> None:
        """
        Report a check-in to the Integrations API.

        Arguments:
        cancellation_token: A token to cancel the check-in reporting.
        """
        with self._lock:
            if not self._has_reported_startup:
                new_errors = [error for error in self._errors.values() if error._task_name is None]
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
                self._logger.debug("Writing %d errors and %d task updates.", len(new_errors), len(task_updates))
                errors_to_write = new_errors
                new_errors = []
                task_updates_to_write = task_updates
                task_updates = []
                self.try_write_checkin(
                    errors_to_write,
                    task_updates_to_write,
                )
                break

            errs_idx = 0
            tasks_idx = 0

            while (
                (errs_idx < len(new_errors) or tasks_idx < len(task_updates))
                and errs_idx < MAX_ERRORS_PER_CHECKIN
                and tasks_idx < MAX_TASK_UPDATES_PER_CHECKIN
            ):
                err = new_errors[errs_idx] if errs_idx < len(new_errors) else None
                err_time = sys.maxsize if err is None else (err.end_time or err.start_time)
                task_time = task_updates[tasks_idx].timestamp if tasks_idx < len(task_updates) else sys.maxsize

                if err_time <= task_time:
                    errs_idx += 1
                if task_time <= err_time:
                    tasks_idx += 1
            self._logger.debug(f"Batching check-in with {errs_idx} errors and {tasks_idx} task updates.")

            errors_to_write = new_errors[:errs_idx]
            task_updates_to_write = task_updates[:tasks_idx]

            self._logger.debug("Writing check-in with batching needed.")
            self._logger.debug(
                "Writing %d errors and %d task updates.", len(errors_to_write), len(task_updates_to_write)
            )

            if errs_idx > 0:
                new_errors = new_errors[errs_idx:]
            if tasks_idx > 0:
                task_updates = task_updates[tasks_idx:]
            self.try_write_checkin(
                errors_to_write,
                task_updates_to_write,
            )
            if errs_idx == 0 and tasks_idx == 0:
                self._logger.debug("Check-in worker finished writing check-in.")
                break

        if cancellation_token.is_cancelled:
            self._logger.debug("Extractor was stopped during check-in, requeuing remaining errors and task updates.")
            self._requeue_checkin(new_errors, task_updates)

    def try_write_checkin(self, errors: list[Error], task_updates: list[TaskUpdate]) -> None:
        """
        We try to write a check-in.

        This will try to write a check in to integrations.

        Arguments:
        errors(list[Error]): The errors to write.
        task_updates(list[TaskUpdate]): The task updates to write.
        """
        checkin_request = CheckinRequest(
            external_id=self._integration,
            errors=list(map(DtoError.from_internal, errors)) if len(errors) > 0 else None,
            task_events=task_updates if len(task_updates) > 0 else None,
        )
        should_requeue = self._wrap_checkin_like_request(
            lambda: self._cognite_client.post(
                f"/api/v1/projects/{self._cognite_client.config.project}/integrations/checkin",
                json=checkin_request.model_dump(mode="json", by_alias=True),
                headers={"cdf-version": "alpha"},
            )
        )

        if should_requeue:
            self._requeue_checkin(errors, checkin_request.task_events)

    def report_error(self, error: Error) -> None:
        """
        Queue check-in error to be reported to Integrations API.

        This method is used to report errors that occur during the execution of the extractor.
        It will automatically requeue the error if the check-in fails.
        """
        with self._lock:
            if error.external_id not in self._errors:
                self._errors[error.external_id] = error
            else:
                self._logger.warning(f"Error {error.external_id} already reported, skipping re-reporting.")

    def try_report_error(self, error: Error) -> None:
        """
        This method will try to queue an error to be reported to the Integrations API.

        Arguments:
            error (Error): The error to report.
        """
        with self._lock:
            if error.external_id not in self._errors:
                self._errors[error.external_id] = error

    def report_task_start(self, name: str, message: MessageType | None = None, timestamp: int | None = None) -> None:
        """
        Queue task start to be reported to Integrations API.

        This method is used to queue start related to tasks that are running in the extractor.
        It will automatically requeue the task update if the check-in fails.
        """
        with self._lock:
            self._task_updates.append(
                TaskUpdate(type="started", name=name, timestamp=timestamp or (int(now() * 1000)), message=message)
            )

    def report_task_end(self, name: str, message: MessageType | None = None, timestamp: int | None = None) -> None:
        """
        Queue task start to be reported to Integrations API.

        This method is used to queue end related to tasks that are running in the extractor.
        It will automatically requeue the task update if the check-in fails.
        """
        with self._lock:
            self._task_updates.append(
                TaskUpdate(type="ended", name=name, timestamp=timestamp or (int(now() * 1000)), message=message)
            )

    def _requeue_checkin(self, errors: list[Error] | None, task_updates: list[TaskUpdate] | None) -> None:
        with self._lock:
            for error in errors or []:
                if error.external_id not in self._errors:
                    self._errors[error.external_id] = error
            self._task_updates.extend(task_updates or [])

    def _wrap_checkin_like_request(self, request: Callable[[], Response]) -> bool:
        exception_to_report: Exception | None = None
        requeue = False
        try:
            response = request()
            self._handle_checkin_response(response.json())
        except CogniteConnectionError as e:
            if e.__cause__ is not None:
                self._logger.error(str(e.__cause__))
            self._logger.critical("Could not connect to CDF. Please check your configuration.")
            requeue = True

        except CogniteAuthError as e:
            self._logger.error(str(e))
            self._logger.critical("Could not get an access token. Please check your configuration.")
            exception_to_report = e
            requeue = True

        except CogniteAPIError as e:
            if e.code == 401:
                self._logger.critical(
                    "Got a 401 error from CDF. Please check your configuration. "
                    "Make sure the credentials and project is correct."
                )
                exception_to_report = e

            elif e.message:
                self._logger.critical(str(e.message))

            else:
                self._logger.critical(f"Error while connecting to CDF {e!s}")
            requeue = True

        except Exception as e:
            self._logger.critical(f"Extractor could not connect to CDF {e!s}")
            exception_to_report = e
            requeue = True

        if exception_to_report is not None and self._on_fatal_error is not None:
            self._on_fatal_error(exception_to_report)

        return requeue
