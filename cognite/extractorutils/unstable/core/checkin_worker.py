from logging import Logger
from secrets import SystemRandom
from threading import RLock
from time import sleep

from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError, CogniteAuthError, CogniteConnectionError
from cognite.extractorutils.threading import CancellationToken
from cognite.extractorutils.unstable.core._dto import CheckinResponse, Error, JSONType, StartupRequest

DEFAULT_SLEEP_INTERVAL = 30.0
STARTUP_BACKOFF_SECONDS = 30.0
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
        self._integration_id: str = integration
        self._logger: Logger = logger
        self._is_running: bool = False
        self._retry_startup: bool = retry_startup
        # self._runtime_messages: Queue[RuntimeMessage] | None = runtime_messages
        self._active_revision: int | None = active_revision

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
                    self._report_startup(startup_request, cancellation_token)
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
            self.flush()
            sleep(report_interval)

    def _report_startup(self, startup_request: StartupRequest, cancellation_token: CancellationToken) -> None:
        try:
            response = self._cognite_client.post(
                f"/api/v1/projects/{self._cognite_client.config.project}/integrations/extractorinfo",
                json=startup_request.model_dump(mode="json"),
                headers={"cdf-version": "alpha"},
            )

            self._handle_checkin_response(response.json(), cancellation_token)
        except CogniteConnectionError as e:
            if e.__cause__ is not None:
                self._logger.error(str(e.__cause__))
            self._logger.critical("Could not connect to CDF. Please check your configuration.")

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

    def _handle_checkin_response(self, response: JSONType, cancellation_token: CancellationToken) -> None:
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

    def flush(self) -> None:
        with self._flush_lock:
            ...

    def report_error(self, error: Error) -> None: ...

    def report_checkin(self) -> None:
        with self._flush_lock:
            ...
