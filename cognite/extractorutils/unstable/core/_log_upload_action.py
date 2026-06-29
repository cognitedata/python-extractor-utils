"""Built-in ``fetch_logs`` action: streams rotated log files to CDF Files."""

import json
import logging
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta, timezone
from datetime import datetime as dt
from pathlib import Path
from typing import BinaryIO

from cognite.client import CogniteClient

from cognite.extractorutils.unstable.configuration.models import ExtractorConfig, LogFileHandlerConfig
from cognite.extractorutils.unstable.core._bounded_reader import BoundedReader
from cognite.extractorutils.unstable.core.actions import ActionContext, ActionError

_logger = logging.getLogger(__name__)

MAX_DATE_RANGE_DAYS = 7
"""Maximum number of calendar days a single ``fetch_logs`` invocation may cover."""

MAX_FILE_SIZE_BYTES = 4 * 1024 * 1024 * 1024  # 4 GiB — CDF single-request upload limit
DEFAULT_CONCURRENT_UPLOADS = 1  # Sequential by default; safe on constrained networks

_FETCH_LOGS_DESCRIPTION = (
    f"Upload rotated log files to CDF Files for a given date range. At most {MAX_DATE_RANGE_DAYS} days per invocation."
)


@dataclass(frozen=True)
class LogFileCandidate:
    """A log file resolved for a given date that exists and has content."""

    log_date: date
    path: Path
    is_current: bool  # True when path is the live (unrotated) file.log


@dataclass
class _FileUploadResult:
    log_date: date
    file_external_id: str
    status: str  # "uploaded" | "skipped_too_large" | "failed"
    size_bytes: int = 0
    error: str | None = None


def _today_utc() -> date:
    return dt.now(tz=timezone.utc).date()


def _parse_date(raw: str, field: str) -> date:
    try:
        return date.fromisoformat(raw)
    except (ValueError, TypeError):
        raise ActionError(
            f"Invalid {field} '{raw}': expected ISO 8601 date (YYYY-MM-DD)",
            error_type="invalid_parameter",
        ) from None


def _resolve_log_file_path(config: ExtractorConfig) -> Path | None:
    """Return the base log file path from the first file handler in config, or None."""
    for handler in config.log_handlers:
        if isinstance(handler, LogFileHandlerConfig):
            return handler.path
    return None


def _build_candidate_files(
    base_path: Path,
    start_date: date,
    end_date: date,
    today: date,
) -> tuple[list[LogFileCandidate], list[date]]:
    """
    Enumerate log files for [start_date, end_date] and partition into candidates vs skipped.

    Rotated files follow the naming convention ``<base_path>.YYYY-MM-DD``.
    The live file (``base_path``) is used for ``today``; all other dates use the rotated name.

    A file is skipped when it does not exist, is empty, or is inaccessible (any ``OSError``).
    This covers the brief rotation race window: if the action is dispatched right after midnight
    before ``TimedRotatingFileHandler`` has renamed ``file.log`` to ``file.log.YYYY-MM-DD``,
    the rotated file will not be found and that date will appear in ``skipped``. Retrying after
    rotation completes (within seconds) will pick it up.

    Returns:
        (candidates, skipped): candidates are files that exist and have content;
        skipped contains dates for which no usable file was found.
    """
    candidates: list[LogFileCandidate] = []
    skipped: list[date] = []

    current = start_date
    while current <= end_date:
        if current == today:
            path = base_path
            is_current = True
        else:
            path = base_path.parent / f"{base_path.name}.{current.isoformat()}"
            is_current = False

        try:
            size = path.stat().st_size
        except OSError as e:
            _logger.warning("fetch_logs: skipping %s for %s — %s", path, current, e)
            skipped.append(current)
        else:
            if size > 0:
                candidates.append(LogFileCandidate(log_date=current, path=path, is_current=is_current))
            else:
                skipped.append(current)

        current += timedelta(days=1)

    return candidates, skipped


def _file_external_id(integration_external_id: str, log_date: date) -> str:
    return f"extractor-logs-{integration_external_id}-{log_date.isoformat()}"


def _upload_candidate(
    candidate: LogFileCandidate,
    integration_external_id: str,
    cdf_client: CogniteClient,
    snapshot_size: int | None,
) -> _FileUploadResult:
    """Upload one candidate log file to CDF Files. Returns a result regardless of success or failure."""
    external_id = _file_external_id(integration_external_id, candidate.log_date)

    try:
        actual_size = snapshot_size if snapshot_size is not None else candidate.path.stat().st_size
    except OSError as e:
        return _FileUploadResult(
            log_date=candidate.log_date, file_external_id=external_id, status="failed", error=str(e)
        )

    if actual_size > MAX_FILE_SIZE_BYTES:
        _logger.warning(
            "fetch_logs: skipping %s (%d bytes) — exceeds MAX_FILE_SIZE_BYTES (%d bytes)",
            candidate.path.name,
            actual_size,
            MAX_FILE_SIZE_BYTES,
        )
        return _FileUploadResult(
            log_date=candidate.log_date,
            file_external_id=external_id,
            status="skipped_too_large",
            size_bytes=actual_size,
        )

    try:
        f = open(candidate.path, "rb")  # noqa: SIM115
        reader: BinaryIO = BoundedReader(f, snapshot_size) if snapshot_size is not None else f  # type: ignore[assignment]
        with reader:
            cdf_client.files.upload_bytes(
                content=reader,
                name=f"{external_id}.log",
                external_id=external_id,
                mime_type="text/plain",
                overwrite=True,
            )
        _logger.info("fetch_logs: uploaded %s (%d bytes)", external_id, actual_size)
        return _FileUploadResult(
            log_date=candidate.log_date,
            file_external_id=external_id,
            status="uploaded",
            size_bytes=actual_size,
        )
    except Exception as e:
        _logger.error("fetch_logs: failed to upload %s — %s", external_id, e)
        return _FileUploadResult(
            log_date=candidate.log_date,
            file_external_id=external_id,
            status="failed",
            error=str(e),
        )


def fetch_logs_action(ctx: ActionContext) -> None:
    """Validate parameters and upload rotated log files for the requested date range to CDF Files."""
    params = ctx.call_metadata or {}

    start_date_raw = params.get("start_date")
    end_date_raw = params.get("end_date")

    if start_date_raw is None:
        raise ActionError("Missing required parameter: start_date", error_type="missing_parameter")
    if end_date_raw is None:
        raise ActionError("Missing required parameter: end_date", error_type="missing_parameter")

    start_date = _parse_date(start_date_raw, "start_date")
    end_date = _parse_date(end_date_raw, "end_date")

    if end_date < start_date:
        raise ActionError(
            f"end_date ({end_date}) must be on or after start_date ({start_date})",
            error_type="invalid_date_range",
        )

    today = _today_utc()

    if end_date > today:
        raise ActionError(
            f"end_date ({end_date}) cannot be in the future (today is {today})",
            error_type="invalid_date_range",
        )

    num_days = (end_date - start_date).days + 1
    if num_days > MAX_DATE_RANGE_DAYS:
        raise ActionError(
            f"Date range of {num_days} days exceeds the maximum of {MAX_DATE_RANGE_DAYS} days; "
            "use multiple invocations for longer spans",
            error_type="invalid_date_range",
        )

    log_file_path = _resolve_log_file_path(ctx.application_config)
    if log_file_path is None:
        raise ActionError(
            "No file log handler configured; add a 'file' type log handler to enable log uploads",
            error_type="no_file_handler_configured",
        )

    candidates, skipped_dates = _build_candidate_files(log_file_path, start_date, end_date, today)
    _logger.info(
        "fetch_logs: %d candidate file(s) for %s to %s; %d date(s) skipped",
        len(candidates),
        start_date,
        end_date,
        len(skipped_dates),
    )

    # Snapshot the current-day file size BEFORE spawning upload threads.
    # This gives a fixed read ceiling for BoundedReader — bytes written after this
    # point are excluded from the upload, preventing Content-Length mismatches.
    snapshot_size: int | None = None
    current_candidate = next((c for c in candidates if c.is_current), None)
    if current_candidate is not None:
        try:
            snapshot_size = current_candidate.path.stat().st_size
            _logger.info(
                "fetch_logs: current-day snapshot %d bytes (%s)",
                snapshot_size,
                current_candidate.path.name,
            )
        except OSError as e:
            _logger.warning("fetch_logs: could not snapshot current-day file size — %s", e)

    integration_external_id = ctx._extractor.connection_config.integration.external_id
    cdf_client = ctx._extractor.cognite_client

    upload_results: list[_FileUploadResult] = []
    with ThreadPoolExecutor(max_workers=DEFAULT_CONCURRENT_UPLOADS) as pool:
        futures: dict[Future[_FileUploadResult], LogFileCandidate] = {
            pool.submit(
                _upload_candidate,
                candidate,
                integration_external_id,
                cdf_client,
                snapshot_size if candidate.is_current else None,
            ): candidate
            for candidate in candidates
        }
        upload_results.extend(future.result() for future in as_completed(futures))

    upload_results.sort(key=lambda r: r.log_date)

    uploaded = [r for r in upload_results if r.status == "uploaded"]
    too_large = [r for r in upload_results if r.status == "skipped_too_large"]
    failed = [r for r in upload_results if r.status == "failed"]

    # Per-file entries: upload results (sorted by date) + missing dates (skipped by candidate builder)
    files_list: list[dict[str, str]] = []
    for r in upload_results:
        entry: dict[str, str] = {
            "date": str(r.log_date),
            "file_external_id": r.file_external_id,
            "status": r.status,
        }
        if r.size_bytes:
            entry["size_bytes"] = str(r.size_bytes)
        if r.error:
            entry["error"] = r.error
        files_list.append(entry)
    files_list.extend(
        {
            "date": str(d),
            "file_external_id": _file_external_id(integration_external_id, d),
            "status": "skipped",
        }
        for d in sorted(skipped_dates)
    )
    files_list.sort(key=lambda e: e["date"])

    total_skipped = len(skipped_dates) + len(too_large)

    ctx.set_result(
        f"{len(uploaded)} of {num_days} log files uploaded to CDF Files",
        metadata={
            "total_files": str(num_days),
            "uploaded_files": str(len(uploaded)),
            "skipped_files": str(total_skipped),
            "failed_files": str(len(failed)),
            "files": json.dumps(files_list),
        },
    )
