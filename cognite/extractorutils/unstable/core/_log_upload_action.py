"""
Built-in ``fetch_logs`` action: streams rotated log files to CDF Files.

Every invocation uploads a new copy of each file (nothing is overwritten): the CDF external ID is
``extractor-logs-<integration>-<log date>-<action external id>``. The files go into the data set returned by
``Extractor.get_log_upload_data_set_id`` (none by default).

Final status: ``failed`` for invalid parameters, ``completed_with_errors`` when no file at all was uploaded
(everything missing, too large or failed), ``succeeded`` otherwise, including when only some days had a file.
"""

import hashlib
import json
import logging
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta, timezone
from datetime import datetime as dt
from pathlib import Path
from typing import Any, BinaryIO, Literal

from cognite.client import CogniteClient

from cognite.extractorutils.unstable.configuration.models import ExtractorConfig, LogFileHandlerConfig
from cognite.extractorutils.unstable.core._bounded_reader import BoundedReader
from cognite.extractorutils.unstable.core._dto import MAX_METADATA_VALUE_BYTES
from cognite.extractorutils.unstable.core.actions import ActionContext, ActionError

_logger = logging.getLogger(__name__)

MAX_DATE_RANGE_DAYS = 7
"""Maximum number of calendar days a single ``fetch_logs`` invocation may cover."""

MAX_FILE_SIZE_BYTES = 4 * 1024 * 1024 * 1024  # 4 GiB — CDF single-request upload limit

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
    status: Literal["uploaded", "skipped_too_large", "failed"]
    file_id: int | None = None  # CDF's internal (numeric) file id; set only when status == "uploaded"
    size_bytes: int | None = None
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
    file_handlers = [h for h in config.log_handlers if isinstance(h, LogFileHandlerConfig)]
    if not file_handlers:
        return None
    if len(file_handlers) > 1:
        _logger.warning(
            "fetch_logs: multiple file log handlers configured; only the first (%s) will be uploaded",
            file_handlers[0].path,
        )
    return file_handlers[0].path


def _build_candidate_files(
    base_path: Path,
    start_date: date,
    end_date: date,
    today: date,
) -> tuple[list[LogFileCandidate], list[date]]:
    """
    Enumerate log files for [start_date, end_date] and partition into candidates vs missing.

    Rotated files follow the naming convention ``<base_path>.YYYY-MM-DD``.
    The live file (``base_path``) is used for ``today``; all other dates use the rotated name.

    A file is missing when it does not exist, is empty, or is inaccessible (any ``OSError``).
    This covers the brief rotation race window: if the action is dispatched right after midnight
    before ``TimedRotatingFileHandler`` has renamed ``file.log`` to ``file.log.YYYY-MM-DD``,
    the rotated file will not be found and that date will be reported as ``missing``. Retrying after
    rotation completes (within seconds) will pick it up.

    Returns:
        (candidates, missing): candidates are files that exist and have content;
        missing contains dates for which no usable file was found.
    """
    candidates: list[LogFileCandidate] = []
    missing: list[date] = []

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
            _logger.warning("fetch_logs: no usable log file %s for %s — %s", path, current, e)
            missing.append(current)
        else:
            if size > 0:
                candidates.append(LogFileCandidate(log_date=current, path=path, is_current=is_current))
            else:
                missing.append(current)

        current += timedelta(days=1)

    return candidates, missing


MAX_EXTERNAL_ID_LENGTH = 255
"""CDF's limit for file external IDs."""


def _file_external_id(integration_external_id: str, log_date: date, run_id: str) -> str:
    """
    Build the CDF external ID of one uploaded log file.

    ``run_id`` (the external ID of the action invocation) makes the ID unique per run so repeated runs never
    overwrite each other, while the ``extractor-logs-<integration>-<date>`` prefix keeps the files findable. IDs
    are deterministic for a given run. If the result would exceed CDF's limit, ``run_id`` is replaced by a hash of
    itself and, as a last resort, the integration part is shortened.
    """
    suffix = f"{log_date.isoformat()}-{run_id}"
    external_id = f"extractor-logs-{integration_external_id}-{suffix}"
    if len(external_id) <= MAX_EXTERNAL_ID_LENGTH:
        return external_id
    suffix = f"{log_date.isoformat()}-{hashlib.sha256(run_id.encode('utf-8')).hexdigest()[:16]}"
    room = MAX_EXTERNAL_ID_LENGTH - len("extractor-logs--") - len(suffix)
    return f"extractor-logs-{integration_external_id[:room]}-{suffix}"


def _upload_candidate(
    candidate: LogFileCandidate,
    integration_external_id: str,
    cdf_client: CogniteClient,
    *,
    run_id: str,
    data_set_id: int | None = None,
) -> _FileUploadResult:
    """
    Upload one candidate log file to CDF Files as a new file. Returns a result regardless of success or failure.

    The file is never overwritten: ``run_id`` makes the external ID unique per ``fetch_logs`` invocation.
    """
    external_id = _file_external_id(integration_external_id, candidate.log_date, run_id)

    try:
        actual_size = candidate.path.stat().st_size
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

    # Only send data_set_id when there is one, so extractors without a data set upload exactly as before.
    extra_upload_args: dict[str, Any] = {} if data_set_id is None else {"data_set_id": data_set_id}
    try:
        with open(candidate.path, "rb") as f:
            reader: BinaryIO = BoundedReader(f, actual_size) if candidate.is_current else f  # type: ignore[assignment]
            uploaded = cdf_client.files.upload_bytes(
                content=reader,
                name=f"{external_id}.log",
                external_id=external_id,
                mime_type="text/plain",
                **extra_upload_args,
            )
        _logger.info("fetch_logs: uploaded %s (%d bytes)", external_id, actual_size)
        return _FileUploadResult(
            log_date=candidate.log_date,
            file_external_id=external_id,
            status="uploaded",
            file_id=uploaded.id,
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

    candidates, missing_dates = _build_candidate_files(log_file_path, start_date, end_date, today)
    _logger.info(
        "fetch_logs: %d candidate file(s) for %s to %s; %d date(s) missing",
        len(candidates),
        start_date,
        end_date,
        len(missing_dates),
    )

    integration_external_id = ctx.integration_external_id
    cdf_client = ctx.cdf_client
    data_set_id = ctx.log_upload_data_set_id()

    total_candidates = len(candidates)
    upload_results: list[_FileUploadResult] = []
    for i, candidate in enumerate(candidates, 1):
        upload_results.append(
            _upload_candidate(
                candidate, integration_external_id, cdf_client, run_id=ctx.external_id, data_set_id=data_set_id
            )
        )
        ctx.report_progress(f"Uploading: {i}/{total_candidates} files complete")

    counts = Counter(r.status for r in upload_results)

    # Per-file entries: upload results (sorted by date) + missing dates (no usable file found by the candidate builder).
    files_list: list[dict[str, str]] = []
    for r in upload_results:
        entry: dict[str, str] = {"date": str(r.log_date), "status": r.status}
        if r.file_id is not None:
            entry["id"] = str(r.file_id)
        if r.status != "uploaded" and r.size_bytes is not None:
            entry["size_bytes"] = str(r.size_bytes)
        if r.error:
            entry["error"] = r.error
        files_list.append(entry)
    files_list.extend({"date": str(missing_date), "status": "missing"} for missing_date in missing_dates)
    files_list.sort(key=lambda e: e["date"])

    uploaded = counts["uploaded"]
    result_message = f"{uploaded} of {num_days} log files uploaded to CDF Files"
    if uploaded == 0:
        result_message = f"No log files were uploaded ({result_message})"
    metadata: dict[str, str] = {
        "total_files": str(num_days),
        "uploaded_files": str(counts["uploaded"]),
        "missing_files": str(len(missing_dates)),
        "skipped_too_large_files": str(counts["skipped_too_large"]),
        "failed_files": str(counts["failed"]),
    }

    # The per-file breakdown is supplementary detail on top of the counts above. For date ranges
    # near MAX_DATE_RANGE_DAYS it can exceed CDF's 512-byte-per-metadata-value limit (each entry
    # embeds a full file_external_id), which would otherwise get the whole result rejected. Rather
    # than risk that, only attach it when it demonstrably fits — the aggregate counts always do,
    # regardless of range length, and full per-file detail has already been logged individually above.
    files_json = json.dumps(files_list, separators=(",", ":"))
    if len(files_json.encode("utf-8")) <= MAX_METADATA_VALUE_BYTES:
        metadata["files"] = files_json
    else:
        _logger.info(
            "fetch_logs: per-file detail (%d bytes) would exceed the %d-byte metadata value limit; omitting "
            "'files' from the reported result. Full per-file detail for this invocation is available above "
            "in the extractor's own logs.",
            len(files_json.encode("utf-8")),
            MAX_METADATA_VALUE_BYTES,
        )
        result_message += " (per-file detail omitted from result metadata — see extractor logs for full detail)"

    ctx.set_result(result_message, metadata=metadata, completed_with_errors=uploaded == 0)
