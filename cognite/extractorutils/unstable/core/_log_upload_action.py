"""Built-in ``fetch_logs`` action: streams rotated log files to CDF Files."""

import logging
from dataclasses import dataclass
from datetime import date, timedelta, timezone
from datetime import datetime as dt
from pathlib import Path

from cognite.extractorutils.unstable.configuration.models import ExtractorConfig, LogFileHandlerConfig
from cognite.extractorutils.unstable.core.actions import ActionContext, ActionError

_logger = logging.getLogger(__name__)

MAX_DATE_RANGE_DAYS = 7
"""Maximum number of calendar days a single ``fetch_logs`` invocation may cover."""

_FETCH_LOGS_DESCRIPTION = (
    f"Upload rotated log files to CDF Files for a given date range. At most {MAX_DATE_RANGE_DAYS} days per invocation."
)


@dataclass(frozen=True)
class LogFileCandidate:
    """A log file resolved for a given date that exists and has content."""

    log_date: date
    path: Path
    is_current: bool  # True when path is the live (unrotated) file.log


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

    candidates, skipped = _build_candidate_files(log_file_path, start_date, end_date, today)
    _logger.info(
        "fetch_logs: %d candidate file(s) for %s to %s; %d date(s) skipped",
        len(candidates),
        start_date,
        end_date,
        len(skipped),
    )
