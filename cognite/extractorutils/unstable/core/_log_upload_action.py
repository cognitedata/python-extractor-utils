"""Built-in ``fetch_logs`` action: streams rotated log files to CDF Files."""

from datetime import date

from cognite.extractorutils.unstable.core.actions import ActionContext, ActionError

MAX_DATE_RANGE_DAYS = 7
"""Maximum number of calendar days a single ``fetch_logs`` invocation may cover."""

_FETCH_LOGS_DESCRIPTION = (
    f"Upload rotated log files to CDF Files for a given date range. At most {MAX_DATE_RANGE_DAYS} days per invocation."
)


def _parse_date(raw: str, field: str) -> date:
    try:
        return date.fromisoformat(raw)
    except (ValueError, TypeError):
        raise ActionError(
            f"Invalid {field} '{raw}': expected ISO 8601 date (YYYY-MM-DD)",
            error_type="invalid_parameter",
        ) from None


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

    num_days = (end_date - start_date).days + 1
    if num_days > MAX_DATE_RANGE_DAYS:
        raise ActionError(
            f"Date range of {num_days} days exceeds the maximum of {MAX_DATE_RANGE_DAYS} days; "
            "use multiple invocations for longer spans",
            error_type="invalid_date_range",
        )
