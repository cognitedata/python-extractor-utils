from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.unstable.configuration.models import (
    LogFileHandlerConfig,
    LogLevel,
)
from cognite.extractorutils.unstable.core._dto import Action, ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core._log_upload_action import (
    MAX_DATE_RANGE_DAYS,
    _build_candidate_files,
    _resolve_log_file_path,
)
from cognite.extractorutils.unstable.core.actions import ActionContext, ActionError, CustomAction
from cognite.extractorutils.unstable.core.base import FullConfig

from .conftest import TestConfig, TestExtractor

_PAST_TODAY = date(2026, 6, 19)


def _make_extractor(log_path: Path | None = None) -> TestExtractor:
    conn = MagicMock()
    conn.integration.external_id = "test-integration"
    config_kwargs: dict = {"parameter_one": 1, "parameter_two": "a"}
    if log_path is not None:
        config_kwargs["log_handlers"] = [LogFileHandlerConfig(type="file", path=log_path, level=LogLevel.INFO)]
    full_config = FullConfig(
        connection_config=conn,
        application_config=TestConfig(**config_kwargs),
        current_config_revision=1,
    )
    return TestExtractor(full_config, MagicMock())


def _queued_updates(extractor: TestExtractor) -> list[ActionUpdate]:
    return [c[0][0] for c in extractor._checkin_worker.queue_action_update.call_args_list]


def _dispatch(extractor: TestExtractor, call_metadata: dict[str, str] | None) -> list[ActionUpdate]:
    action = Action(
        external_id="act-1",
        action_name="fetch_logs",
        status=ActionStatus.pending,
        call_metadata=call_metadata,
    )
    extractor._dispatch_single_action(action)
    return _queued_updates(extractor)


def _failed_update(updates: list[ActionUpdate]) -> ActionUpdate:
    return next(u for u in updates if u.status == ActionStatus.failed)


def test_fetch_logs_registered_as_builtin_with_description() -> None:
    extractor = _make_extractor()
    action = next((a for a in extractor._custom_actions if a.name == "fetch_logs"), None)
    assert action is not None
    assert action.description


def test_registering_fetch_logs_as_user_action_raises() -> None:
    extractor = _make_extractor()
    with pytest.raises(ValueError, match="fetch_logs"):
        extractor.add_action(CustomAction(name="fetch_logs", target=lambda ctx: None))


@pytest.mark.parametrize(
    "call_metadata,missing_field",
    [
        (None, "start_date"),
        ({"end_date": "2026-06-10"}, "start_date"),
        ({"start_date": "2026-06-10"}, "end_date"),
    ],
    ids=["no_metadata", "missing_start_date", "missing_end_date"],
)
def test_missing_required_date_reports_missing_parameter(
    call_metadata: dict[str, str] | None, missing_field: str
) -> None:
    extractor = _make_extractor()
    updates = _dispatch(extractor, call_metadata)
    failed = _failed_update(updates)
    assert failed.result_metadata == {"error_type": "missing_parameter"}
    assert missing_field in (failed.result_message or "")


@pytest.mark.parametrize(
    "call_metadata,bad_field",
    [
        ({"start_date": "not-a-date", "end_date": "2026-06-10"}, "start_date"),
        ({"start_date": "2026-06-10", "end_date": "2026/06/11"}, "end_date"),
        ({"start_date": "2026-13-01", "end_date": "2026-06-11"}, "start_date"),
    ],
    ids=["invalid_start", "slash_end", "out_of_range_month"],
)
def test_non_iso_date_reports_invalid_parameter(call_metadata: dict[str, str], bad_field: str) -> None:
    extractor = _make_extractor()
    updates = _dispatch(extractor, call_metadata)
    failed = _failed_update(updates)
    assert failed.result_metadata == {"error_type": "invalid_parameter"}
    assert bad_field in (failed.result_message or "")


def test_parse_date_non_string_type_raises_action_error() -> None:
    # Pydantic guards dict[str, str] at the DTO boundary, but _parse_date may be called
    # directly, so TypeError from date.fromisoformat must also surface as ActionError.
    from cognite.extractorutils.unstable.core._log_upload_action import _parse_date

    with pytest.raises(ActionError) as exc_info:
        _parse_date(20260610, "start_date")  # type: ignore[arg-type]
    assert exc_info.value.error_type == "invalid_parameter"


@pytest.mark.parametrize(
    "call_metadata,message_contains",
    [
        ({"start_date": "2026-06-10", "end_date": "2026-06-09"}, None),
        (
            {
                "start_date": str(date(2026, 6, 1)),
                "end_date": str(date(2026, 6, 1) + timedelta(days=MAX_DATE_RANGE_DAYS)),
            },
            str(MAX_DATE_RANGE_DAYS),
        ),
        ({"start_date": "2020-01-01", "end_date": "2099-01-01"}, "future"),
    ],
    ids=["end_before_start", "exceeds_max_days", "end_date_in_future"],
)
def test_invalid_date_range_reports_invalid_date_range(
    call_metadata: dict[str, str], message_contains: str | None
) -> None:
    extractor = _make_extractor()
    updates = _dispatch(extractor, call_metadata)
    failed = _failed_update(updates)
    assert failed.result_metadata == {"error_type": "invalid_date_range"}
    if message_contains:
        assert message_contains in (failed.result_message or "")


def test_action_error_details_included_in_result_metadata() -> None:
    extractor = _make_extractor()

    def raise_with_details(ctx: ActionContext) -> None:
        raise ActionError("boom", error_type="unexpected_error", details="inner detail")

    extractor.add_action(CustomAction(name="boom-action", target=raise_with_details))
    action = Action(external_id="act-detail", action_name="boom-action", status=ActionStatus.pending)
    extractor._dispatch_single_action(action)
    failed = _failed_update(_queued_updates(extractor))
    assert failed.result_metadata == {"error_type": "unexpected_error", "error_detail": "inner detail"}
    assert failed.result_message == "boom"


def test_resolve_log_file_path_returns_none_for_console_only_config() -> None:
    config = TestConfig(parameter_one=1, parameter_two="a")
    assert _resolve_log_file_path(config) is None


@pytest.mark.parametrize(
    "handler_names,expected_name",
    [
        (["extractor.log"], "extractor.log"),
        (["first.log", "second.log"], "first.log"),
    ],
    ids=["single_handler", "multiple_handlers_returns_first"],
)
def test_resolve_log_file_path_with_file_handlers(tmp_path: Path, handler_names: list[str], expected_name: str) -> None:
    config = TestConfig(
        parameter_one=1,
        parameter_two="a",
        log_handlers=[LogFileHandlerConfig(type="file", path=tmp_path / n, level=LogLevel.INFO) for n in handler_names],
    )
    assert _resolve_log_file_path(config) == tmp_path / expected_name


def test_existing_rotated_files_become_candidates(tmp_path: Path) -> None:
    base = tmp_path / "extractor.log"
    start = date(2026, 6, 1)
    end = date(2026, 6, 3)
    for d in [start, start + timedelta(days=1), end]:
        (tmp_path / f"extractor.log.{d.isoformat()}").write_bytes(b"data")
    candidates, skipped = _build_candidate_files(base, start, end, _PAST_TODAY)
    assert len(candidates) == 3
    assert skipped == []
    assert all(not c.is_current for c in candidates)


@pytest.mark.parametrize(
    "create_file",
    [False, True],
    ids=["missing", "empty"],
)
def test_unusable_file_goes_to_skipped(tmp_path: Path, create_file: bool) -> None:
    # Covers missing files (including the rotation race window) and 0-byte files.
    base = tmp_path / "extractor.log"
    d = date(2026, 6, 1)
    if create_file:
        (tmp_path / f"extractor.log.{d.isoformat()}").touch()  # 0 bytes
    candidates, skipped = _build_candidate_files(base, d, d, _PAST_TODAY)
    assert candidates == []
    assert skipped == [d]


def test_today_uses_live_file_not_rotated_name(tmp_path: Path) -> None:
    base = tmp_path / "extractor.log"
    base.write_bytes(b"today data")
    candidates, skipped = _build_candidate_files(base, _PAST_TODAY, _PAST_TODAY, _PAST_TODAY)
    assert len(candidates) == 1
    assert candidates[0].path == base
    assert candidates[0].is_current is True
    assert skipped == []


def test_mixed_range_partitions_correctly(tmp_path: Path) -> None:
    base = tmp_path / "extractor.log"
    start = date(2026, 6, 1)
    end = date(2026, 6, 3)
    (tmp_path / "extractor.log.2026-06-01").write_bytes(b"data")
    (tmp_path / "extractor.log.2026-06-03").write_bytes(b"data")
    candidates, skipped = _build_candidate_files(base, start, end, _PAST_TODAY)
    assert len(candidates) == 2
    assert skipped == [date(2026, 6, 2)]


@pytest.mark.parametrize(
    "start,end",
    [
        (_PAST_TODAY, _PAST_TODAY),
        (date(2026, 6, 1), date(2026, 6, 1) + timedelta(days=MAX_DATE_RANGE_DAYS - 1)),
    ],
    ids=["single_today", "exact_max_historical"],
)
def test_boundary_ranges_produce_correct_candidate_count(tmp_path: Path, start: date, end: date) -> None:
    base = tmp_path / "extractor.log"
    expected = (end - start).days + 1
    if start == _PAST_TODAY:
        base.write_bytes(b"data")
    else:
        current = start
        while current <= end:
            (tmp_path / f"extractor.log.{current.isoformat()}").write_bytes(b"data")
            current += timedelta(days=1)
    candidates, skipped = _build_candidate_files(base, start, end, _PAST_TODAY)
    assert len(candidates) == expected
    assert skipped == []


def test_no_file_handler_reports_no_file_handler_configured() -> None:
    extractor = _make_extractor()
    updates = _dispatch(extractor, {"start_date": "2026-06-01", "end_date": "2026-06-07"})
    failed = _failed_update(updates)
    assert failed.result_metadata == {"error_type": "no_file_handler_configured"}


@pytest.mark.parametrize(
    "create_log_file",
    [True, False],
    ids=["files_present", "all_files_missing"],
)
def test_fetch_logs_action_with_file_handler_succeeds(tmp_path: Path, create_log_file: bool) -> None:
    log_path = tmp_path / "extractor.log"
    if create_log_file:
        (tmp_path / "extractor.log.2026-06-10").write_bytes(b"log data")
    extractor = _make_extractor(log_path=log_path)
    updates = _dispatch(extractor, {"start_date": "2026-06-10", "end_date": "2026-06-10"})
    statuses = [u.status for u in updates]
    assert ActionStatus.succeeded in statuses
    assert ActionStatus.failed not in statuses


def test_valid_exact_max_days_range_succeeds_at_dispatch(tmp_path: Path) -> None:
    # Exactly MAX_DATE_RANGE_DAYS is the boundary — one more would be rejected.
    start = date(2026, 6, 1)
    end = start + timedelta(days=MAX_DATE_RANGE_DAYS - 1)
    log_path = tmp_path / "extractor.log"
    extractor = _make_extractor(log_path=log_path)
    updates = _dispatch(extractor, {"start_date": str(start), "end_date": str(end)})
    statuses = [u.status for u in updates]
    assert ActionStatus.succeeded in statuses
    assert ActionStatus.failed not in statuses


def test_range_spanning_rotated_and_live_file(tmp_path: Path) -> None:
    # Covers the common case: start_date = yesterday (rotated file), end_date = today (live file).
    base = tmp_path / "extractor.log"
    yesterday = _PAST_TODAY - timedelta(days=1)
    base.write_bytes(b"today data")
    (tmp_path / f"extractor.log.{yesterday.isoformat()}").write_bytes(b"yesterday data")
    candidates, skipped = _build_candidate_files(base, yesterday, _PAST_TODAY, _PAST_TODAY)
    assert skipped == []
    assert len(candidates) == 2
    rotated = next(c for c in candidates if c.log_date == yesterday)
    live = next(c for c in candidates if c.log_date == _PAST_TODAY)
    assert rotated.is_current is False
    assert live.is_current is True
    assert live.path == base
