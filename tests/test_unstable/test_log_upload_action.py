from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from cognite.extractorutils.unstable.core._dto import Action, ActionStatus, ActionUpdate
from cognite.extractorutils.unstable.core._log_upload_action import MAX_DATE_RANGE_DAYS
from cognite.extractorutils.unstable.core.actions import ActionContext, ActionError, CustomAction
from cognite.extractorutils.unstable.core.base import FullConfig

from .conftest import TestConfig, TestExtractor


def _make_extractor() -> TestExtractor:
    conn = MagicMock()
    conn.integration.external_id = "test-integration"
    full_config = FullConfig(
        connection_config=conn,
        application_config=TestConfig(parameter_one=1, parameter_two="a"),
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
    ],
    ids=["end_before_start", "exceeds_max_days"],
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


@pytest.mark.parametrize(
    "start_str,end_str",
    [
        ("2026-06-10", "2026-06-10"),
        ("2026-06-01", str(date(2026, 6, 1) + timedelta(days=MAX_DATE_RANGE_DAYS - 1))),
    ],
    ids=["single_day", "exact_max_days"],
)
def test_valid_date_range_succeeds(start_str: str, end_str: str) -> None:
    extractor = _make_extractor()
    updates = _dispatch(extractor, {"start_date": start_str, "end_date": end_str})
    statuses = [u.status for u in updates]
    assert ActionStatus.succeeded in statuses
    assert ActionStatus.failed not in statuses


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
