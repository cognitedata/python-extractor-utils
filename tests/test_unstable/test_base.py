from time import sleep

import pytest

from cognite.extractorutils.unstable.configuration.models import ConnectionConfig, IntervalConfig, TimeIntervalConfig
from cognite.extractorutils.unstable.core.base import FullConfig
from cognite.extractorutils.unstable.core.tasks import ScheduledTask
from cognite.extractorutils.util import now

from .conftest import MockFunction, TestConfig, TestExtractor


@pytest.mark.parametrize("checkin_between", [True, False])
def test_simple_task_report(
    connection_config: ConnectionConfig,
    application_config: TestConfig,
    checkin_between: bool,
) -> None:
    mock = MockFunction(5)

    # Create a simple test extractor
    extractor = TestExtractor(
        FullConfig(
            connection_config=connection_config,
            application_config=application_config,
            current_config_revision=1,
        )
    )

    extractor.add_task(
        ScheduledTask(
            name="TestTask",
            target=lambda _t: mock(),
            schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig("15m")),
        )
    )

    # Do parts of a startup routine
    start_time = now()
    extractor._report_extractor_info()

    assert extractor._task_updates == []

    # Manually trigger task, wait a bit to make sure it has started
    extractor._scheduler.trigger("TestTask")
    sleep(1)

    # Test that the start of the task was tracked correctly
    assert len(extractor._task_updates) == 1
    assert extractor._task_updates[0].type == "started"
    assert extractor._task_updates[0].name == "TestTask"
    assert start_time <= extractor._task_updates[0].timestamp < now()

    if checkin_between:
        assert len(extractor._task_updates) == 1
        extractor._checkin()
        # Check that the update queue is cleared on a successful checkin
        assert len(extractor._task_updates) == 0

    mid_way = now()

    sleep(5)

    if checkin_between:
        assert len(extractor._task_updates) == 1
    else:
        assert len(extractor._task_updates) == 2

    end_time = now()

    # Test that the end of the task was tracked correctly
    assert extractor._task_updates[-1].type == "ended"
    assert extractor._task_updates[-1].name == "TestTask"
    assert mid_way < extractor._task_updates[-1].timestamp < end_time

    # Make sure all the changes are checked in
    extractor._checkin()
    assert extractor._task_updates == []

    # Test that the task run is entered into the history for that task
    res = extractor.cognite_client.get(
        f"/api/v1/projects/{extractor.cognite_client.config.project}/integrations/history?integration={connection_config.integration.external_id}&taskName=TestTask",
        headers={"cdf-version": "alpha"},
    ).json()

    assert len(res["items"]) == 1
    assert res["items"][0]["taskName"] == "TestTask"
    assert res["items"][0]["errorCount"] == 0
    assert start_time <= res["items"][0]["startTime"] < mid_way
    assert mid_way < res["items"][0]["endTime"] < end_time
