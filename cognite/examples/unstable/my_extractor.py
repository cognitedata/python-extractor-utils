from cognite.extractorutils.unstable.configuration.models import ExtractorConfig, IntervalConfig, TimeIntervalConfig
from cognite.extractorutils.unstable.core.base import Extractor, StartupTask, TaskContext
from cognite.extractorutils.unstable.core.runtime import Runtime
from cognite.extractorutils.unstable.core.tasks import ScheduledTask


class MyConfig(ExtractorConfig):
    pass


class MyExtractor(Extractor[MyConfig]):
    NAME = "MyTestExtractor"
    EXTERNAL_ID = "test-extractor"
    DESCRIPTION = "An extractor for testing log levels"
    VERSION = "1.0.0"
    CONFIG_TYPE = MyConfig
    SUPPORTS_DRY_RUN = True

    def __init_tasks__(self) -> None:
        self.add_task(StartupTask(name="main_task", target=self.run_my_task))
        self.add_task(
            ScheduledTask(
                name="scheduled_task",
                target=self.scheduled_example_task,
                schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig("5s")),
            )
        )
        pass

    def run_my_task(self, ctx: TaskContext) -> None:
        ctx.debug("This is a detailed debug message.")
        ctx.info("This is an informational message.")
        ctx.warning("This is a warning message.")
        ctx.info("Test finished.")

    def scheduled_example_task(self, ctx: TaskContext) -> None:
        ctx.info("This is a scheduled task running every 5 seconds.")
        ctx.warning("This is a warning from the scheduled task.")
        ctx.error("This is an error from the scheduled task.")


def main() -> None:
    runtime = Runtime(MyExtractor)
    runtime.run()


if __name__ == "__main__":
    main()
