"""
An example extractor that logs messages at various levels.
"""

from cognite.extractorutils.unstable.configuration.models import ExtractorConfig, IntervalConfig, TimeIntervalConfig
from cognite.extractorutils.unstable.core.base import Extractor, StartupTask, TaskContext
from cognite.extractorutils.unstable.core.runtime import Runtime
from cognite.extractorutils.unstable.core.tasks import ScheduledTask


class SimpleConfig(ExtractorConfig):
    """
    Defines the configuration for the SimpleExtractor.
    """

    pass


class SimpleExtractor(Extractor[SimpleConfig]):
    """
    An example extractor that logs messages at various levels.
    """

    NAME = "SimpleTestExtractor"
    EXTERNAL_ID = "test-extractor"
    DESCRIPTION = "An extractor for testing log levels"
    VERSION = "1.0.0"
    CONFIG_TYPE = SimpleConfig
    SUPPORTS_DRY_RUN = True

    def __init_tasks__(self) -> None:
        """
        Initializes and adds tasks to the extractor.
        """
        self.add_task(StartupTask(name="main_task", target=self.run_my_task))
        self.add_task(
            ScheduledTask(
                name="scheduled_task",
                target=self.scheduled_task,
                schedule=IntervalConfig(type="interval", expression=TimeIntervalConfig("3s")),
            )
        )

    # example task that logs messages at different levels
    def run_my_task(self, ctx: TaskContext) -> None:
        """
        An example task that logs messages at different levels.

        Args:
            ctx: The context for the task execution, used for logging.
        """
        ctx.debug("This is a detailed debug message.")
        ctx.info("This is an informational message.")
        ctx.warning("This is a warning message.")
        ctx.info("Test finished.")

    def scheduled_task(self, ctx: TaskContext) -> None:
        """
        An example scheduled task that logs a message.

        Args:
            ctx: The context for the task execution, used for logging.
        """
        ctx.info("This is a scheduled task running.")
        ctx.warning("This is a warning from the scheduled task.")
        ctx.debug("Debugging the scheduled task execution.")
        ctx.error("This is an error message from the scheduled task.")

    # add more tasks as needed


def main() -> None:
    """
    Main function to run the SimpleExtractor.
    """
    runtime = Runtime(SimpleExtractor)
    runtime.run()


if __name__ == "__main__":
    main()
