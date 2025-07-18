"""
An example extractor that logs messages at various levels.
"""

from cognite.extractorutils.unstable.configuration.models import ExtractorConfig
from cognite.extractorutils.unstable.core.base import Extractor, StartupTask, TaskContext
from cognite.extractorutils.unstable.core.runtime import Runtime


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

    # add more tasks as needed


if __name__ == "__main__":
    runtime = Runtime(SimpleExtractor)
    runtime.run()
