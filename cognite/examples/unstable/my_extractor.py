from cognite.extractorutils.unstable.configuration.models import ExtractorConfig
from cognite.extractorutils.unstable.core.base import Extractor, StartupTask, TaskContext
from cognite.extractorutils.unstable.core.runtime import Runtime


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

    def run_my_task(self, ctx: TaskContext) -> None:
        ctx.debug("This is a detailed debug message.")
        ctx.info("This is an informational message.")
        ctx.warning("This is a warning message.")
        ctx.info("Test finished.")


def main() -> None:
    runtime = Runtime(MyExtractor)
    runtime.run()


if __name__ == "__main__":
    main()
