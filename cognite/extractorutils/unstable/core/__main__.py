"""
Example of how you would build an extractor with the new base class
"""

from cognite.extractorutils.unstable.configuration.models import ExtractorConfig

from .base import Extractor
from .runtime import Runtime


class MyConfig(ExtractorConfig):
    parameter_one: int
    parameter_two: str


class MyExtractor(Extractor[MyConfig]):
    NAME = "Test extractor"
    EXTERNAL_ID = "test-extractor"
    DESCRIPTION = "Test of the new runtime"
    VERSION = "1.0.0"
    CONFIG_TYPE = MyConfig

    def run(self) -> None:
        self.logger.info("Started!")
        if not self.cancellation_token.wait(10):
            raise ValueError("Oops")


if __name__ == "__main__":
    runtime = Runtime(MyExtractor)
    runtime.run()
