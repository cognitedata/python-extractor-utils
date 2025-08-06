"""
Exceptions representing invalid configurations.
"""


class InvalidConfigError(Exception):
    """
    Exception thrown from ``load_yaml`` and ``load_yaml_dict`` if config file is invalid. This can be due to.

      * Missing fields
      * Incompatible types
      * Unknown fields
    """

    def __init__(self, message: str, details: list[str] | None = None) -> None:
        super().__init__()
        self.message = message
        self.details = details

        self.attempted_revision: int | None = None

    def __str__(self) -> str:
        """
        Underlying message prefixed with 'Invalid config:'.
        """
        return f"Invalid config: {self.message}"

    def __repr__(self) -> str:
        """
        Underlying message prefixed with 'Invalid config:'.
        """
        return self.__str__()


class InvalidArgumentError(Exception):
    """
    Exception thrown when an invalid argument is passed to the extractor.

    This can be due to:
      * Missing required arguments
      * Invalid argument types
      * Unsupported argument values
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        """
        Underlying message prefixed with 'Invalid argument:'.
        """
        return f"Invalid argument: {self.message}"

    def __repr__(self) -> str:
        """
        Underlying message prefixed with 'Invalid argument:'.
        """
        return self.__str__()
