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

    def __init__(self, message: str, details: list[str] | None = None):
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
