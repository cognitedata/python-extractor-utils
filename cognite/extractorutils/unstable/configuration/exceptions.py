from typing import List, Optional


class InvalidConfigError(Exception):
    """
    Exception thrown from ``load_yaml`` and ``load_yaml_dict`` if config file is invalid. This can be due to

      * Missing fields
      * Incompatible types
      * Unkown fields
    """

    def __init__(self, message: str, details: Optional[List[str]] = None):
        super(InvalidConfigError, self).__init__()
        self.message = message
        self.details = details

        self.attempted_revision: int | None = None

    def __str__(self) -> str:
        return f"Invalid config: {self.message}"

    def __repr__(self) -> str:
        return self.__str__()
