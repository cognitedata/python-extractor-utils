import re
from datetime import timedelta
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, Dict, List, Literal, Optional, Union

from humps import kebabize
from pydantic import BaseModel, ConfigDict, Field, GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema

from cognite.extractorutils.exceptions import InvalidConfigError


class ConfigModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=kebabize,
        populate_by_name=True,
        extra="forbid",
        # arbitrary_types_allowed=True,
    )


class _ClientCredentialsConfig(ConfigModel):
    type: Literal["client-credentials"]
    client_id: str
    client_secret: str
    token_url: str
    scopes: List[str]
    resource: Optional[str] = None
    audience: Optional[str] = None


class _ClientCertificateConfig(ConfigModel):
    type: Literal["client-certificate"]
    client_id: str
    certificate_path: Path
    scopes: List[str]


AuthenticationConfig = Annotated[Union[_ClientCredentialsConfig, _ClientCertificateConfig], Field(discriminator="type")]


class TimeIntervalConfig:
    """
    Configuration parameter for setting a time interval
    """

    def __init__(self, expression: str) -> None:
        self._interval, self._expression = TimeIntervalConfig._parse_expression(expression)

    @classmethod
    def __get_pydantic_core_schema__(cls, source_type: Any, handler: GetCoreSchemaHandler) -> CoreSchema:
        return core_schema.no_info_after_validator_function(cls, handler(Union[str, int]))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TimeIntervalConfig):
            return NotImplemented
        return self._interval == other._interval

    def __hash__(self) -> int:
        return hash(self._interval)

    @classmethod
    def _parse_expression(cls, expression: str) -> tuple[int, str]:
        # First, try to parse pure number and assume seconds (for backwards compatibility)
        try:
            return int(expression), f"{expression}s"
        except ValueError:
            pass

        match = re.match(r"(\d+)[ \t]*(s|m|h|d)", expression)
        if not match:
            raise InvalidConfigError("Invalid interval pattern")

        number, unit = match.groups()
        numeric_unit = {"s": 1, "m": 60, "h": 60 * 60, "d": 60 * 60 * 24}[unit]

        return int(number) * numeric_unit, expression

    @property
    def seconds(self) -> int:
        return self._interval

    @property
    def minutes(self) -> float:
        return self._interval / 60

    @property
    def hours(self) -> float:
        return self._interval / (60 * 60)

    @property
    def days(self) -> float:
        return self._interval / (60 * 60 * 24)

    @property
    def timedelta(self) -> timedelta:
        days = self._interval // (60 * 60 * 24)
        seconds = self._interval % (60 * 60 * 24)
        return timedelta(days=days, seconds=seconds)

    def __int__(self) -> int:
        return int(self._interval)

    def __float__(self) -> float:
        return float(self._interval)

    def __str__(self) -> str:
        return self._expression

    def __repr__(self) -> str:
        return self._expression


class _ConnectionParameters(ConfigModel):
    gzip_compression: bool = False
    status_forcelist: List[int] = Field(default_factory=lambda: [429, 502, 503, 504])
    max_retries: int = 10
    max_retries_connect: int = 3
    max_retry_backoff: TimeIntervalConfig = Field(default_factory=lambda: TimeIntervalConfig("30s"))
    max_connection_pool_size: int = 50
    ssl_verify: bool = True
    proxies: Dict[str, str] = Field(default_factory=dict)


class ConnectionConfig(ConfigModel):
    project: str
    base_url: str

    extraction_pipeline: str

    authentication: AuthenticationConfig

    connection: _ConnectionParameters = Field(default_factory=_ConnectionParameters)


class LogLevel(Enum):
    CRITICAL = "CRITICAL"
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"
    DEBUG = "DEBUG"


class LogFileHandlerConfig(ConfigModel):
    path: Path
    level: LogLevel
    retention: int = 7


class LogConsoleHandlerConfig(ConfigModel):
    level: LogLevel


LogHandlerConfig = Union[LogFileHandlerConfig, LogConsoleHandlerConfig]


class ExtractorConfig(ConfigModel):
    log_handlers: List[LogHandlerConfig] = Field(default_factory=lambda: [LogConsoleHandlerConfig(level=LogLevel.INFO)])
