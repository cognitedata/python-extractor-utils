"""
This module is deprecated and will be removed in a future version
"""


from sys import platform
from typing import Any, Union

from cognite.client.data_classes import Row


class JQMiddleware:
    def __init__(self, jq_rules: str) -> None:
        if platform == "win32":
            raise Exception("Windows platform doesn't support jq bindings for Python yet")
        import jq  # type: ignore

        self._jq = jq.compile(jq_rules)

    def __call__(self, data: Union[Row, dict]) -> Union[Row, dict]:
        if not isinstance(data, (Row, dict)):
            raise ValueError(f"type {type(data).__name__} is not currently supported")

        if isinstance(data, Row):
            data.columns = self._jq.input(data.columns).first()
            self._raise_for_non_dict(data.columns)

        if isinstance(data, dict):
            data = self._jq.input(data).first()
            self._raise_for_non_dict(data)

        return data

    def _raise_for_non_dict(self, data: Any) -> None:
        if not isinstance(data, dict):
            raise ValueError("output of jq middleware must be a dict")
