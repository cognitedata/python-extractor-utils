from sys import platform

from cognite.client.data_classes import Row


class JQMiddleware:
    def __init__(self, jq_rules: str) -> None:
        if platform == "win32":
            raise Exception("Windows platform doesn't support jq bindings for Python yet")
        import jq

        self._jq = jq.compile(jq_rules)

    def __call__(self, data: Row) -> dict:
        if not isinstance(data, Row):
            raise ValueError(f"type {type(data).__name__} is not currently supported")

        data.columns = self._jq.input(data.columns).first()
        if not isinstance(data.columns, dict):
            raise ValueError("output of jq middleware must be a dict")

        return data
