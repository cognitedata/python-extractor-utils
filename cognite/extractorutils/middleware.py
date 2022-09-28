import jq
from cognite.client.data_classes import Row

class JQMiddleware:


    def __init__(self, jq_rules: str) -> None:
        self._jq = jq.compile(jq_rules)

    def __call__(self, data: Row) -> dict:
        if not isinstance(data, Row):
            raise ValueError(f"Type {type(data).__name__} is not currently supported")
        
        data.columns = self._jq.input(data.columns).first()
        if not isinstance(data.columns, dict):
            raise ValueError("output of jq middleware must be a dict")
        print(data.columns)
        return data