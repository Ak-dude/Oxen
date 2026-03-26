"""Fluent OxenQL query builder.

Example usage::

    q = QueryBuilder().scan().from_key("a").to_key("z").limit(100)
    ql_string = q.build()
    # -> 'SCAN FROM "a" TO "z" LIMIT 100'

    q2 = QueryBuilder().get("mykey").build()
    # -> 'GET "mykey"'

    q3 = QueryBuilder().put("k", "v").build()
    # -> 'PUT "k" "v"'

    q4 = (
        QueryBuilder()
        .batch()
        .add_put("k1", "v1")
        .add_put("k2", "v2")
        .add_delete("old")
        .build()
    )
    # -> 'BATCH { PUT "k1" "v1" PUT "k2" "v2" DELETE "old" }'
"""

from __future__ import annotations

from typing import Optional


def _quote(s: str | bytes) -> str:
    """Wrap a string in double quotes, escaping inner quotes and backslashes."""
    if isinstance(s, bytes):
        s = s.decode("utf-8", errors="replace")
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


class QueryBuilder:
    """Entry point for building OxenQL query strings."""

    def get(self, key: str | bytes) -> "_GetBuilder":
        return _GetBuilder(key)

    def put(self, key: str | bytes, value: str | bytes) -> "_PutBuilder":
        return _PutBuilder(key, value)

    def delete(self, key: str | bytes) -> "_DeleteBuilder":
        return _DeleteBuilder(key)

    def scan(self) -> "_ScanBuilder":
        return _ScanBuilder()

    def batch(self) -> "_BatchBuilder":
        return _BatchBuilder()


class _GetBuilder:
    def __init__(self, key: str | bytes) -> None:
        self._key = key

    def build(self) -> str:
        return f"GET {_quote(self._key)}"


class _PutBuilder:
    def __init__(self, key: str | bytes, value: str | bytes) -> None:
        self._key = key
        self._value = value

    def build(self) -> str:
        return f"PUT {_quote(self._key)} {_quote(self._value)}"


class _DeleteBuilder:
    def __init__(self, key: str | bytes) -> None:
        self._key = key

    def build(self) -> str:
        return f"DELETE {_quote(self._key)}"


class _ScanBuilder:
    """Fluent builder for SCAN statements.

    Example::

        _ScanBuilder().from_key("a").to_key("z").limit(50).build()
        # -> 'SCAN FROM "a" TO "z" LIMIT 50'
    """

    def __init__(self) -> None:
        self._from: Optional[str | bytes] = None
        self._to: Optional[str | bytes] = None
        self._limit: Optional[int] = None

    def from_key(self, key: str | bytes) -> "_ScanBuilder":
        self._from = key
        return self

    def to_key(self, key: str | bytes) -> "_ScanBuilder":
        self._to = key
        return self

    def limit(self, n: int) -> "_ScanBuilder":
        if n < 0:
            raise ValueError("limit must be non-negative")
        self._limit = n
        return self

    def build(self) -> str:
        parts = ["SCAN"]
        if self._from is not None:
            parts.extend(["FROM", _quote(self._from)])
        if self._to is not None:
            parts.extend(["TO", _quote(self._to)])
        if self._limit is not None:
            parts.extend(["LIMIT", str(self._limit)])
        return " ".join(parts)


class _BatchOp:
    def __init__(self, op: str, key: str | bytes, value: str | bytes = "") -> None:
        self.op = op
        self.key = key
        self.value = value

    def render(self) -> str:
        if self.op == "PUT":
            return f"PUT {_quote(self.key)} {_quote(self.value)}"
        return f"DELETE {_quote(self.key)}"


class _BatchBuilder:
    """Fluent builder for BATCH statements.

    Example::

        _BatchBuilder().add_put("k", "v").add_delete("old").build()
        # -> 'BATCH { PUT "k" "v" DELETE "old" }'
    """

    def __init__(self) -> None:
        self._ops: list[_BatchOp] = []

    def add_put(self, key: str | bytes, value: str | bytes) -> "_BatchBuilder":
        self._ops.append(_BatchOp("PUT", key, value))
        return self

    def add_delete(self, key: str | bytes) -> "_BatchBuilder":
        self._ops.append(_BatchOp("DELETE", key))
        return self

    def build(self) -> str:
        if not self._ops:
            raise ValueError("batch must contain at least one operation")
        inner = " ".join(op.render() for op in self._ops)
        return f"BATCH {{ {inner} }}"
