"""Pydantic v2 models for OxenDB API request/response payloads."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class KVPair(BaseModel):
    """A single key-value pair as returned by GET or SCAN."""

    key: str
    value: str = Field(description="Base64-encoded raw value bytes")

    def decode_value(self) -> bytes:
        """Return the raw decoded bytes for the value."""
        import base64
        return base64.b64decode(self.value)

    def decode_value_str(self, encoding: str = "utf-8") -> str:
        """Decode the value bytes as a string."""
        return self.decode_value().decode(encoding, errors="replace")


class ScanResult(BaseModel):
    """The result of a SCAN or range-scan operation."""

    pairs: list[KVPair] = Field(default_factory=list)
    message: str = ""

    @property
    def count(self) -> int:
        return len(self.pairs)

    def as_dict(self, encoding: str = "utf-8") -> dict[str, str]:
        """Return the scan result as a {key: decoded_value} dict."""
        return {p.key: p.decode_value_str(encoding) for p in self.pairs}


class BatchOp(BaseModel):
    """One operation inside a batch write request."""

    op: str = Field(description="Operation type: 'put' or 'delete'")
    key: str
    value: str = Field(default="", description="Value for put operations (raw string)")

    @classmethod
    def put(cls, key: str, value: str | bytes) -> "BatchOp":
        """Convenience constructor for a put operation."""
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        return cls(op="put", key=key, value=value)

    @classmethod
    def delete(cls, key: str) -> "BatchOp":
        """Convenience constructor for a delete operation."""
        return cls(op="delete", key=key)


class BatchResult(BaseModel):
    """The result of a batch write operation."""

    applied: int
    message: str = ""


class AdminStats(BaseModel):
    """Server stats returned by GET /v1/admin/stats."""

    goroutines: int = 0
    heap_alloc_mb: float = 0.0
    sys_mb: float = 0.0
    gc_runs: int = 0
    db_status: str = "unknown"


class QueryResult(BaseModel):
    """The result of executing an OxenQL query."""

    message: str = ""
    key: str | None = None
    value: str | None = None  # base64-encoded
    pairs: list[KVPair] = Field(default_factory=list)

    def decode_value(self) -> bytes | None:
        """Decode the single-value response (for GET queries)."""
        if self.value is None:
            return None
        import base64
        return base64.b64decode(self.value)

    def decode_value_str(self, encoding: str = "utf-8") -> str | None:
        """Decode the single-value response as a string."""
        raw = self.decode_value()
        if raw is None:
            return None
        return raw.decode(encoding, errors="replace")

    def as_dict(self, encoding: str = "utf-8") -> dict[str, str]:
        """Return SCAN query pairs as a {key: decoded_value} dict."""
        return {p.key: p.decode_value_str(encoding) for p in self.pairs}


class ServerEnvelope(BaseModel):
    """Generic envelope wrapping all server responses."""

    status: str
    message: str = ""
    data: Any = None


class ErrorEnvelope(BaseModel):
    """JSON body returned for error responses."""

    status: str
    code: str = ""
    message: str = ""
