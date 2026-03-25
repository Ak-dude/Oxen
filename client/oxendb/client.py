"""OxenDBClient — the primary interface for interacting with OxenDB."""

from __future__ import annotations

from typing import Any

from oxendb._internal.serializer import decode_value, decode_value_str
from oxendb.batch import BatchWriter
from oxendb.connection import Connection
from oxendb.exceptions import OxenDBError
from oxendb.models import (
    AdminStats,
    BatchOp,
    BatchResult,
    KVPair,
    QueryResult,
    ScanResult,
)
from oxendb.query_builder import QueryBuilder


class OxenDBClient:
    """High-level client for the OxenDB REST API.

    Args:
        base_url:   Base URL of the OxenDB server, e.g. ``"http://localhost:8080"``.
        token:      Optional bearer authentication token.
        timeout:    Request timeout in seconds (default: 30).
        max_retries: Number of retry attempts for transient network errors (default: 3).

    Example::

        client = OxenDBClient("http://localhost:8080", token="secret")
        client.put("hello", b"world")
        value = client.get("hello")
        print(value)  # b"world"
        client.close()
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 3,
    ) -> None:
        self._conn = Connection(
            base_url=base_url,
            token=token,
            timeout=timeout,
            max_retries=max_retries,
        )
        self.query = QueryBuilder()

    # ---- key-value operations ----

    def get(self, key: str) -> bytes:
        """Retrieve the raw bytes stored at ``key``.

        Args:
            key: The lookup key.

        Returns:
            The raw bytes value.

        Raises:
            NotFoundError: If the key does not exist.
            OxenDBError: On any other server or network error.
        """
        resp = self._conn.get(f"/v1/kv/{_encode_key(key)}")
        data: dict[str, Any] = resp.json().get("data", {})
        return decode_value(data["value"])

    def get_str(self, key: str, encoding: str = "utf-8") -> str:
        """Retrieve the value at ``key`` decoded as a string."""
        return self.get(key).decode(encoding, errors="replace")

    def put(self, key: str, value: str | bytes) -> None:
        """Store ``value`` under ``key``.

        Args:
            key:   The key to write.
            value: The raw bytes or UTF-8 string to store.
        """
        if isinstance(value, str):
            value = value.encode("utf-8")
        self._conn.put(f"/v1/kv/{_encode_key(key)}", content=value)

    def delete(self, key: str) -> None:
        """Delete the entry at ``key`` (idempotent).

        Args:
            key: The key to delete.
        """
        self._conn.delete(f"/v1/kv/{_encode_key(key)}")

    def scan(
        self,
        start: str | None = None,
        end: str | None = None,
        limit: int = 0,
    ) -> ScanResult:
        """Scan keys in the half-open range [start, end).

        Args:
            start: Inclusive lower bound key, or None for open-ended.
            end:   Exclusive upper bound key, or None for open-ended.
            limit: Maximum number of results to return (0 = no limit).

        Returns:
            A ScanResult with all matching key-value pairs.
        """
        builder = self.query.scan()
        if start is not None:
            builder = builder.from_key(start)
        if end is not None:
            builder = builder.to_key(end)
        if limit > 0:
            builder = builder.limit(limit)

        return self._execute_query(builder.build(), ScanResult)

    def query_raw(self, oxenql: str) -> QueryResult:
        """Execute a raw OxenQL query string and return the structured result.

        Args:
            oxenql: An OxenQL query string (e.g. ``'GET "mykey"'``).

        Returns:
            A QueryResult.

        Raises:
            QuerySyntaxError: If the server cannot parse the query.
        """
        resp = self._conn.post("/v1/query", json={"query": oxenql})
        data = resp.json().get("data", {})
        pairs = [KVPair(**p) for p in data.get("pairs", [])]
        return QueryResult(
            message=data.get("message", ""),
            key=data.get("key"),
            value=data.get("value"),
            pairs=pairs,
        )

    # ---- batch writes ----

    def batch(self, chunk_size: int = 1000) -> BatchWriter:
        """Return a BatchWriter context manager.

        Usage::

            with client.batch() as bw:
                bw.put("k1", "v1")
                bw.delete("k2")
        """
        return BatchWriter(self, auto_flush=True, chunk_size=chunk_size)

    def _send_batch(self, ops: list[BatchOp]) -> BatchResult:
        """Internal: flush a list of BatchOp objects to POST /v1/batch."""
        payload = {"ops": [op.model_dump() for op in ops]}
        resp = self._conn.post("/v1/batch", json=payload)
        data = resp.json().get("data", {})
        return BatchResult(
            applied=data.get("applied", 0),
            message=data.get("message", ""),
        )

    # ---- admin ----

    def stats(self) -> AdminStats:
        """Retrieve server runtime statistics.

        Returns:
            An AdminStats model with goroutines, heap usage, etc.
        """
        resp = self._conn.get("/v1/admin/stats")
        data = resp.json().get("data", {})
        return AdminStats(**data)

    def compact(self) -> str:
        """Trigger a synchronous compaction round in the storage engine.

        Returns:
            A status message from the server.
        """
        resp = self._conn.post("/v1/admin/compact")
        return resp.json().get("message", "ok")

    # ---- lifecycle ----

    def close(self) -> None:
        """Close the underlying HTTP connection pool."""
        self._conn.close()

    def __enter__(self) -> "OxenDBClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ---- private helpers ----

    def _execute_query(self, ql: str, result_type: type) -> Any:
        """Execute an OxenQL string and coerce the response into ``result_type``."""
        result = self.query_raw(ql)
        if result_type is ScanResult:
            return ScanResult(pairs=result.pairs, message=result.message)
        return result


def _encode_key(key: str) -> str:
    """URL-encode a key for embedding in the path segment."""
    from urllib.parse import quote
    return quote(key, safe="")
