"""BatchWriter — a context manager for accumulating and flushing batch operations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from oxendb.models import BatchOp, BatchResult

if TYPE_CHECKING:
    from oxendb.client import OxenDBClient


class BatchWriter:
    """Accumulates put/delete operations and flushes them in a single HTTP request.

    Use as a context manager::

        with client.batch() as batch:
            batch.put("key1", "value1")
            batch.put("key2", b"binary value")
            batch.delete("old-key")
        # Flushed automatically on __exit__

    Or flush manually::

        batch = client.batch()
        batch.put("k", "v")
        result = batch.flush()

    Args:
        client:     The OxenDBClient to flush through.
        auto_flush: If True, flush automatically when the context manager exits.
                    If an exception is raised, the flush is skipped.
        chunk_size: Maximum number of operations per HTTP request.
                    Larger batches are automatically split.
    """

    def __init__(
        self,
        client: "OxenDBClient",
        auto_flush: bool = True,
        chunk_size: int = 1000,
    ) -> None:
        self._client = client
        self._auto_flush = auto_flush
        self._chunk_size = chunk_size
        self._ops: list[BatchOp] = []
        self._results: list[BatchResult] = []

    # ---- public interface ----

    def put(self, key: str, value: str | bytes) -> "BatchWriter":
        """Queue a put operation."""
        self._ops.append(BatchOp.put(key, value))
        return self

    def delete(self, key: str) -> "BatchWriter":
        """Queue a delete operation."""
        self._ops.append(BatchOp.delete(key))
        return self

    @property
    def pending_count(self) -> int:
        """Number of operations queued but not yet flushed."""
        return len(self._ops)

    @property
    def results(self) -> list[BatchResult]:
        """BatchResult objects from all flushes performed so far."""
        return self._results

    def flush(self) -> BatchResult:
        """Send all pending operations to the server as a batch.

        Ops are split into chunks of ``chunk_size`` and sent as separate
        requests if the total count exceeds the chunk limit.

        Returns:
            A BatchResult summarising the last chunk (or a combined summary).

        Raises:
            BatchError: If any individual chunk request fails.
        """
        if not self._ops:
            return BatchResult(applied=0, message="no ops to flush")

        total_applied = 0
        ops_to_send = list(self._ops)
        self._ops.clear()

        for i in range(0, len(ops_to_send), self._chunk_size):
            chunk = ops_to_send[i : i + self._chunk_size]
            result = self._client._send_batch(chunk)
            total_applied += result.applied
            self._results.append(result)

        return BatchResult(
            applied=total_applied,
            message=f"flushed {total_applied} ops in {len(self._results)} request(s)",
        )

    def clear(self) -> None:
        """Discard all pending operations without sending them."""
        self._ops.clear()

    # ---- context manager ----

    def __enter__(self) -> "BatchWriter":
        return self

    def __exit__(self, exc_type: type | None, exc_val: BaseException | None, exc_tb: object) -> None:
        if exc_type is None and self._auto_flush and self._ops:
            self.flush()
