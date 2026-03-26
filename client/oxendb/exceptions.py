"""Exception hierarchy for the OxenDB Python client SDK."""

from __future__ import annotations


class OxenDBError(Exception):
    """Base class for all OxenDB client errors."""

    def __init__(self, message: str, code: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.code = code

    def __repr__(self) -> str:
        return f"{type(self).__name__}(message={self.message!r}, code={self.code!r})"


class ConnectionError(OxenDBError):
    """Raised when the client cannot connect to the server."""


class AuthenticationError(OxenDBError):
    """Raised when the server rejects the authentication token."""


class NotFoundError(OxenDBError):
    """Raised when a requested key does not exist in the database."""

    def __init__(self, key: str | bytes) -> None:
        key_str = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key
        super().__init__(f"key not found: {key_str!r}", code="not_found")
        self.key = key


class BadRequestError(OxenDBError):
    """Raised when the server rejects the request as malformed."""


class ServerError(OxenDBError):
    """Raised when the server returns an unexpected 5xx error."""


class TimeoutError(OxenDBError):
    """Raised when a request exceeds the configured timeout."""


class QuerySyntaxError(OxenDBError):
    """Raised when an OxenQL query cannot be parsed by the server."""


class BatchError(OxenDBError):
    """Raised when one or more operations in a batch fail."""

    def __init__(self, message: str, failed_index: int | None = None) -> None:
        super().__init__(message, code="batch_error")
        self.failed_index = failed_index
