"""HTTP connection wrapper with automatic retry and exponential back-off."""

from __future__ import annotations

import time
from typing import Any

import httpx

from oxendb._internal.auth import BearerAuth
from oxendb.exceptions import (
    AuthenticationError,
    BadRequestError,
    ConnectionError,
    NotFoundError,
    QuerySyntaxError,
    ServerError,
    TimeoutError,
)


_DEFAULT_TIMEOUT = 30.0  # seconds
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_BACKOFF_BASE = 0.5  # seconds


class Connection:
    """A thin wrapper around an httpx.Client that adds retry, back-off, and
    structured error mapping.

    Args:
        base_url: The root URL of the OxenDB server (e.g. ``"http://localhost:8080"``).
        token:    Optional bearer authentication token.
        timeout:  Request timeout in seconds.
        max_retries: Maximum number of retry attempts for transient errors.
        backoff_base: Initial back-off delay (doubles each retry).
    """

    def __init__(
        self,
        base_url: str,
        token: str | None = None,
        timeout: float = _DEFAULT_TIMEOUT,
        max_retries: int = _DEFAULT_MAX_RETRIES,
        backoff_base: float = _DEFAULT_BACKOFF_BASE,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_base = backoff_base

        auth = BearerAuth(token) if token else None
        self._client = httpx.Client(
            base_url=self._base_url,
            timeout=timeout,
            auth=auth,
            headers={"User-Agent": "oxendb-python/0.1.0", "Accept": "application/json"},
            follow_redirects=True,
        )

    # ---- public interface ----

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> httpx.Response:
        return self._request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> httpx.Response:
        return self._request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> httpx.Response:
        return self._request("DELETE", path, **kwargs)

    def close(self) -> None:
        """Close the underlying httpx transport."""
        self._client.close()

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ---- internal ----

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Send an HTTP request with retry logic for transient failures."""
        url = path if path.startswith("http") else path
        attempt = 0
        delay = self._backoff_base

        while True:
            attempt += 1
            try:
                response = self._client.request(method, url, **kwargs)
                self._raise_for_status(response)
                return response

            except httpx.TimeoutException as exc:
                if attempt > self._max_retries:
                    raise TimeoutError(
                        f"request to {method} {path} timed out after {self._timeout}s"
                    ) from exc
                time.sleep(delay)
                delay *= 2
                continue

            except httpx.NetworkError as exc:
                if attempt > self._max_retries:
                    raise ConnectionError(
                        f"network error on {method} {path}: {exc}"
                    ) from exc
                time.sleep(delay)
                delay *= 2
                continue

            except (
                AuthenticationError,
                NotFoundError,
                BadRequestError,
                QuerySyntaxError,
                ServerError,
            ):
                # These are definitive errors — do not retry
                raise

    def _raise_for_status(self, response: httpx.Response) -> None:
        """Map HTTP status codes to typed exceptions."""
        if response.status_code < 400:
            return

        body: dict[str, Any] = {}
        try:
            body = response.json()
        except Exception:
            pass

        code = body.get("code", "")
        message = body.get("message", response.text)

        if response.status_code == 401:
            raise AuthenticationError(message, code="unauthorized")
        if response.status_code == 404:
            raise NotFoundError(message)
        if response.status_code == 400:
            if "parse error" in message.lower() or "syntax" in message.lower():
                raise QuerySyntaxError(message, code=code)
            raise BadRequestError(message, code=code)
        if response.status_code >= 500:
            raise ServerError(message, code=code)

        raise OxenDBError(message, code=code)  # type: ignore[name-defined]
