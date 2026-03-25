"""Bearer token injection for authenticated requests."""

from __future__ import annotations

import httpx


class BearerAuth(httpx.Auth):
    """An httpx.Auth implementation that adds an Authorization: Bearer header.

    Usage::

        client = httpx.Client(auth=BearerAuth("my-secret-token"))
    """

    def __init__(self, token: str) -> None:
        self._token = token

    def auth_flow(self, request: httpx.Request):  # type: ignore[override]
        request.headers["Authorization"] = f"Bearer {self._token}"
        yield request
