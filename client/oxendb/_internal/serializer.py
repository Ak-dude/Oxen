"""Serialization helpers — base64 encode/decode for binary key-value payloads."""

from __future__ import annotations

import base64


def encode_value(data: bytes | str) -> str:
    """Encode raw bytes to a base64 string for transmission in JSON.

    Args:
        data: Raw bytes or a str (which will be UTF-8 encoded first).

    Returns:
        Standard base64-encoded string (no line breaks).
    """
    if isinstance(data, str):
        data = data.encode("utf-8")
    return base64.b64encode(data).decode("ascii")


def decode_value(encoded: str) -> bytes:
    """Decode a base64 string received from the server into raw bytes.

    Args:
        encoded: Standard base64-encoded string.

    Returns:
        Decoded raw bytes.

    Raises:
        ValueError: If `encoded` is not valid base64.
    """
    try:
        return base64.b64decode(encoded)
    except Exception as exc:
        raise ValueError(f"invalid base64 value: {encoded!r}") from exc


def decode_value_str(encoded: str, encoding: str = "utf-8") -> str:
    """Decode a base64 string into a Python str using the given encoding.

    Args:
        encoded: Standard base64-encoded string.
        encoding: Character encoding for the decoded bytes (default: utf-8).

    Returns:
        Decoded string.
    """
    raw = decode_value(encoded)
    return raw.decode(encoding, errors="replace")
