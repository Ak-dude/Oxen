"""OxenDB Python client SDK.

Quick start::

    from oxendb import OxenDBClient

    with OxenDBClient("http://localhost:8080") as client:
        client.put("greeting", "hello world")
        value = client.get_str("greeting")
        print(value)  # "hello world"

        results = client.scan(start="a", end="z", limit=100)
        for pair in results.pairs:
            print(pair.key, pair.decode_value_str())
"""

from oxendb.client import OxenDBClient
from oxendb.batch import BatchWriter
from oxendb.models import (
    AdminStats,
    BatchOp,
    BatchResult,
    KVPair,
    QueryResult,
    ScanResult,
)
from oxendb.query_builder import QueryBuilder
from oxendb.exceptions import (
    OxenDBError,
    ConnectionError,
    AuthenticationError,
    NotFoundError,
    BadRequestError,
    ServerError,
    TimeoutError,
    QuerySyntaxError,
    BatchError,
)

__all__ = [
    # Client
    "OxenDBClient",
    "BatchWriter",
    "QueryBuilder",
    # Models
    "KVPair",
    "ScanResult",
    "BatchOp",
    "BatchResult",
    "AdminStats",
    "QueryResult",
    # Exceptions
    "OxenDBError",
    "ConnectionError",
    "AuthenticationError",
    "NotFoundError",
    "BadRequestError",
    "ServerError",
    "TimeoutError",
    "QuerySyntaxError",
    "BatchError",
]

__version__ = "0.1.0"
