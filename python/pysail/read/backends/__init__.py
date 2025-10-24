"""Database backend implementations for JDBC reader.

Available backends:
- ConnectorX: Rust-based, high-performance, distributed reading
- ADBC: Arrow Database Connectivity standard
- Fallback: Row-wise fallback using pyodbc (slow, for testing only)
"""

from .base import DatabaseBackend
from .connectorx import ConnectorXBackend
from .adbc import ADBCBackend
from .fallback import FallbackBackend

__all__ = [
    "DatabaseBackend",
    "ConnectorXBackend",
    "ADBCBackend",
    "FallbackBackend",
    "get_backend",
]


def get_backend(engine: str) -> DatabaseBackend:
    """
    Get backend instance by name.

    Args:
        engine: Backend name ('connectorx', 'adbc', or 'fallback')

    Returns:
        DatabaseBackend instance

    Raises:
        ValueError: If engine name is invalid
    """
    if engine == "connectorx":
        return ConnectorXBackend()
    elif engine == "adbc":
        return ADBCBackend()
    elif engine == "fallback":
        return FallbackBackend()
    else:
        raise ValueError(f"Invalid engine: {engine}. Must be one of: connectorx, adbc, fallback")
