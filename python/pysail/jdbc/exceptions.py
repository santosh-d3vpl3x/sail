"""Custom exceptions for JDBC reader module."""


class JDBCReaderError(Exception):
    """Base exception for JDBC reader errors."""
    pass


class InvalidJDBCUrlError(JDBCReaderError):
    """Raised when JDBC URL format is invalid."""
    pass


class BackendNotAvailableError(JDBCReaderError):
    """Raised when requested backend is not installed."""
    pass


class UnsupportedDatabaseError(JDBCReaderError):
    """Raised when database driver is not supported by any backend."""
    pass


class DatabaseError(JDBCReaderError):
    """Raised when database operation fails."""
    pass


class SchemaInferenceError(JDBCReaderError):
    """Raised when Arrow schema cannot be inferred."""
    pass


class InvalidOptionsError(JDBCReaderError):
    """Raised when JDBC options are invalid or inconsistent."""
    pass
