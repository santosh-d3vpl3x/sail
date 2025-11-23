//! Error types for HMS catalog operations

use sail_catalog::error::CatalogError;
use thiserror::Error;

/// Result type for HMS operations
pub type HmsResult<T> = Result<T, HmsError>;

/// Errors that can occur when interacting with Hive Metastore
#[derive(Debug, Error)]
pub enum HmsError {
    /// Database not found in HMS
    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    /// Table not found in HMS
    #[error("table not found: {database}.{table}")]
    TableNotFound { database: String, table: String },

    /// Database already exists
    #[error("database already exists: {0}")]
    DatabaseAlreadyExists(String),

    /// Table already exists
    #[error("table already exists: {database}.{table}")]
    TableAlreadyExists { database: String, table: String },

    /// Invalid HMS URI format
    #[error("invalid HMS URI: {0}")]
    InvalidUri(String),

    /// Invalid configuration
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// HMS version detection failed
    #[error("HMS version detection failed: {0}")]
    VersionDetectionFailed(String),

    /// Feature not supported by this HMS version
    #[error("feature not supported by HMS version {version}: {feature}")]
    NotSupported { version: String, feature: String },

    /// Type conversion error between HMS and Sail types
    #[error("type conversion error: {0}")]
    TypeConversion(String),

    /// Connection pool error
    #[error("connection pool error: {0}")]
    ConnectionPool(String),

    /// Thrift protocol error
    #[error("Thrift protocol error: {0}")]
    ThriftProtocol(String),

    /// Thrift transport error
    #[error("Thrift transport error: {0}")]
    ThriftTransport(String),

    /// Network I/O error
    #[error("network I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// ACID transaction error
    #[cfg(feature = "acid")]
    #[error("ACID transaction error: {0}")]
    Transaction(String),

    /// Lock acquisition failed
    #[cfg(feature = "acid")]
    #[error("lock acquisition failed: {0}")]
    LockFailed(String),

    /// URL parsing error
    #[error("URL parsing error: {0}")]
    UrlParse(#[from] url::ParseError),

    /// Invalid namespace
    #[error("invalid namespace: {0}")]
    InvalidNamespace(String),

    /// Internal error (should not happen)
    #[error("internal error: {0}")]
    Internal(String),
}

/// Convert HMS errors to Sail catalog errors
impl From<HmsError> for CatalogError {
    fn from(err: HmsError) -> Self {
        match err {
            HmsError::DatabaseNotFound(name) => CatalogError::NotFound("database", name),

            HmsError::TableNotFound { database, table } => {
                CatalogError::NotFound("table", format!("{}.{}", database, table))
            }

            HmsError::DatabaseAlreadyExists(name) => {
                CatalogError::AlreadyExists("database", name)
            }

            HmsError::TableAlreadyExists { database, table } => {
                CatalogError::AlreadyExists("table", format!("{}.{}", database, table))
            }

            HmsError::NotSupported { feature, .. } => CatalogError::NotSupported(feature),

            HmsError::InvalidConfig(msg)
            | HmsError::InvalidUri(msg)
            | HmsError::InvalidNamespace(msg) => CatalogError::InvalidArgument(msg),

            HmsError::TypeConversion(msg)
            | HmsError::VersionDetectionFailed(msg)
            | HmsError::Internal(msg) => CatalogError::Internal(msg),

            HmsError::ConnectionPool(msg)
            | HmsError::ThriftProtocol(msg)
            | HmsError::ThriftTransport(msg)
            | HmsError::Io(ref io_err) => CatalogError::External(format!("{}", err)),

            #[cfg(feature = "acid")]
            HmsError::Transaction(msg) | HmsError::LockFailed(msg) => {
                CatalogError::External(msg)
            }

            HmsError::UrlParse(ref err) => {
                CatalogError::InvalidArgument(format!("Invalid URL: {}", err))
            }
        }
    }
}
