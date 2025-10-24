// SPDX-License-Identifier: Apache-2.0

use datafusion_common::DataFusionError;
use std::fmt;

/// JDBC-specific errors
#[derive(Debug)]
pub enum JDBCError {
    /// Connection error
    ConnectionError(String),
    /// Query execution error
    QueryError(String),
    /// Schema inference error
    SchemaError(String),
    /// Invalid options
    InvalidOptions(String),
    /// Python bridge error
    PythonError(String),
    /// General error
    General(String),
}

impl fmt::Display for JDBCError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JDBCError::ConnectionError(msg) => write!(f, "JDBC connection error: {}", msg),
            JDBCError::QueryError(msg) => write!(f, "JDBC query error: {}", msg),
            JDBCError::SchemaError(msg) => write!(f, "JDBC schema error: {}", msg),
            JDBCError::InvalidOptions(msg) => write!(f, "Invalid JDBC options: {}", msg),
            JDBCError::PythonError(msg) => write!(f, "Python bridge error: {}", msg),
            JDBCError::General(msg) => write!(f, "JDBC error: {}", msg),
        }
    }
}

impl std::error::Error for JDBCError {}

impl From<JDBCError> for DataFusionError {
    fn from(err: JDBCError) -> Self {
        DataFusionError::External(Box::new(err))
    }
}

impl From<DataFusionError> for JDBCError {
    fn from(err: DataFusionError) -> Self {
        JDBCError::General(err.to_string())
    }
}

/// Result type for JDBC operations
pub type Result<T> = std::result::Result<T, JDBCError>;
