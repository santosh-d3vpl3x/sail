//! Hive Metastore catalog provider for Sail
//!
//! This crate provides a `CatalogProvider` implementation that connects to
//! Apache Hive Metastore (HMS) via the Thrift protocol. It supports multiple
//! HMS versions (2.3.x through 4.0.x) with graceful feature degradation.
//!
//! # Features
//!
//! - **Multi-version support**: Automatically detects HMS version and capabilities
//! - **Connection pooling**: Efficient connection reuse with bb8
//! - **Metadata caching**: Configurable caching with moka for performance
//! - **ACID support**: Read and write operations on transactional tables
//! - **Async-first**: Built on Tokio for high-performance async I/O
//!
//! # Example
//!
//! ```no_run
//! use sail_catalog_hms::{HmsConfig, HmsProvider};
//! use sail_catalog::provider::CatalogProvider;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = HmsConfig {
//!         name: "hive".to_string(),
//!         uri: "thrift://localhost:9083".to_string(),
//!         username: "hive".to_string(),
//!         ..Default::default()
//!     };
//!
//!     let provider = HmsProvider::new(config).await?;
//!     let databases = provider.list_databases(None).await?;
//!     println!("Found {} databases", databases.len());
//!
//!     Ok(())
//! }
//! ```

// Re-export commonly used types
pub use config::{
    HmsAuthConfig, HmsCacheConfig, HmsConfig, HmsConnectionPoolConfig, HmsThriftConfig,
    SaslQop, ThriftProtocol, ThriftTransport,
};
pub use error::{HmsError, HmsResult};
pub use provider::HmsProvider;

// Public modules
pub mod auth;
pub mod config;
pub mod error;
pub mod provider;

// Internal modules
mod cache;
mod client;
mod types;

// ACID support (feature-gated for future extension)
#[cfg(feature = "acid")]
mod acid;
