//! Configuration for HMS catalog provider

use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Configuration for HMS catalog provider
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsConfig {
    /// Catalog name
    pub name: String,

    /// HMS Thrift URI (e.g., "thrift://localhost:9083")
    pub uri: String,

    /// Username for HMS authentication
    #[serde(default = "default_username")]
    pub username: String,

    /// Optional password for HMS authentication
    pub password: Option<String>,

    /// Connection pool configuration
    #[serde(default)]
    pub connection_pool: HmsConnectionPoolConfig,

    /// Cache configuration
    #[serde(default)]
    pub cache: HmsCacheConfig,

    /// Thrift protocol configuration
    #[serde(default)]
    pub thrift: HmsThriftConfig,
}

impl Default for HmsConfig {
    fn default() -> Self {
        Self {
            name: "hive".to_string(),
            uri: "thrift://localhost:9083".to_string(),
            username: default_username(),
            password: None,
            connection_pool: Default::default(),
            cache: Default::default(),
            thrift: Default::default(),
        }
    }
}

/// Connection pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsConnectionPoolConfig {
    /// Maximum number of connections in the pool
    #[serde(default = "default_max_size")]
    pub max_size: u32,

    /// Minimum idle connections
    #[serde(default = "default_min_idle")]
    pub min_idle: Option<u32>,

    /// Connection timeout in milliseconds
    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_ms: u64,

    /// Idle timeout in milliseconds
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_ms: u64,

    /// Max lifetime of a connection in milliseconds
    #[serde(default = "default_max_lifetime")]
    pub max_lifetime_ms: Option<u64>,
}

impl Default for HmsConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_size: default_max_size(),
            min_idle: default_min_idle(),
            connection_timeout_ms: default_connection_timeout(),
            idle_timeout_ms: default_idle_timeout(),
            max_lifetime_ms: default_max_lifetime(),
        }
    }
}

impl HmsConnectionPoolConfig {
    /// Get connection timeout as Duration
    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.connection_timeout_ms)
    }

    /// Get idle timeout as Duration
    pub fn idle_timeout(&self) -> Option<Duration> {
        Some(Duration::from_millis(self.idle_timeout_ms))
    }

    /// Get max lifetime as Duration
    pub fn max_lifetime(&self) -> Option<Duration> {
        self.max_lifetime_ms.map(Duration::from_millis)
    }
}

/// Metadata cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsCacheConfig {
    /// Enable metadata caching
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,

    /// Maximum number of entries in cache
    #[serde(default = "default_max_capacity")]
    pub max_capacity: u64,

    /// Time-to-live for cache entries in seconds
    #[serde(default = "default_ttl")]
    pub ttl_seconds: u64,

    /// Time-to-idle for cache entries in seconds
    #[serde(default = "default_tti")]
    pub tti_seconds: u64,
}

impl Default for HmsCacheConfig {
    fn default() -> Self {
        Self {
            enabled: default_cache_enabled(),
            max_capacity: default_max_capacity(),
            ttl_seconds: default_ttl(),
            tti_seconds: default_tti(),
        }
    }
}

impl HmsCacheConfig {
    /// Get TTL as Duration
    pub fn ttl(&self) -> Duration {
        Duration::from_secs(self.ttl_seconds)
    }

    /// Get TTI as Duration
    pub fn tti(&self) -> Duration {
        Duration::from_secs(self.tti_seconds)
    }
}

/// Thrift protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsThriftConfig {
    /// Thrift protocol type
    #[serde(default = "default_protocol")]
    pub protocol: ThriftProtocol,

    /// Thrift transport type
    #[serde(default = "default_transport")]
    pub transport: ThriftTransport,

    /// Request timeout in milliseconds
    #[serde(default = "default_thrift_timeout")]
    pub timeout_ms: u64,

    /// Maximum number of retries for transient failures
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Base backoff duration in milliseconds
    #[serde(default = "default_backoff_ms")]
    pub backoff_ms: u64,
}

impl Default for HmsThriftConfig {
    fn default() -> Self {
        Self {
            protocol: default_protocol(),
            transport: default_transport(),
            timeout_ms: default_thrift_timeout(),
            max_retries: default_max_retries(),
            backoff_ms: default_backoff_ms(),
        }
    }
}

impl HmsThriftConfig {
    /// Get timeout as Duration
    pub fn timeout(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Get backoff as Duration
    pub fn backoff(&self) -> Duration {
        Duration::from_millis(self.backoff_ms)
    }
}

/// Thrift protocol type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThriftProtocol {
    /// Binary protocol (default, all HMS versions)
    Binary,
    /// Compact protocol (more efficient, HMS 3.0+)
    Compact,
}

/// Thrift transport type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ThriftTransport {
    /// Buffered transport (simple, lower performance)
    Buffered,
    /// Framed transport (default, better for large messages)
    Framed,
}

// Default value functions for connection pool
fn default_max_size() -> u32 {
    10
}

fn default_min_idle() -> Option<u32> {
    Some(2)
}

fn default_connection_timeout() -> u64 {
    30_000 // 30 seconds
}

fn default_idle_timeout() -> u64 {
    600_000 // 10 minutes
}

fn default_max_lifetime() -> Option<u64> {
    Some(1_800_000) // 30 minutes
}

// Default value functions for cache
fn default_cache_enabled() -> bool {
    true
}

fn default_max_capacity() -> u64 {
    10_000
}

fn default_ttl() -> u64 {
    300 // 5 minutes
}

fn default_tti() -> u64 {
    180 // 3 minutes
}

// Default value functions for Thrift
fn default_protocol() -> ThriftProtocol {
    ThriftProtocol::Binary
}

fn default_transport() -> ThriftTransport {
    ThriftTransport::Framed
}

fn default_thrift_timeout() -> u64 {
    60_000 // 60 seconds
}

fn default_max_retries() -> u32 {
    3
}

fn default_backoff_ms() -> u64 {
    100
}

// Default username
fn default_username() -> String {
    "hive".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = HmsConfig::default();
        assert_eq!(config.name, "hive");
        assert_eq!(config.uri, "thrift://localhost:9083");
        assert_eq!(config.username, "hive");
        assert!(config.password.is_none());
    }

    #[test]
    fn test_connection_pool_config() {
        let config = HmsConnectionPoolConfig::default();
        assert_eq!(config.max_size, 10);
        assert_eq!(config.min_idle, Some(2));
        assert_eq!(config.connection_timeout(), Duration::from_secs(30));
        assert_eq!(config.idle_timeout(), Some(Duration::from_secs(600)));
    }

    #[test]
    fn test_cache_config() {
        let config = HmsCacheConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_capacity, 10_000);
        assert_eq!(config.ttl(), Duration::from_secs(300));
        assert_eq!(config.tti(), Duration::from_secs(180));
    }

    #[test]
    fn test_thrift_config() {
        let config = HmsThriftConfig::default();
        assert_eq!(config.protocol, ThriftProtocol::Binary);
        assert_eq!(config.transport, ThriftTransport::Framed);
        assert_eq!(config.timeout(), Duration::from_secs(60));
        assert_eq!(config.max_retries, 3);
    }
}
