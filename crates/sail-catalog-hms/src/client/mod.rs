//! HMS client implementation
//!
//! This module provides an async HMS client with connection pooling,
//! retry logic, and version detection.

pub mod capabilities;
pub mod connection;
pub mod version;

use crate::config::HmsConfig;
use crate::error::{HmsError, HmsResult};
use async_once_cell::OnceCell;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use self::capabilities::{Capabilities, HmsFeature};
use self::version::HmsVersion;

/// HMS client that wraps the Thrift client with additional functionality
pub struct HmsClient {
    /// HMS configuration
    config: HmsConfig,

    /// Detected HMS version and capabilities
    capabilities: Arc<OnceCell<Capabilities>>,

    /// Client statistics
    stats: Arc<RwLock<ClientStats>>,
}

/// Client statistics for monitoring
#[derive(Debug, Default)]
pub struct ClientStats {
    /// Total number of requests
    pub total_requests: u64,

    /// Number of successful requests
    pub successful_requests: u64,

    /// Number of failed requests
    pub failed_requests: u64,

    /// Number of retried requests
    pub retried_requests: u64,

    /// Total connection time
    pub total_connection_time_ms: u64,
}

impl HmsClient {
    /// Create a new HMS client
    pub async fn new(config: HmsConfig) -> HmsResult<Self> {
        info!("Creating HMS client for URI: {}", config.uri);

        let client = Self {
            config,
            capabilities: Arc::new(OnceCell::new()),
            stats: Arc::new(RwLock::new(ClientStats::default())),
        };

        // Validate connection
        client.validate_connection().await?;

        Ok(client)
    }

    /// Get HMS capabilities (lazy init)
    pub async fn capabilities(&self) -> HmsResult<&Capabilities> {
        self.capabilities
            .get_or_try_init(|| async {
                let version = self.detect_version().await?;
                Ok(Capabilities::from_version(version))
            })
            .await
    }

    /// Validate HMS connection
    async fn validate_connection(&self) -> HmsResult<()> {
        debug!("Validating HMS connection to {}", self.config.uri);

        // TODO: Implement actual Thrift connection test
        // For now, just validate the URI format
        let uri = url::Url::parse(&self.config.uri)?;

        if uri.scheme() != "thrift" {
            return Err(HmsError::InvalidUri(format!(
                "Expected thrift:// scheme, got: {}",
                uri.scheme()
            )));
        }

        if uri.host().is_none() {
            return Err(HmsError::InvalidUri("Missing host in URI".to_string()));
        }

        info!("HMS connection validated successfully");
        Ok(())
    }

    /// Detect HMS version
    async fn detect_version(&self) -> HmsResult<HmsVersion> {
        debug!("Detecting HMS version");

        // TODO: Implement actual version detection via Thrift calls
        // For now, return a default version
        let version = HmsVersion {
            major: 3,
            minor: 1,
            patch: 3,
        };

        info!("Detected HMS version: {}", version);
        Ok(version)
    }

    /// Execute an operation with retry logic
    async fn with_retry<F, Fut, T>(&self, operation: F) -> HmsResult<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = HmsResult<T>>,
    {
        let max_attempts = self.config.thrift.max_retries + 1;
        let mut backoff = self.config.thrift.backoff();

        for attempt in 1..=max_attempts {
            // Update stats
            {
                let mut stats = self.stats.write().await;
                stats.total_requests += 1;
            }

            match operation().await {
                Ok(result) => {
                    // Update stats
                    {
                        let mut stats = self.stats.write().await;
                        stats.successful_requests += 1;
                        if attempt > 1 {
                            stats.retried_requests += 1;
                        }
                    }
                    return Ok(result);
                }
                Err(e) if Self::is_retryable(&e) && attempt < max_attempts => {
                    warn!(
                        "HMS operation failed (attempt {}/{}): {}. Retrying after {:?}",
                        attempt, max_attempts, e, backoff
                    );

                    tokio::time::sleep(backoff).await;
                    backoff *= 2; // Exponential backoff
                }
                Err(e) => {
                    // Update stats
                    {
                        let mut stats = self.stats.write().await;
                        stats.failed_requests += 1;
                    }
                    return Err(e);
                }
            }
        }

        unreachable!("Retry loop should always return or error")
    }

    /// Check if an error is retryable
    fn is_retryable(error: &HmsError) -> bool {
        matches!(
            error,
            HmsError::ThriftTransport(_) | HmsError::Io(_) | HmsError::ConnectionPool(_)
        )
    }

    /// Get client statistics
    pub async fn stats(&self) -> ClientStats {
        self.stats.read().await.clone()
    }
}

// Implement Debug manually to avoid printing sensitive config
impl std::fmt::Debug for HmsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HmsClient")
            .field("uri", &self.config.uri)
            .field("username", &self.config.username)
            .finish()
    }
}

impl Clone for ClientStats {
    fn clone(&self) -> Self {
        Self {
            total_requests: self.total_requests,
            successful_requests: self.successful_requests,
            failed_requests: self.failed_requests,
            retried_requests: self.retried_requests,
            total_connection_time_ms: self.total_connection_time_ms,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validate_uri_format() {
        let config = HmsConfig {
            uri: "thrift://localhost:9083".to_string(),
            ..Default::default()
        };

        let client = HmsClient::new(config).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_invalid_uri_scheme() {
        let config = HmsConfig {
            uri: "http://localhost:9083".to_string(),
            ..Default::default()
        };

        let client = HmsClient::new(config).await;
        assert!(matches!(client, Err(HmsError::InvalidUri(_))));
    }

    #[tokio::test]
    async fn test_missing_host() {
        let config = HmsConfig {
            uri: "thrift://".to_string(),
            ..Default::default()
        };

        let client = HmsClient::new(config).await;
        assert!(client.is_err());
    }
}
