//! Connection pooling for HMS clients

use super::HmsClient;
use crate::config::HmsConfig;
use crate::error::{HmsError, HmsResult};
use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use tracing::{debug, info};

/// Connection pool manager for HMS clients
pub struct HmsConnectionManager {
    config: HmsConfig,
}

impl HmsConnectionManager {
    /// Create a new connection manager
    pub fn new(config: HmsConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl bb8::ManageConnection for HmsConnectionManager {
    type Connection = HmsClient;
    type Error = HmsError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        debug!("Creating new HMS connection");
        HmsClient::new(self.config.clone()).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        debug!("Validating HMS connection");

        // TODO: Implement actual health check via Thrift call
        // For now, just check that the connection was initialized
        let _caps = conn.capabilities().await?;

        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        // TODO: Implement actual broken connection detection
        false
    }
}

/// HMS connection pool
pub struct HmsConnectionPool {
    pool: Pool<HmsConnectionManager>,
    config: HmsConfig,
}

impl HmsConnectionPool {
    /// Create a new connection pool
    pub async fn new(config: HmsConfig) -> HmsResult<Self> {
        info!(
            "Creating HMS connection pool with max_size={}",
            config.connection_pool.max_size
        );

        let manager = HmsConnectionManager::new(config.clone());

        let pool = Pool::builder()
            .max_size(config.connection_pool.max_size)
            .min_idle(config.connection_pool.min_idle)
            .connection_timeout(config.connection_pool.connection_timeout())
            .idle_timeout(config.connection_pool.idle_timeout())
            .max_lifetime(config.connection_pool.max_lifetime())
            .build(manager)
            .await
            .map_err(|e| HmsError::ConnectionPool(e.to_string()))?;

        Ok(Self { pool, config })
    }

    /// Get a connection from the pool
    pub async fn get(&self) -> HmsResult<PooledConnection<'_, HmsConnectionManager>> {
        self.pool
            .get()
            .await
            .map_err(|e| HmsError::ConnectionPool(e.to_string()))
    }

    /// Get pool state information
    pub fn state(&self) -> PoolState {
        let state = self.pool.state();
        PoolState {
            connections: state.connections,
            idle_connections: state.idle_connections,
            max_size: self.config.connection_pool.max_size,
        }
    }
}

/// Pool state information
#[derive(Debug, Clone, Copy)]
pub struct PoolState {
    /// Total number of connections in the pool
    pub connections: u32,

    /// Number of idle connections
    pub idle_connections: u32,

    /// Maximum pool size
    pub max_size: u32,
}

impl std::fmt::Display for PoolState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "connections={}/{}, idle={}",
            self.connections, self.max_size, self.idle_connections
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_connection_pool() {
        let config = HmsConfig {
            uri: "thrift://localhost:9083".to_string(),
            ..Default::default()
        };

        let pool = HmsConnectionPool::new(config).await;
        assert!(pool.is_ok());
    }

    #[tokio::test]
    async fn test_pool_state() {
        let config = HmsConfig {
            uri: "thrift://localhost:9083".to_string(),
            connection_pool: crate::config::HmsConnectionPoolConfig {
                max_size: 5,
                ..Default::default()
            },
            ..Default::default()
        };

        let pool = HmsConnectionPool::new(config).await.unwrap();
        let state = pool.state();

        assert_eq!(state.max_size, 5);
        assert!(state.connections <= state.max_size);
    }
}
