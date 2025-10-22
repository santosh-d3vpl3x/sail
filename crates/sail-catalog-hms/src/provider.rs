//! HMS catalog provider implementation

use async_trait::async_trait;
use sail_catalog::error::CatalogResult;
use sail_catalog::provider::namespace::Namespace;
use sail_catalog::provider::status::{DatabaseStatus, TableKind, TableStatus};
use sail_catalog::provider::{
    CatalogProvider, CreateDatabaseRequest, CreateTableRequest, CreateViewRequest,
};
use std::sync::Arc;
use tracing::{debug, info};

use crate::cache::MetadataCache;
use crate::client::connection::HmsConnectionPool;
use crate::client::HmsClient;
use crate::config::HmsConfig;
use crate::error::{HmsError, HmsResult};

/// HMS catalog provider
///
/// Implements the `CatalogProvider` trait to provide access to Hive Metastore
/// metadata. Supports multiple HMS versions with automatic capability detection.
///
/// # Features
///
/// - Automatic version detection and capability management
/// - Connection pooling for efficient resource usage
/// - Metadata caching for improved performance
/// - Graceful degradation for unsupported features
///
/// # Example
///
/// ```no_run
/// use sail_catalog_hms::{HmsConfig, HmsProvider};
/// use sail_catalog::provider::CatalogProvider;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let config = HmsConfig {
///         name: "hive".to_string(),
///         uri: "thrift://localhost:9083".to_string(),
///         ..Default::default()
///     };
///
///     let provider = HmsProvider::new(config).await?;
///
///     // List all databases
///     let databases = provider.list_databases(None).await?;
///     println!("Found {} databases", databases.len());
///
///     Ok(())
/// }
/// ```
pub struct HmsProvider {
    /// Catalog name
    name: String,

    /// Connection pool for HMS clients
    pool: Arc<HmsConnectionPool>,

    /// Metadata cache
    cache: Arc<MetadataCache>,

    /// HMS configuration
    config: HmsConfig,
}

impl HmsProvider {
    /// Create a new HMS provider
    pub async fn new(config: HmsConfig) -> HmsResult<Self> {
        info!("Initializing HMS provider: {}", config.name);

        // Create connection pool
        let pool = HmsConnectionPool::new(config.clone()).await?;

        // Create metadata cache
        let cache = MetadataCache::new(config.cache.clone());

        info!("HMS provider initialized successfully: {}", config.name);

        Ok(Self {
            name: config.name.clone(),
            pool: Arc::new(pool),
            cache: Arc::new(cache),
            config,
        })
    }

    /// Get an HMS client from the pool
    async fn get_client(&self) -> HmsResult<impl std::ops::Deref<Target = HmsClient> + '_> {
        self.pool.get().await
    }

    /// Convert namespace to database name
    fn namespace_to_db_name(namespace: &Namespace) -> String {
        // HMS typically uses single-level database names
        // For multi-level namespaces, we join with underscores
        if namespace.tail.is_empty() {
            namespace.head.to_string()
        } else {
            let mut parts = vec![namespace.head.to_string()];
            parts.extend(namespace.tail.iter().map(|s| s.to_string()));
            parts.join("_")
        }
    }

    /// Create database name from Namespace
    fn db_name_to_namespace(db_name: &str) -> CatalogResult<Namespace> {
        Namespace::try_from(vec![db_name])
    }
}

#[async_trait]
impl CatalogProvider for HmsProvider {
    fn get_name(&self) -> &str {
        &self.name
    }

    async fn create_database(
        &self,
        request: CreateDatabaseRequest,
    ) -> CatalogResult<DatabaseStatus> {
        let db_name = Self::namespace_to_db_name(&request.database);
        info!("Creating database: {}", db_name);

        // TODO: Implement actual HMS create_database call via Thrift
        // For now, return error indicating not yet implemented

        Err(sail_catalog::error::CatalogError::NotSupported(
            "HMS create_database not yet fully implemented - Thrift client needed".into(),
        ))
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        if_exists: bool,
        cascade: bool,
    ) -> CatalogResult<()> {
        let db_name = Self::namespace_to_db_name(database);
        info!(
            "Dropping database: {} (if_exists={}, cascade={})",
            db_name, if_exists, cascade
        );

        // Invalidate cache
        self.cache.invalidate_database(&db_name);

        // TODO: Implement actual HMS drop_database call via Thrift

        Err(sail_catalog::error::CatalogError::NotSupported(
            "HMS drop_database not yet fully implemented - Thrift client needed".into(),
        ))
    }

    async fn get_database(&self, database: &Namespace) -> CatalogResult<DatabaseStatus> {
        let db_name = Self::namespace_to_db_name(database);
        debug!("Getting database: {}", db_name);

        // Check cache first
        if let Some(cached) = self.cache.get_database(&db_name).await {
            debug!("Cache hit for database: {}", db_name);
            return Ok(cached);
        }

        // TODO: Fetch from HMS via Thrift
        // For now, return a mock database for demonstration

        let status = DatabaseStatus {
            catalog: self.name.clone(),
            database: database.to_vec(),
            comment: Some(format!("HMS database: {}", db_name)),
            location: Some(format!("/warehouse/{}", db_name)),
            properties: vec![],
        };

        // Cache the result
        self.cache.put_database(db_name, status.clone()).await;

        Ok(status)
    }

    async fn list_databases(&self, pattern: Option<&str>) -> CatalogResult<Vec<DatabaseStatus>> {
        debug!("Listing databases (pattern: {:?})", pattern);

        // TODO: Implement actual HMS get_all_databases or get_databases(pattern) call

        // For now, return empty list with informative message
        info!("HMS list_databases not yet fully implemented - Thrift client needed");

        Ok(vec![])
    }

    async fn create_table(&self, request: CreateTableRequest) -> CatalogResult<TableStatus> {
        let db_name = Self::namespace_to_db_name(&request.database);
        info!("Creating table: {}.{}", db_name, request.name);

        // Invalidate cache
        self.cache.invalidate_table(&db_name, &request.name);

        // TODO: Implement actual HMS create_table call via Thrift

        Err(sail_catalog::error::CatalogError::NotSupported(
            "HMS create_table not yet fully implemented - Thrift client needed".into(),
        ))
    }

    async fn get_table(&self, database: &Namespace, name: &str) -> CatalogResult<TableStatus> {
        let db_name = Self::namespace_to_db_name(database);
        debug!("Getting table: {}.{}", db_name, name);

        // Check cache first
        if let Some(cached) = self.cache.get_table(&db_name, name).await {
            debug!("Cache hit for table: {}.{}", db_name, name);
            return Ok(cached);
        }

        // TODO: Fetch from HMS via Thrift
        // For now, return a mock table for demonstration

        let status = TableStatus::Table {
            catalog: self.name.clone(),
            database: database.to_vec(),
            name: name.to_string(),
            columns: vec![],
            comment: Some(format!("HMS table: {}.{}", db_name, name)),
            location: Some(format!("/warehouse/{}/{}", db_name, name)),
            format: Some("parquet".to_string()),
            partition_by: vec![],
            sort_by: vec![],
            bucket_by: None,
            options: vec![],
            properties: vec![],
        };

        // Cache the result
        self.cache.put_table(&db_name, name, status.clone()).await;

        Ok(status)
    }

    async fn list_tables(
        &self,
        database: &Namespace,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let db_name = Self::namespace_to_db_name(database);
        debug!("Listing tables in {} (pattern: {:?})", db_name, pattern);

        // TODO: Implement actual HMS get_all_tables or get_table_names_by_filter call

        info!("HMS list_tables not yet fully implemented - Thrift client needed");

        Ok(vec![])
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        name: &str,
        if_exists: bool,
    ) -> CatalogResult<()> {
        let db_name = Self::namespace_to_db_name(database);
        info!(
            "Dropping table: {}.{} (if_exists={})",
            db_name, name, if_exists
        );

        // Invalidate cache
        self.cache.invalidate_table(&db_name, name);

        // TODO: Implement actual HMS drop_table call via Thrift

        Err(sail_catalog::error::CatalogError::NotSupported(
            "HMS drop_table not yet fully implemented - Thrift client needed".into(),
        ))
    }

    // View operations - HMS treats views as special tables
    async fn create_view(&self, request: CreateViewRequest) -> CatalogResult<TableStatus> {
        let db_name = Self::namespace_to_db_name(&request.database);
        info!("Creating view: {}.{}", db_name, request.name);

        // TODO: Create view as special table type in HMS

        Err(sail_catalog::error::CatalogError::NotSupported(
            "HMS create_view not yet fully implemented - Thrift client needed".into(),
        ))
    }

    async fn get_view(&self, database: &Namespace, name: &str) -> CatalogResult<TableStatus> {
        let db_name = Self::namespace_to_db_name(database);
        debug!("Getting view: {}.{}", db_name, name);

        // TODO: Get table and verify it's a view

        Err(sail_catalog::error::CatalogError::NotSupported(
            "HMS get_view not yet fully implemented - Thrift client needed".into(),
        ))
    }

    async fn list_views(
        &self,
        database: &Namespace,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let db_name = Self::namespace_to_db_name(database);
        debug!("Listing views in {} (pattern: {:?})", db_name, pattern);

        // TODO: List tables and filter for views

        Ok(vec![])
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        name: &str,
        if_exists: bool,
    ) -> CatalogResult<()> {
        // Views are dropped like tables in HMS
        self.drop_table(database, name, if_exists).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_provider() {
        let config = HmsConfig {
            uri: "thrift://localhost:9083".to_string(),
            ..Default::default()
        };

        let provider = HmsProvider::new(config).await;
        assert!(provider.is_ok());
    }

    #[tokio::test]
    async fn test_namespace_conversion() {
        let namespace = Namespace::try_from(vec!["default"]).unwrap();
        let db_name = HmsProvider::namespace_to_db_name(&namespace);
        assert_eq!(db_name, "default");

        let namespace = Namespace::try_from(vec!["db", "schema"]).unwrap();
        let db_name = HmsProvider::namespace_to_db_name(&namespace);
        assert_eq!(db_name, "db_schema");
    }
}
