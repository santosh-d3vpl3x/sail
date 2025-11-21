//! Metadata caching layer for HMS

use crate::config::HmsCacheConfig;
use moka::future::Cache;
use sail_catalog::provider::status::{DatabaseStatus, TableStatus};
use std::sync::Arc;
use tracing::{debug, trace};

/// Metadata cache for databases and tables
pub struct MetadataCache {
    /// Cache for database metadata
    databases: Cache<String, Arc<DatabaseStatus>>,

    /// Cache for table metadata (key: "db_name.table_name")
    tables: Cache<String, Arc<TableStatus>>,

    /// Cache configuration
    config: HmsCacheConfig,
}

impl MetadataCache {
    /// Create a new metadata cache
    pub fn new(config: HmsCacheConfig) -> Self {
        let databases = if config.enabled {
            Cache::builder()
                .max_capacity(config.max_capacity)
                .time_to_live(config.ttl())
                .time_to_idle(config.tti())
                .name("hms-databases")
                .build()
        } else {
            // Disabled cache with capacity 0
            Cache::builder().max_capacity(0).build()
        };

        let tables = if config.enabled {
            Cache::builder()
                .max_capacity(config.max_capacity * 10) // More tables than databases
                .time_to_live(config.ttl())
                .time_to_idle(config.tti())
                .name("hms-tables")
                .build()
        } else {
            Cache::builder().max_capacity(0).build()
        };

        Self {
            databases,
            tables,
            config,
        }
    }

    /// Get database from cache
    pub async fn get_database(&self, name: &str) -> Option<DatabaseStatus> {
        if !self.config.enabled {
            return None;
        }

        let result = self.databases.get(name).await;

        if result.is_some() {
            trace!("Cache hit for database: {}", name);
        } else {
            trace!("Cache miss for database: {}", name);
        }

        result.map(|arc| (*arc).clone())
    }

    /// Put database in cache
    pub async fn put_database(&self, name: String, status: DatabaseStatus) {
        if self.config.enabled {
            debug!("Caching database: {}", name);
            self.databases.insert(name, Arc::new(status)).await;
        }
    }

    /// Invalidate database cache entry
    pub fn invalidate_database(&self, name: &str) {
        debug!("Invalidating database cache: {}", name);
        self.databases.invalidate(name);

        // Also invalidate all tables in this database
        let prefix = format!("{}.", name);
        self.tables
            .invalidate_entries_if(move |key, _| key.starts_with(&prefix))
            .expect("Failed to invalidate table entries");
    }

    /// Get table from cache
    pub async fn get_table(&self, db_name: &str, table_name: &str) -> Option<TableStatus> {
        if !self.config.enabled {
            return None;
        }

        let key = Self::table_key(db_name, table_name);
        let result = self.tables.get(&key).await;

        if result.is_some() {
            trace!("Cache hit for table: {}.{}", db_name, table_name);
        } else {
            trace!("Cache miss for table: {}.{}", db_name, table_name);
        }

        result.map(|arc| (*arc).clone())
    }

    /// Put table in cache
    pub async fn put_table(&self, db_name: &str, table_name: &str, status: TableStatus) {
        if self.config.enabled {
            debug!("Caching table: {}.{}", db_name, table_name);
            let key = Self::table_key(db_name, table_name);
            self.tables.insert(key, Arc::new(status)).await;
        }
    }

    /// Invalidate table cache entry
    pub fn invalidate_table(&self, db_name: &str, table_name: &str) {
        debug!("Invalidating table cache: {}.{}", db_name, table_name);
        let key = Self::table_key(db_name, table_name);
        self.tables.invalidate(&key);
    }

    /// Invalidate all tables in a database
    pub fn invalidate_database_tables(&self, db_name: &str) {
        debug!("Invalidating all tables in database: {}", db_name);
        let prefix = format!("{}.", db_name);
        self.tables
            .invalidate_entries_if(move |key, _| key.starts_with(&prefix))
            .expect("Failed to invalidate table entries");
    }

    /// Clear all cache entries
    pub async fn clear(&self) {
        debug!("Clearing all cache entries");
        self.databases.invalidate_all();
        self.tables.invalidate_all();

        // Wait for invalidation to complete
        self.databases.run_pending_tasks().await;
        self.tables.run_pending_tasks().await;
    }

    /// Get cache statistics
    pub async fn stats(&self) -> CacheStats {
        CacheStats {
            database_entries: self.databases.entry_count(),
            table_entries: self.tables.entry_count(),
            database_hits: self.databases.hit_count(),
            database_misses: self.databases.miss_count(),
            table_hits: self.tables.hit_count(),
            table_misses: self.tables.miss_count(),
        }
    }

    /// Create table cache key
    fn table_key(db_name: &str, table_name: &str) -> String {
        format!("{}.{}", db_name, table_name)
    }
}

/// Cache statistics
#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub database_entries: u64,
    pub table_entries: u64,
    pub database_hits: u64,
    pub database_misses: u64,
    pub table_hits: u64,
    pub table_misses: u64,
}

impl CacheStats {
    /// Calculate database hit rate
    pub fn database_hit_rate(&self) -> f64 {
        let total = self.database_hits + self.database_misses;
        if total == 0 {
            0.0
        } else {
            self.database_hits as f64 / total as f64
        }
    }

    /// Calculate table hit rate
    pub fn table_hit_rate(&self) -> f64 {
        let total = self.table_hits + self.table_misses;
        if total == 0 {
            0.0
        } else {
            self.table_hits as f64 / total as f64
        }
    }
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "databases: {} entries ({:.1}% hit rate), tables: {} entries ({:.1}% hit rate)",
            self.database_entries,
            self.database_hit_rate() * 100.0,
            self.table_entries,
            self.table_hit_rate() * 100.0
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sail_catalog::provider::namespace::Namespace;

    #[tokio::test]
    async fn test_cache_disabled() {
        let config = HmsCacheConfig {
            enabled: false,
            ..Default::default()
        };
        let cache = MetadataCache::new(config);

        let db = DatabaseStatus {
            catalog: "hive".to_string(),
            database: vec!["default".to_string()],
            comment: None,
            location: None,
            properties: vec![],
        };

        cache.put_database("default".to_string(), db).await;
        let result = cache.get_database("default").await;

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_cache_database() {
        let config = HmsCacheConfig::default();
        let cache = MetadataCache::new(config);

        let db = DatabaseStatus {
            catalog: "hive".to_string(),
            database: vec!["testdb".to_string()],
            comment: Some("Test database".to_string()),
            location: None,
            properties: vec![],
        };

        cache.put_database("testdb".to_string(), db.clone()).await;
        let result = cache.get_database("testdb").await;

        assert!(result.is_some());
        let cached = result.unwrap();
        assert_eq!(cached.database, vec!["testdb"]);
    }

    #[tokio::test]
    async fn test_invalidate_database() {
        let config = HmsCacheConfig::default();
        let cache = MetadataCache::new(config);

        let db = DatabaseStatus {
            catalog: "hive".to_string(),
            database: vec!["testdb".to_string()],
            comment: None,
            location: None,
            properties: vec![],
        };

        cache.put_database("testdb".to_string(), db).await;
        cache.invalidate_database("testdb");

        let result = cache.get_database("testdb").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let config = HmsCacheConfig::default();
        let cache = MetadataCache::new(config);

        // Initial stats
        let stats = cache.stats().await;
        assert_eq!(stats.database_entries, 0);

        // Add entry
        let db = DatabaseStatus {
            catalog: "hive".to_string(),
            database: vec!["testdb".to_string()],
            comment: None,
            location: None,
            properties: vec![],
        };

        cache.put_database("testdb".to_string(), db).await;

        // Check hit and miss
        let _ = cache.get_database("testdb").await; // Hit
        let _ = cache.get_database("missing").await; // Miss

        let stats = cache.stats().await;
        assert_eq!(stats.database_hits, 1);
        assert_eq!(stats.database_misses, 1);
        assert_eq!(stats.database_hit_rate(), 0.5);
    }
}
