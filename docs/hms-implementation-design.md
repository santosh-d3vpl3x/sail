# Hive Metastore (HMS) Support for LakeSail - Implementation Design

**Version:** 1.0
**Date:** 2025-10-22
**Status:** Design Phase

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Architecture Overview](#architecture-overview)
3. [Backwards and Forward Compatibility](#backwards-and-forward-compatibility)
4. [Transactionality Support](#transactionality-support)
5. [Testability Strategy](#testability-strategy)
6. [Integration with LakeSail](#integration-with-lakesail)
7. [Technical Implementation Details](#technical-implementation-details)
8. [Known Gotchas and Mitigation](#known-gotchas-and-mitigation)
9. [Implementation Roadmap](#implementation-roadmap)
10. [Success Criteria](#success-criteria)

---

## 1. Executive Summary

This document outlines the design for adding Hive Metastore (HMS) support to LakeSail, enabling compatibility with the vast HMS ecosystem (Spark, Trino, Presto, Hive) while maintaining production-grade reliability and testability.

### Key Design Decisions

1. **Multi-version Support:** Support HMS 2.3.x through 4.0.x via capability detection
2. **ACID as Optional:** Read-only ACID support in MVP, write support in Phase 2
3. **Async-first:** Fully async Thrift client with Tokio integration
4. **Layered Testing:** Unit (mock), integration (containerized), and compatibility (multi-version) tests
5. **Graceful Degradation:** Feature detection with fallbacks for older HMS versions

---

## 2. Architecture Overview

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                    LakeSail Session Layer                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      CatalogManager                              │
│              (Delegates to CatalogProviders)                     │
└────────────────────────────┬────────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────┐
         │                   │               │
         ▼                   ▼               ▼
   ┌──────────┐      ┌──────────┐   ┌──────────────────┐
   │ Memory   │      │ Iceberg  │   │ HMS              │
   │ Catalog  │      │ Catalog  │   │ Catalog          │
   └──────────┘      └──────────┘   └────────┬─────────┘
                                              │
                         ┌────────────────────┴────────────────┐
                         │                                     │
                         ▼                                     ▼
                ┌─────────────────┐               ┌───────────────────┐
                │ HmsClient       │               │ Connection        │
                │ (Async Thrift)  │               │ Pool              │
                │                 │               │ (bb8/deadpool)    │
                │ • Version       │               └───────────────────┘
                │   Detection     │                         │
                │ • Capability    │                         ▼
                │   Registry      │               ┌───────────────────┐
                │ • Error Mapping │               │ Metadata          │
                │ • Retry Logic   │               │ Cache             │
                └────────┬────────┘               │ (moka)            │
                         │                        └───────────────────┘
                         ▼
                ┌─────────────────┐
                │ HMS Thrift      │
                │ Protocol        │
                │ (Generated)     │
                │                 │
                │ • TBinaryProto  │
                │ • TCompactProto │
                │ • TFramedTransp │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │ HMS Server      │
                │ thrift://host   │
                │ :9083           │
                └─────────────────┘
```

### 2.2 Module Structure

```
crates/
├── sail-catalog-hms/
│   ├── Cargo.toml
│   ├── src/
│   │   ├── lib.rs                  # Public API exports
│   │   ├── provider.rs             # CatalogProvider implementation
│   │   ├── client/
│   │   │   ├── mod.rs              # HmsClient abstraction
│   │   │   ├── connection.rs       # Connection pooling
│   │   │   ├── version.rs          # Version detection
│   │   │   └── capabilities.rs     # Capability registry
│   │   ├── types/
│   │   │   ├── mod.rs
│   │   │   ├── database.rs         # HMS Database <-> DatabaseStatus
│   │   │   ├── table.rs            # HMS Table <-> TableStatus
│   │   │   ├── partition.rs        # Partition conversions
│   │   │   ├── schema.rs           # FieldSchema <-> TableColumnStatus
│   │   │   └── storage.rs          # StorageDescriptor handling
│   │   ├── cache/
│   │   │   ├── mod.rs
│   │   │   ├── metadata.rs         # Metadata caching layer
│   │   │   └── strategy.rs         # Cache invalidation
│   │   ├── acid/
│   │   │   ├── mod.rs
│   │   │   ├── transaction.rs      # ACID transaction support
│   │   │   ├── lock.rs             # HMS lock service
│   │   │   └── delta.rs            # Delta file handling
│   │   ├── error.rs                # HMS-specific errors
│   │   └── config.rs               # HMS configuration
│   ├── thrift/
│   │   ├── hive_metastore.thrift   # Thrift IDL from Apache Hive
│   │   └── build.rs                # Thrift code generation
│   ├── tests/
│   │   ├── unit/                   # Unit tests with mocks
│   │   ├── integration/            # Integration tests
│   │   └── compatibility/          # Multi-version tests
│   └── docker/
│       ├── docker-compose.yml      # HMS test instances
│       └── hms-2.3/                # Different HMS versions
│           ├── hms-3.1/
│           └── hms-4.0/
```

---

## 3. Backwards and Forward Compatibility

### 3.1 Challenge Analysis

HMS has evolved significantly across versions:
- **2.x → 3.x:** Schema registry, transaction API changes, new table properties
- **3.x → 4.x:** Iceberg/Delta Lake native support, improved ACID
- **Thrift Protocol:** Generally stable but with added methods

### 3.2 Compatibility Strategy

#### 3.2.1 Version Detection

```rust
pub struct HmsVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl HmsClient {
    async fn detect_version(&self) -> HmsResult<HmsVersion> {
        // 1. Try get_metastore_db_uuid() (HMS 3.0+)
        if let Ok(uuid) = self.thrift_client.get_metastore_db_uuid().await {
            // Parse version from config
            return self.parse_version_from_config().await;
        }

        // 2. Fallback: Call get_version() or parse from exception messages
        self.legacy_version_detection().await
    }
}
```

#### 3.2.2 Capability Registry

```rust
#[derive(Debug, Clone)]
pub struct Capabilities {
    pub version: HmsVersion,
    pub features: HashSet<HmsFeature>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum HmsFeature {
    // Core features
    Databases,
    Tables,
    Partitions,

    // Advanced features
    AcidTransactions,       // HMS 3.0+
    LockService,            // HMS 2.0+
    TableStatistics,        // HMS 2.3+
    Constraints,            // HMS 3.1+
    MaterializedViews,      // HMS 3.0+
    CompactionHistory,      // HMS 3.0+
    SchemaRegistry,         // HMS 3.0+ (Confluent extension)

    // Optimizations
    BulkOperations,         // get_tables_by_name vs. loop
    PartitionBatching,      // add_partitions vs. add_partition loop
}

impl Capabilities {
    pub fn from_version(version: HmsVersion) -> Self {
        let mut features = HashSet::new();

        // Core features (all versions)
        features.insert(HmsFeature::Databases);
        features.insert(HmsFeature::Tables);
        features.insert(HmsFeature::Partitions);

        // HMS 2.0+
        if version >= HmsVersion { major: 2, minor: 0, patch: 0 } {
            features.insert(HmsFeature::LockService);
        }

        // HMS 2.3+
        if version >= HmsVersion { major: 2, minor: 3, patch: 0 } {
            features.insert(HmsFeature::TableStatistics);
            features.insert(HmsFeature::BulkOperations);
        }

        // HMS 3.0+
        if version >= HmsVersion { major: 3, minor: 0, patch: 0 } {
            features.insert(HmsFeature::AcidTransactions);
            features.insert(HmsFeature::MaterializedViews);
            features.insert(HmsFeature::CompactionHistory);
        }

        // HMS 3.1+
        if version >= HmsVersion { major: 3, minor: 1, patch: 0 } {
            features.insert(HmsFeature::Constraints);
        }

        Self { version, features }
    }

    pub fn supports(&self, feature: &HmsFeature) -> bool {
        self.features.contains(feature)
    }
}
```

#### 3.2.3 Graceful Degradation Example

```rust
impl HmsProvider {
    async fn list_tables(
        &self,
        database: &Namespace,
    ) -> CatalogResult<Vec<TableStatus>> {
        let db_name = database.to_string();

        if self.capabilities.supports(&HmsFeature::BulkOperations) {
            // HMS 2.3+: Use efficient batch API
            let table_names = self.client.get_all_tables(&db_name).await?;
            let tables = self.client.get_tables_by_name(&db_name, &table_names).await?;
            tables.into_iter().map(|t| self.convert_table(t)).collect()
        } else {
            // HMS 2.0-2.2: Fallback to individual fetches
            let table_names = self.client.get_all_tables(&db_name).await?;
            let mut tables = Vec::with_capacity(table_names.len());
            for name in table_names {
                let table = self.client.get_table(&db_name, &name).await?;
                tables.push(self.convert_table(table)?);
            }
            Ok(tables)
        }
    }
}
```

#### 3.2.4 Thrift Protocol Compatibility

```rust
pub struct ThriftConfig {
    pub protocol: ThriftProtocol,
    pub transport: ThriftTransport,
}

pub enum ThriftProtocol {
    Binary,   // Default, all versions
    Compact,  // HMS 3.0+, more efficient
}

pub enum ThriftTransport {
    Buffered,  // Simple, lower performance
    Framed,    // Default, better for large messages
}

impl HmsClient {
    pub async fn new(config: HmsConfig) -> HmsResult<Self> {
        // Auto-detect or use explicit protocol
        let protocol = config.protocol.unwrap_or(ThriftProtocol::Binary);

        // Negotiate transport based on server capabilities
        let transport = Self::negotiate_transport(&config).await?;

        let thrift_client = Self::create_thrift_client(
            &config.uri,
            protocol,
            transport,
        ).await?;

        let version = Self::detect_version(&thrift_client).await?;
        let capabilities = Capabilities::from_version(version);

        Ok(Self {
            thrift_client,
            capabilities,
            config,
        })
    }
}
```

### 3.3 Schema Evolution Handling

```rust
impl TypeConverter {
    fn convert_field_schema(
        &self,
        field: &hms::FieldSchema,
        version: &HmsVersion,
    ) -> CatalogResult<TableColumnStatus> {
        // Handle type string evolution
        let data_type = if version.major >= 3 {
            // HMS 3.0+: Proper type system with nested types
            self.parse_modern_type(&field.type_)?
        } else {
            // HMS 2.x: Limited type string parsing
            self.parse_legacy_type(&field.type_)?
        };

        Ok(TableColumnStatus {
            name: field.name.clone(),
            data_type,
            nullable: true,  // HMS doesn't track nullability explicitly
            comment: field.comment.clone(),
            default: None,   // HMS 2.x doesn't support defaults
            generated_always_as: None,
            is_partition: false,  // Set by caller
            is_bucket: false,
            is_cluster: false,
        })
    }
}
```

### 3.4 Supported Version Matrix

| HMS Version | Support Level | Features                           | Status    |
|-------------|---------------|------------------------------------|-----------|
| 2.0-2.2     | Partial       | Basic tables, partitions           | Tested    |
| 2.3.x       | Full          | + Statistics, bulk ops             | Tested    |
| 3.0.x       | Full          | + ACID, materialized views         | Tested    |
| 3.1.x       | Full          | + Constraints, schema registry     | Tested    |
| 4.0.x       | Full          | + Native Iceberg/Delta support     | In Progress |

---

## 4. Transactionality Support

### 4.1 ACID Overview

HMS ACID support (from Hive 0.14+, formalized in 3.0) provides:
- INSERT, UPDATE, DELETE operations
- Multi-version concurrency control (MVCC)
- Snapshot isolation
- Compaction and cleanup

### 4.2 ACID Metadata Structure

```rust
#[derive(Debug, Clone)]
pub struct AcidMetadata {
    pub transaction_id: i64,
    pub write_id: i64,
    pub valid_write_id_list: String,  // "1:5:4" format
    pub is_acid: bool,
    pub bucket_count: Option<i32>,
}

impl AcidMetadata {
    pub fn from_table_params(params: &HashMap<String, String>) -> Option<Self> {
        let transactional = params.get("transactional")
            .map(|v| v == "true")
            .unwrap_or(false);

        if !transactional {
            return None;
        }

        Some(Self {
            transaction_id: params.get("transactionId")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            write_id: params.get("writeId")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            valid_write_id_list: params.get("validWriteIdList")
                .cloned()
                .unwrap_or_default(),
            is_acid: true,
            bucket_count: params.get("bucketing_version")
                .and_then(|v| v.parse().ok()),
        })
    }
}
```

### 4.3 Transaction API Integration

```rust
pub struct HmsTransactionClient {
    client: Arc<HmsClient>,
}

impl HmsTransactionClient {
    pub async fn open_transaction(
        &self,
        user: &str,
        hostname: &str,
    ) -> HmsResult<i64> {
        if !self.client.capabilities.supports(&HmsFeature::AcidTransactions) {
            return Err(HmsError::NotSupported(
                "ACID transactions not supported by this HMS version".into()
            ));
        }

        self.client.thrift_client
            .open_txn(user, hostname)
            .await
            .map_err(Into::into)
    }

    pub async fn allocate_table_write_id(
        &self,
        txn_id: i64,
        db_name: &str,
        table_name: &str,
    ) -> HmsResult<i64> {
        self.client.thrift_client
            .allocate_table_write_id(txn_id, db_name, table_name)
            .await
            .map_err(Into::into)
    }

    pub async fn commit_transaction(&self, txn_id: i64) -> HmsResult<()> {
        self.client.thrift_client
            .commit_txn(txn_id)
            .await
            .map_err(Into::into)
    }

    pub async fn abort_transaction(&self, txn_id: i64) -> HmsResult<()> {
        self.client.thrift_client
            .abort_txn(txn_id)
            .await
            .map_err(Into::into)
    }
}
```

### 4.4 Lock Service Integration

```rust
pub struct HmsLockClient {
    client: Arc<HmsClient>,
}

#[derive(Debug)]
pub struct LockRequest {
    pub db_name: String,
    pub table_name: String,
    pub lock_type: LockType,
    pub user: String,
    pub hostname: String,
}

#[derive(Debug, Clone, Copy)]
pub enum LockType {
    Shared,
    Exclusive,
    SemiShared,  // For ACID updates
}

impl HmsLockClient {
    pub async fn acquire_lock(
        &self,
        request: LockRequest,
    ) -> HmsResult<i64> {
        if !self.client.capabilities.supports(&HmsFeature::LockService) {
            return Err(HmsError::NotSupported(
                "Lock service not supported by this HMS version".into()
            ));
        }

        let lock_req = self.build_lock_request(request);
        let response = self.client.thrift_client
            .lock(lock_req)
            .await?;

        Ok(response.lockid)
    }

    pub async fn release_lock(&self, lock_id: i64) -> HmsResult<()> {
        self.client.thrift_client
            .unlock(lock_id)
            .await
            .map_err(Into::into)
    }

    pub async fn check_lock(&self, lock_id: i64) -> HmsResult<LockState> {
        let response = self.client.thrift_client
            .check_lock(lock_id)
            .await?;

        Ok(match response.state {
            hms::LockState::Acquired => LockState::Acquired,
            hms::LockState::Waiting => LockState::Waiting,
            hms::LockState::NotAcquired => LockState::NotAcquired,
            hms::LockState::Aborted => LockState::Aborted,
        })
    }
}
```

### 4.5 Read ACID Tables (MVP)

```rust
impl HmsProvider {
    async fn get_table_with_acid(
        &self,
        database: &Namespace,
        name: &str,
    ) -> CatalogResult<TableStatus> {
        let db_name = database.to_string();
        let hms_table = self.client.get_table(&db_name, name).await?;

        // Extract ACID metadata
        let acid_metadata = AcidMetadata::from_table_params(&hms_table.parameters);

        let mut table_status = self.convert_table(hms_table)?;

        // Add ACID properties to table options
        if let Some(acid) = acid_metadata {
            if let TableKind::Table { ref mut options, .. } = table_status {
                options.insert("transactional".to_string(), "true".to_string());
                options.insert("write_id".to_string(), acid.write_id.to_string());

                // For read operations, we need to know which files are valid
                // This is handled by the table format implementation (Iceberg/Delta)
            }
        }

        Ok(table_status)
    }
}
```

### 4.6 Write ACID Tables (Phase 2)

```rust
impl HmsProvider {
    async fn write_to_acid_table(
        &self,
        database: &Namespace,
        table_name: &str,
        operation: WriteOperation,
    ) -> CatalogResult<()> {
        // 1. Open transaction
        let txn_id = self.transaction_client
            .open_transaction(&self.config.username, &self.config.hostname)
            .await?;

        // 2. Acquire lock
        let lock_id = self.lock_client
            .acquire_lock(LockRequest {
                db_name: database.to_string(),
                table_name: table_name.to_string(),
                lock_type: match operation {
                    WriteOperation::Insert => LockType::SemiShared,
                    WriteOperation::Update | WriteOperation::Delete => LockType::Exclusive,
                },
                user: self.config.username.clone(),
                hostname: self.config.hostname.clone(),
            })
            .await?;

        // 3. Allocate write ID
        let write_id = self.transaction_client
            .allocate_table_write_id(txn_id, &database.to_string(), table_name)
            .await?;

        // 4. Perform write (delegate to format implementation)
        let result = self.execute_write(database, table_name, write_id, operation).await;

        // 5. Commit or abort
        match result {
            Ok(()) => {
                self.transaction_client.commit_transaction(txn_id).await?;
                self.lock_client.release_lock(lock_id).await?;
                Ok(())
            }
            Err(e) => {
                let _ = self.transaction_client.abort_transaction(txn_id).await;
                let _ = self.lock_client.release_lock(lock_id).await;
                Err(e)
            }
        }
    }
}
```

### 4.7 ACID Support Matrix

| Feature                  | MVP (Read-only) | Phase 2 (Write) | Status    |
|--------------------------|-----------------|-----------------|-----------|
| Read ACID tables         | Yes             | Yes             | Planned   |
| Detect transactional props | Yes           | Yes             | Planned   |
| Transaction lifecycle    | No              | Yes             | Future    |
| Lock acquisition         | No              | Yes             | Future    |
| Write ID allocation      | No              | Yes             | Future    |
| Delta file handling      | No              | Yes             | Future    |
| Compaction support       | No              | No              | Future    |

---

## 5. Testability Strategy

### 5.1 Testing Pyramid

```
           ┌─────────────────┐
           │  Compatibility  │  <-- 3 HMS versions × key scenarios
           │     Tests       │
           └─────────────────┘
          /                   \
         /                     \
    ┌────────────────────────────┐
    │   Integration Tests        │  <-- Docker Compose HMS instances
    │  (Real HMS instances)      │
    └────────────────────────────┘
   /                              \
  /                                \
┌──────────────────────────────────┐
│        Unit Tests                │  <-- Mock Thrift clients
│    (Mock HMS responses)          │
└──────────────────────────────────┘
```

### 5.2 Unit Testing with Mocks

```rust
// tests/unit/mock_client.rs

use async_trait::async_trait;
use mockall::mock;

mock! {
    pub HmsThriftClient {
        async fn get_database(&self, name: &str) -> Result<hms::Database, hms::ThriftError>;
        async fn get_all_databases(&self) -> Result<Vec<String>, hms::ThriftError>;
        async fn get_table(&self, db_name: &str, table_name: &str)
            -> Result<hms::Table, hms::ThriftError>;
        async fn get_all_tables(&self, db_name: &str) -> Result<Vec<String>, hms::ThriftError>;
        // ... other methods
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_get_database_success() {
        let mut mock_client = MockHmsThriftClient::new();

        mock_client
            .expect_get_database()
            .with(eq("default"))
            .times(1)
            .returning(|_| {
                Ok(hms::Database {
                    name: "default".to_string(),
                    description: Some("Default database".to_string()),
                    location_uri: Some("hdfs://namenode/warehouse/default".to_string()),
                    parameters: HashMap::new(),
                    ..Default::default()
                })
            });

        let provider = HmsProvider::with_client(Arc::new(mock_client));

        let result = provider.get_database(&Namespace::try_from(vec!["default"]).unwrap()).await;

        assert!(result.is_ok());
        let db = result.unwrap();
        assert_eq!(db.database, vec!["default"]);
    }

    #[tokio::test]
    async fn test_get_database_not_found() {
        let mut mock_client = MockHmsThriftClient::new();

        mock_client
            .expect_get_database()
            .with(eq("nonexistent"))
            .times(1)
            .returning(|_| Err(hms::ThriftError::NoSuchObjectException));

        let provider = HmsProvider::with_client(Arc::new(mock_client));

        let result = provider.get_database(&Namespace::try_from(vec!["nonexistent"]).unwrap()).await;

        assert!(matches!(result, Err(CatalogError::NotFound(_, _))));
    }
}
```

### 5.3 Integration Testing Infrastructure

#### 5.3.1 Docker Compose Setup

```yaml
# docker/docker-compose.yml

version: '3.8'

services:
  # PostgreSQL backend for HMS
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: metastore
      POSTGRES_USER: hive
      POSTGRES_PASSWORD: hive
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "hive"]
      interval: 5s
      timeout: 3s
      retries: 5

  # HMS 2.3.x (Legacy support)
  hms-2.3:
    image: apache/hive:2.3.9
    environment:
      SERVICE_NAME: metastore
      DB_DRIVER: postgres
      SERVICE_OPTS: "-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
                     -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore
                     -Djavax.jdo.option.ConnectionUserName=hive
                     -Djavax.jdo.option.ConnectionPassword=hive"
    ports:
      - "9083:9083"
    depends_on:
      postgres:
        condition: service_healthy

  # HMS 3.1.x (Current stable)
  hms-3.1:
    image: apache/hive:3.1.3
    environment:
      SERVICE_NAME: metastore
      DB_DRIVER: postgres
      SERVICE_OPTS: "-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
                     -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore31
                     -Djavax.jdo.option.ConnectionUserName=hive
                     -Djavax.jdo.option.ConnectionPassword=hive"
    ports:
      - "9084:9083"
    depends_on:
      postgres:
        condition: service_healthy

  # HMS 4.0.x (Latest)
  hms-4.0:
    image: apache/hive:4.0.0
    environment:
      SERVICE_NAME: metastore
      DB_DRIVER: postgres
      SERVICE_OPTS: "-Djavax.jdo.option.ConnectionDriverName=org.postgresql.Driver
                     -Djavax.jdo.option.ConnectionURL=jdbc:postgresql://postgres:5432/metastore40
                     -Djavax.jdo.option.ConnectionUserName=hive
                     -Djavax.jdo.option.ConnectionPassword=hive"
    ports:
      - "9085:9083"
    depends_on:
      postgres:
        condition: service_healthy
```

#### 5.3.2 Integration Test Framework

```rust
// tests/integration/mod.rs

use testcontainers::{clients, images::generic::GenericImage, Container};

pub struct HmsTestContext {
    pub hms_uri: String,
    pub hms_version: String,
    _container: Container<'static, GenericImage>,
}

impl HmsTestContext {
    pub async fn new(version: &str) -> Self {
        let docker = clients::Cli::default();

        let image = GenericImage::new("apache/hive", version)
            .with_env_var("SERVICE_NAME", "metastore")
            .with_wait_for(testcontainers::core::WaitFor::message_on_stdout(
                "Starting Hive Metastore Server"
            ));

        let container = docker.run(image);
        let port = container.get_host_port_ipv4(9083);

        let hms_uri = format!("thrift://localhost:{}", port);

        // Wait for HMS to be ready
        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

        Self {
            hms_uri,
            hms_version: version.to_string(),
            _container: container,
        }
    }

    pub async fn create_provider(&self) -> HmsProvider {
        let config = HmsConfig {
            name: "test".to_string(),
            uri: self.hms_uri.clone(),
            username: "hive".to_string(),
            ..Default::default()
        };

        HmsProvider::new(config).await.unwrap()
    }
}

#[tokio::test]
async fn test_create_and_get_database() {
    let ctx = HmsTestContext::new("3.1.3").await;
    let provider = ctx.create_provider().await;

    // Create database
    let create_req = CreateDatabaseRequest {
        database: Namespace::try_from(vec!["test_db"]).unwrap(),
        comment: Some("Test database".to_string()),
        location: None,
        properties: vec![],
        if_not_exists: false,
    };

    provider.create_database(create_req).await.unwrap();

    // Get database
    let db = provider.get_database(&Namespace::try_from(vec!["test_db"]).unwrap()).await.unwrap();

    assert_eq!(db.database, vec!["test_db"]);
    assert_eq!(db.comment, Some("Test database".to_string()));
}
```

### 5.4 Compatibility Testing

```rust
// tests/compatibility/mod.rs

#[rstest]
#[case::hms_2_3("2.3.9")]
#[case::hms_3_1("3.1.3")]
#[case::hms_4_0("4.0.0")]
#[tokio::test]
async fn test_basic_operations_across_versions(#[case] version: &str) {
    let ctx = HmsTestContext::new(version).await;
    let provider = ctx.create_provider().await;

    // Test database operations
    test_database_lifecycle(&provider).await;

    // Test table operations
    test_table_lifecycle(&provider).await;

    // Test partition operations
    test_partition_operations(&provider).await;
}

async fn test_database_lifecycle(provider: &HmsProvider) {
    // Create, list, get, drop
    // ...
}

async fn test_table_lifecycle(provider: &HmsProvider) {
    // Create various table types:
    // - Simple table
    // - Partitioned table
    // - Bucketed table
    // - External table
    // - Transactional table (if supported)
    // ...
}
```

### 5.5 Test Fixtures

```rust
// tests/fixtures/tables.rs

pub fn simple_table_fixture() -> CreateTableRequest {
    CreateTableRequest {
        database: Namespace::try_from(vec!["default"]).unwrap(),
        name: "simple_table".to_string(),
        columns: vec![
            TableColumnStatus {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                comment: None,
                ..Default::default()
            },
            TableColumnStatus {
                name: "name".to_string(),
                data_type: DataType::Utf8,
                nullable: true,
                comment: Some("User name".to_string()),
                ..Default::default()
            },
        ],
        comment: Some("Simple test table".to_string()),
        location: Some("hdfs://namenode/warehouse/simple_table".to_string()),
        format: Some("parquet".to_string()),
        ..Default::default()
    }
}

pub fn partitioned_table_fixture() -> CreateTableRequest {
    let mut req = simple_table_fixture();
    req.name = "partitioned_table".to_string();
    req.partition_by = vec!["year".to_string(), "month".to_string()];
    req.columns.push(TableColumnStatus {
        name: "year".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_partition: true,
        ..Default::default()
    });
    req.columns.push(TableColumnStatus {
        name: "month".to_string(),
        data_type: DataType::Int32,
        nullable: false,
        is_partition: true,
        ..Default::default()
    });
    req
}

pub fn acid_table_fixture() -> CreateTableRequest {
    let mut req = simple_table_fixture();
    req.name = "acid_table".to_string();
    req.options.insert("transactional".to_string(), "true".to_string());
    req.options.insert("transactional_properties".to_string(), "insert_only".to_string());
    req
}
```

### 5.6 Test Coverage Goals

| Component                | Unit Coverage | Integration Coverage | Notes                    |
|--------------------------|---------------|----------------------|--------------------------|
| HmsClient                | 90%           | 100%                 | Core client logic        |
| Type conversions         | 95%           | 80%                  | Edge cases in unit tests |
| Version detection        | 85%           | 100%                 | Multi-version scenarios  |
| Error handling           | 90%           | 70%                  | Network failures, etc.   |
| ACID operations          | 80%           | 90%                  | Transaction lifecycle    |
| Cache layer              | 85%           | 60%                  | Invalidation logic       |
| **Overall Target**       | **85%**       | **80%**              |                          |

---

## 6. Integration with LakeSail

### 6.1 CatalogProvider Implementation

```rust
// crates/sail-catalog-hms/src/provider.rs

use async_trait::async_trait;
use sail_catalog::provider::{CatalogProvider, CatalogResult};
use sail_catalog::provider::status::{DatabaseStatus, TableStatus};
use sail_catalog::provider::namespace::Namespace;

pub struct HmsProvider {
    name: String,
    client: Arc<HmsClient>,
    cache: Arc<MetadataCache>,
    config: HmsConfig,
}

impl HmsProvider {
    pub async fn new(config: HmsConfig) -> HmsResult<Self> {
        let client = HmsClient::new(&config).await?;
        let cache = MetadataCache::new(config.cache_config.clone());

        Ok(Self {
            name: config.name.clone(),
            client: Arc::new(client),
            cache: Arc::new(cache),
            config,
        })
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
        let hms_database = hms::Database {
            name: request.database.to_string(),
            description: request.comment,
            location_uri: request.location,
            parameters: request.properties.into_iter().collect(),
            ..Default::default()
        };

        self.client.create_database(hms_database, request.if_not_exists)
            .await
            .map_err(|e| e.into())?;

        // Invalidate cache
        self.cache.invalidate_database(&request.database);

        // Return created database
        self.get_database(&request.database).await
    }

    async fn drop_database(
        &self,
        database: &Namespace,
        if_exists: bool,
        cascade: bool,
    ) -> CatalogResult<()> {
        let db_name = database.to_string();

        self.client.drop_database(&db_name, if_exists, cascade)
            .await
            .map_err(|e| e.into())?;

        // Invalidate cache
        self.cache.invalidate_database(database);

        Ok(())
    }

    async fn get_database(
        &self,
        database: &Namespace,
    ) -> CatalogResult<DatabaseStatus> {
        let db_name = database.to_string();

        // Check cache first
        if let Some(cached) = self.cache.get_database(&db_name).await {
            return Ok(cached);
        }

        // Fetch from HMS
        let hms_database = self.client.get_database(&db_name)
            .await
            .map_err(|e| e.into())?;

        let status = DatabaseStatus {
            catalog: self.name.clone(),
            database: vec![hms_database.name.clone()],
            comment: hms_database.description,
            location: hms_database.location_uri,
            properties: hms_database.parameters.into_iter().collect(),
        };

        // Update cache
        self.cache.put_database(db_name, status.clone()).await;

        Ok(status)
    }

    async fn list_databases(
        &self,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<DatabaseStatus>> {
        let db_names = if let Some(pattern) = pattern {
            self.client.get_databases(pattern).await?
        } else {
            self.client.get_all_databases().await?
        };

        let mut databases = Vec::with_capacity(db_names.len());
        for name in db_names {
            let db = self.get_database(&Namespace::try_from(vec![name])?)
                .await?;
            databases.push(db);
        }

        Ok(databases)
    }

    async fn create_table(
        &self,
        request: CreateTableRequest,
    ) -> CatalogResult<TableStatus> {
        let hms_table = self.convert_to_hms_table(&request)?;

        self.client.create_table(hms_table, request.if_not_exists)
            .await
            .map_err(|e| e.into())?;

        // Invalidate cache
        self.cache.invalidate_table(&request.database, &request.name);

        // Return created table
        self.get_table(&request.database, &request.name).await
    }

    async fn get_table(
        &self,
        database: &Namespace,
        name: &str,
    ) -> CatalogResult<TableStatus> {
        let db_name = database.to_string();

        // Check cache first
        if let Some(cached) = self.cache.get_table(&db_name, name).await {
            return Ok(cached);
        }

        // Fetch from HMS
        let hms_table = self.client.get_table(&db_name, name)
            .await
            .map_err(|e| e.into())?;

        let status = self.convert_from_hms_table(hms_table)?;

        // Update cache
        self.cache.put_table(&db_name, name, status.clone()).await;

        Ok(status)
    }

    async fn list_tables(
        &self,
        database: &Namespace,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let db_name = database.to_string();

        let table_names = if let Some(pattern) = pattern {
            self.client.get_table_names_by_filter(&db_name, pattern, -1).await?
        } else {
            self.client.get_all_tables(&db_name).await?
        };

        // Use bulk API if available
        if self.client.capabilities.supports(&HmsFeature::BulkOperations) {
            let hms_tables = self.client.get_tables_by_name(&db_name, &table_names).await?;
            hms_tables.into_iter()
                .map(|t| self.convert_from_hms_table(t))
                .collect()
        } else {
            let mut tables = Vec::with_capacity(table_names.len());
            for name in table_names {
                let table = self.get_table(database, &name).await?;
                tables.push(table);
            }
            Ok(tables)
        }
    }

    async fn drop_table(
        &self,
        database: &Namespace,
        name: &str,
        if_exists: bool,
    ) -> CatalogResult<()> {
        let db_name = database.to_string();

        self.client.drop_table(&db_name, name, if_exists)
            .await
            .map_err(|e| e.into())?;

        // Invalidate cache
        self.cache.invalidate_table(database, name);

        Ok(())
    }

    // View operations delegate to table operations in HMS
    async fn create_view(
        &self,
        request: CreateViewRequest,
    ) -> CatalogResult<TableStatus> {
        let mut table_request = self.convert_view_to_table(request)?;
        self.create_table(table_request).await
    }

    async fn get_view(
        &self,
        database: &Namespace,
        name: &str,
    ) -> CatalogResult<TableStatus> {
        let table = self.get_table(database, name).await?;

        // Verify it's actually a view
        if !self.is_view(&table) {
            return Err(CatalogError::NotFound("view", name.to_string()));
        }

        Ok(table)
    }

    async fn list_views(
        &self,
        database: &Namespace,
        pattern: Option<&str>,
    ) -> CatalogResult<Vec<TableStatus>> {
        let all_tables = self.list_tables(database, pattern).await?;
        Ok(all_tables.into_iter().filter(|t| self.is_view(t)).collect())
    }

    async fn drop_view(
        &self,
        database: &Namespace,
        name: &str,
        if_exists: bool,
    ) -> CatalogResult<()> {
        self.drop_table(database, name, if_exists).await
    }
}
```

### 6.2 Configuration Integration

```rust
// Update: crates/sail-common/src/config/application.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CatalogType {
    Memory {
        name: String,
        initial_database: Vec<String>,
        initial_database_comment: Option<String>,
    },

    #[cfg(feature = "hms")]
    HiveMetastore {
        name: String,
        uri: String,
        username: Option<String>,
        password: Option<String>,

        #[serde(default)]
        connection_pool: HmsConnectionPoolConfig,

        #[serde(default)]
        cache: HmsCacheConfig,

        #[serde(default)]
        thrift: HmsThriftConfig,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsConnectionPoolConfig {
    #[serde(default = "default_max_size")]
    pub max_size: u32,

    #[serde(default = "default_min_idle")]
    pub min_idle: Option<u32>,

    #[serde(default = "default_connection_timeout")]
    pub connection_timeout_ms: u64,

    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsCacheConfig {
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,

    #[serde(default = "default_max_capacity")]
    pub max_capacity: u64,

    #[serde(default = "default_ttl")]
    pub ttl_seconds: u64,

    #[serde(default = "default_tti")]
    pub tti_seconds: u64,  // Time to idle
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HmsThriftConfig {
    #[serde(default = "default_protocol")]
    pub protocol: String,  // "binary" or "compact"

    #[serde(default = "default_transport")]
    pub transport: String,  // "buffered" or "framed"

    #[serde(default = "default_thrift_timeout")]
    pub timeout_ms: u64,
}

// Default value functions
fn default_max_size() -> u32 { 10 }
fn default_min_idle() -> Option<u32> { Some(2) }
fn default_connection_timeout() -> u64 { 30_000 }
fn default_idle_timeout() -> u64 { 600_000 }
fn default_cache_enabled() -> bool { true }
fn default_max_capacity() -> u64 { 10_000 }
fn default_ttl() -> u64 { 300 }  // 5 minutes
fn default_tti() -> u64 { 180 }  // 3 minutes
fn default_protocol() -> String { "binary".to_string() }
fn default_transport() -> String { "framed".to_string() }
fn default_thrift_timeout() -> u64 { 60_000 }
```

### 6.3 Session Initialization Update

```rust
// Update: crates/sail-session/src/catalog.rs

pub fn create_catalog_manager(config: &AppConfig) -> Result<CatalogManager> {
    let catalogs = config.catalog.list
        .iter()
        .map(|x| -> CatalogResult<(String, Arc<dyn CatalogProvider>)> {
            match x {
                CatalogType::Memory { name, initial_database, initial_database_comment } => {
                    let provider = MemoryCatalogProvider::new(
                        name.clone(),
                        initial_database.clone().try_into()?,
                        initial_database_comment.clone(),
                    );
                    Ok((name.clone(), Arc::new(provider)))
                }

                #[cfg(feature = "hms")]
                CatalogType::HiveMetastore { name, uri, username, password, connection_pool, cache, thrift } => {
                    let hms_config = sail_catalog_hms::HmsConfig {
                        name: name.clone(),
                        uri: uri.clone(),
                        username: username.clone().unwrap_or_else(|| "hive".to_string()),
                        password: password.clone(),
                        connection_pool_config: connection_pool.clone(),
                        cache_config: cache.clone(),
                        thrift_config: thrift.clone(),
                    };

                    let provider = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(
                            sail_catalog_hms::HmsProvider::new(hms_config)
                        )
                    })?;

                    Ok((name.clone(), Arc::new(provider)))
                }
            }
        })
        .collect::<CatalogResult<HashMap<_, _>>>()
        .map_err(|e| plan_datafusion_err!("failed to create catalog: {e}"))?;

    let options = CatalogManagerOptions {
        catalogs,
        default_catalog: config.catalog.default_catalog.clone(),
        default_database: config.catalog.default_database.clone().try_into()?,
        global_temporary_database: config.catalog.global_temporary_database.clone().try_into()?,
    };

    CatalogManager::new(options)
}
```

### 6.4 Example Configuration

```yaml
# application.yaml

catalog:
  default_catalog: "hive"
  default_database: ["default"]
  global_temporary_database: ["global_temp"]

  list:
    # In-memory catalog for temporary tables
    - name: "sail"
      type: "memory"
      initial_database: ["default"]
      initial_database_comment: "Default in-memory database"

    # Hive Metastore catalog
    - name: "hive"
      type: "hive_metastore"
      uri: "thrift://hms-server:9083"
      username: "hadoop"
      password: null  # Optional

      connection_pool:
        max_size: 20
        min_idle: 5
        connection_timeout_ms: 30000
        idle_timeout_ms: 600000

      cache:
        enabled: true
        max_capacity: 10000
        ttl_seconds: 300
        tti_seconds: 180

      thrift:
        protocol: "binary"
        transport: "framed"
        timeout_ms: 60000
```

---

## 7. Technical Implementation Details

### 7.1 Thrift Code Generation

#### 7.1.1 Thrift Definition Source

```bash
# crates/sail-catalog-hms/thrift/download-thrift.sh

#!/bin/bash
set -e

HIVE_VERSION="4.0.0"
THRIFT_URL="https://raw.githubusercontent.com/apache/hive/rel/release-${HIVE_VERSION}/standalone-metastore/metastore-common/src/main/thrift/hive_metastore.thrift"

curl -o hive_metastore.thrift "$THRIFT_URL"

echo "Downloaded Hive Metastore Thrift IDL for version $HIVE_VERSION"
```

#### 7.1.2 Build Script

```rust
// crates/sail-catalog-hms/build.rs

use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=thrift/hive_metastore.thrift");

    let thrift_file = PathBuf::from("thrift/hive_metastore.thrift");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").unwrap());

    // Generate Rust code from Thrift IDL
    thrift_compiler::Compiler::new()
        .input(&thrift_file)
        .output_dir(&out_dir)
        .compile()
        .expect("Failed to compile Thrift IDL");
}
```

### 7.2 Async Thrift Client Wrapper

```rust
// crates/sail-catalog-hms/src/client/mod.rs

use tokio::net::TcpStream;
use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol};
use thrift::transport::{TFramedReadTransport, TFramedWriteTransport, TIoChannel};

pub struct HmsClient {
    thrift_client: Arc<ThriftHiveMetastoreClient>,
    capabilities: Capabilities,
    config: HmsConfig,
}

impl HmsClient {
    pub async fn new(config: &HmsConfig) -> HmsResult<Self> {
        let uri = url::Url::parse(&config.uri)?;
        let host = uri.host_str().ok_or(HmsError::InvalidUri)?;
        let port = uri.port().unwrap_or(9083);

        let stream = TcpStream::connect((host, port)).await?;
        let channel = TIoChannel::new(stream);

        let (read_transport, write_transport) = match config.thrift_config.transport.as_str() {
            "framed" => {
                let read = TFramedReadTransport::new(channel.clone());
                let write = TFramedWriteTransport::new(channel);
                (read, write)
            }
            "buffered" => {
                // Use buffered transport
                unimplemented!("Buffered transport not yet implemented")
            }
            _ => return Err(HmsError::InvalidConfig("Unknown transport type".into())),
        };

        let (input_protocol, output_protocol) = match config.thrift_config.protocol.as_str() {
            "binary" => {
                let input = TBinaryInputProtocol::new(read_transport, true);
                let output = TBinaryOutputProtocol::new(write_transport, true);
                (input, output)
            }
            "compact" => {
                // Use compact protocol
                unimplemented!("Compact protocol not yet implemented")
            }
            _ => return Err(HmsError::InvalidConfig("Unknown protocol type".into())),
        };

        let thrift_client = ThriftHiveMetastoreClient::new(input_protocol, output_protocol);
        let thrift_client = Arc::new(thrift_client);

        // Detect version and capabilities
        let version = Self::detect_version_internal(&thrift_client).await?;
        let capabilities = Capabilities::from_version(version);

        Ok(Self {
            thrift_client,
            capabilities,
            config: config.clone(),
        })
    }

    async fn detect_version_internal(
        client: &ThriftHiveMetastoreClient,
    ) -> HmsResult<HmsVersion> {
        // Try modern method (HMS 3.0+)
        if let Ok(version_str) = client.get_version().await {
            return HmsVersion::parse(&version_str);
        }

        // Fallback: Infer from available methods
        // Try calling a 3.0+ method
        if client.get_metastore_db_uuid().await.is_ok() {
            return Ok(HmsVersion { major: 3, minor: 0, patch: 0 });
        }

        // Assume 2.3 as minimum supported
        Ok(HmsVersion { major: 2, minor: 3, patch: 0 })
    }

    pub async fn get_database(&self, name: &str) -> HmsResult<hms::Database> {
        self.with_retry(|| self.thrift_client.get_database(name)).await
    }

    pub async fn get_all_databases(&self) -> HmsResult<Vec<String>> {
        self.with_retry(|| self.thrift_client.get_all_databases()).await
    }

    pub async fn create_database(
        &self,
        database: hms::Database,
        if_not_exists: bool,
    ) -> HmsResult<()> {
        self.with_retry(|| async {
            match self.thrift_client.create_database(&database).await {
                Ok(()) => Ok(()),
                Err(e) if if_not_exists && Self::is_already_exists(&e) => Ok(()),
                Err(e) => Err(e),
            }
        }).await
    }

    // Retry logic with exponential backoff
    async fn with_retry<F, Fut, T>(&self, mut f: F) -> HmsResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, thrift::Error>>,
    {
        let mut attempts = 0;
        let max_attempts = 3;
        let mut backoff = Duration::from_millis(100);

        loop {
            attempts += 1;

            match f().await {
                Ok(result) => return Ok(result),
                Err(e) if Self::is_retryable(&e) && attempts < max_attempts => {
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn is_retryable(error: &thrift::Error) -> bool {
        matches!(
            error,
            thrift::Error::Transport(_) | thrift::Error::Protocol(_)
        )
    }

    fn is_already_exists(error: &thrift::Error) -> bool {
        // Check for AlreadyExistsException
        // Implementation depends on generated Thrift code
        false
    }
}
```

### 7.3 Connection Pooling

```rust
// crates/sail-catalog-hms/src/client/connection.rs

use bb8::{Pool, PooledConnection};

pub struct HmsConnectionManager {
    config: HmsConfig,
}

#[async_trait]
impl bb8::ManageConnection for HmsConnectionManager {
    type Connection = HmsClient;
    type Error = HmsError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        HmsClient::new(&self.config).await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        // Health check: try to get databases
        conn.get_all_databases().await?;
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

pub struct HmsConnectionPool {
    pool: Pool<HmsConnectionManager>,
}

impl HmsConnectionPool {
    pub async fn new(config: HmsConfig) -> HmsResult<Self> {
        let manager = HmsConnectionManager { config: config.clone() };

        let pool = Pool::builder()
            .max_size(config.connection_pool_config.max_size)
            .min_idle(config.connection_pool_config.min_idle)
            .connection_timeout(Duration::from_millis(
                config.connection_pool_config.connection_timeout_ms
            ))
            .idle_timeout(Some(Duration::from_millis(
                config.connection_pool_config.idle_timeout_ms
            )))
            .build(manager)
            .await?;

        Ok(Self { pool })
    }

    pub async fn get(&self) -> HmsResult<PooledConnection<'_, HmsConnectionManager>> {
        self.pool.get().await.map_err(Into::into)
    }
}
```

### 7.4 Metadata Caching

```rust
// crates/sail-catalog-hms/src/cache/metadata.rs

use moka::future::Cache;

pub struct MetadataCache {
    databases: Cache<String, DatabaseStatus>,
    tables: Cache<(String, String), TableStatus>,
    config: HmsCacheConfig,
}

impl MetadataCache {
    pub fn new(config: HmsCacheConfig) -> Self {
        let databases = Cache::builder()
            .max_capacity(config.max_capacity)
            .time_to_live(Duration::from_secs(config.ttl_seconds))
            .time_to_idle(Duration::from_secs(config.tti_seconds))
            .build();

        let tables = Cache::builder()
            .max_capacity(config.max_capacity * 10)  // More tables than databases
            .time_to_live(Duration::from_secs(config.ttl_seconds))
            .time_to_idle(Duration::from_secs(config.tti_seconds))
            .build();

        Self {
            databases,
            tables,
            config,
        }
    }

    pub async fn get_database(&self, name: &str) -> Option<DatabaseStatus> {
        if !self.config.enabled {
            return None;
        }
        self.databases.get(name).await
    }

    pub async fn put_database(&self, name: String, status: DatabaseStatus) {
        if self.config.enabled {
            self.databases.insert(name, status).await;
        }
    }

    pub fn invalidate_database(&self, namespace: &Namespace) {
        let name = namespace.to_string();
        self.databases.invalidate(&name);

        // Also invalidate all tables in this database
        // Note: This is not efficient for large caches
        // Consider using a secondary index for production
        self.tables.invalidate_entries_if(move |(db, _)| db == &name);
    }

    pub async fn get_table(&self, db_name: &str, table_name: &str) -> Option<TableStatus> {
        if !self.config.enabled {
            return None;
        }
        let key = (db_name.to_string(), table_name.to_string());
        self.tables.get(&key).await
    }

    pub async fn put_table(&self, db_name: &str, table_name: &str, status: TableStatus) {
        if self.config.enabled {
            let key = (db_name.to_string(), table_name.to_string());
            self.tables.insert(key, status).await;
        }
    }

    pub fn invalidate_table(&self, namespace: &Namespace, name: &str) {
        let db_name = namespace.to_string();
        let key = (db_name, name.to_string());
        self.tables.invalidate(&key);
    }
}
```

### 7.5 Error Handling

```rust
// crates/sail-catalog-hms/src/error.rs

use thiserror::Error;

pub type HmsResult<T> = Result<T, HmsError>;

#[derive(Debug, Error)]
pub enum HmsError {
    #[error("HMS Thrift error: {0}")]
    Thrift(#[from] thrift::Error),

    #[error("Database not found: {0}")]
    DatabaseNotFound(String),

    #[error("Table not found: {0}.{1}")]
    TableNotFound(String, String),

    #[error("Database already exists: {0}")]
    DatabaseAlreadyExists(String),

    #[error("Table already exists: {0}.{1}")]
    TableAlreadyExists(String, String),

    #[error("Invalid URI: {0}")]
    InvalidUri(String),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("Version detection failed: {0}")]
    VersionDetectionFailed(String),

    #[error("Feature not supported: {0}")]
    NotSupported(String),

    #[error("Type conversion error: {0}")]
    TypeConversion(String),

    #[error("Connection pool error: {0}")]
    ConnectionPool(String),

    #[error("ACID transaction error: {0}")]
    Transaction(String),

    #[error("Lock acquisition failed: {0}")]
    LockFailed(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<HmsError> for CatalogError {
    fn from(err: HmsError) -> Self {
        match err {
            HmsError::DatabaseNotFound(name) => {
                CatalogError::NotFound("database", name)
            }
            HmsError::TableNotFound(db, table) => {
                CatalogError::NotFound("table", format!("{}.{}", db, table))
            }
            HmsError::DatabaseAlreadyExists(name) => {
                CatalogError::AlreadyExists("database", name)
            }
            HmsError::TableAlreadyExists(db, table) => {
                CatalogError::AlreadyExists("table", format!("{}.{}", db, table))
            }
            HmsError::NotSupported(msg) => {
                CatalogError::NotSupported(msg)
            }
            HmsError::InvalidConfig(msg) | HmsError::InvalidUri(msg) => {
                CatalogError::InvalidArgument(msg)
            }
            _ => CatalogError::External(err.to_string()),
        }
    }
}
```

---

## 8. Known Gotchas and Mitigation

### 8.1 String Encoding Issues

**Problem:** HMS doesn't enforce UTF-8; some deployments use Latin-1 or other encodings.

**Mitigation:**
```rust
fn decode_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn safe_string_conversion(hms_string: &str) -> Result<String, HmsError> {
    // Attempt UTF-8 validation
    match std::str::from_utf8(hms_string.as_bytes()) {
        Ok(s) => Ok(s.to_string()),
        Err(_) => {
            // Fallback: Replace invalid sequences
            Ok(String::from_utf8_lossy(hms_string.as_bytes()).into_owned())
        }
    }
}
```

### 8.2 Null Handling in Thrift

**Problem:** Thrift uses `Option` for optional fields, but some HMS implementations return empty strings instead of null.

**Mitigation:**
```rust
fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.and_then(|s| {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}
```

### 8.3 Timestamp Precision and Timezones

**Problem:** HMS stores timestamps as seconds since epoch, but doesn't track timezones.

**Mitigation:**
```rust
fn convert_hms_timestamp(hms_time: i32) -> Result<DateTime<Utc>, HmsError> {
    Utc.timestamp_opt(hms_time as i64, 0)
        .single()
        .ok_or_else(|| HmsError::TypeConversion("Invalid timestamp".into()))
}

fn normalize_to_utc(timestamp: i64) -> DateTime<Utc> {
    // Always assume UTC for HMS timestamps
    Utc.timestamp(timestamp, 0)
}
```

### 8.4 Partition Key Special Characters

**Problem:** Partition values can contain special characters (=, /, etc.) that need escaping.

**Mitigation:**
```rust
fn escape_partition_value(value: &str) -> String {
    // HMS uses URI-style escaping
    urlencoding::encode(value).into_owned()
}

fn unescape_partition_value(value: &str) -> Result<String, HmsError> {
    urlencoding::decode(value)
        .map(|s| s.into_owned())
        .map_err(|e| HmsError::TypeConversion(format!("Invalid partition value: {}", e)))
}

fn parse_partition_spec(spec: &str) -> Result<Vec<(String, String)>, HmsError> {
    // Parse "year=2023/month=01" format
    spec.split('/')
        .map(|part| {
            let mut kv = part.splitn(2, '=');
            let key = kv.next().ok_or(HmsError::TypeConversion("Missing key".into()))?;
            let value = kv.next().ok_or(HmsError::TypeConversion("Missing value".into()))?;
            Ok((key.to_string(), unescape_partition_value(value)?))
        })
        .collect()
}
```

### 8.5 Location URI Normalization

**Problem:** Different systems use different URI schemes (hdfs://, s3://, file://, etc.) with varying normalization.

**Mitigation:**
```rust
fn normalize_location_uri(uri: &str) -> Result<String, HmsError> {
    let parsed = url::Url::parse(uri)
        .map_err(|e| HmsError::TypeConversion(format!("Invalid URI: {}", e)))?;

    // Normalize path component
    let normalized_path = parsed.path()
        .trim_end_matches('/')  // Remove trailing slashes
        .replace("//", "/");     // Collapse multiple slashes

    // Rebuild URI
    let mut normalized = parsed.clone();
    normalized.set_path(&normalized_path);

    Ok(normalized.to_string())
}
```

### 8.6 Decimal Precision Handling

**Problem:** HMS represents decimals as strings like "decimal(10,2)" but implementations vary.

**Mitigation:**
```rust
fn parse_decimal_type(type_str: &str) -> Result<(u8, i8), HmsError> {
    // Parse "decimal(precision,scale)" or "decimal(precision)"
    let regex = regex::Regex::new(r"decimal\((\d+)(?:,\s*(\d+))?\)").unwrap();

    let captures = regex.captures(type_str)
        .ok_or_else(|| HmsError::TypeConversion(format!("Invalid decimal type: {}", type_str)))?;

    let precision: u8 = captures.get(1)
        .unwrap()
        .as_str()
        .parse()
        .map_err(|_| HmsError::TypeConversion("Invalid precision".into()))?;

    let scale: i8 = captures.get(2)
        .map(|m| m.as_str().parse().ok())
        .flatten()
        .unwrap_or(0);

    Ok((precision, scale))
}
```

### 8.7 Metadata Schema Variations

**Problem:** Spark, Trino, Presto store different metadata in table properties.

**Mitigation:**
```rust
fn extract_metadata_extensions(
    params: &HashMap<String, String>,
) -> MetadataExtensions {
    MetadataExtensions {
        // Spark-specific
        spark_version: params.get("spark.version").cloned(),
        spark_sql_sources: params.get("spark.sql.sources.provider").cloned(),

        // Trino-specific
        trino_version: params.get("trino.version").cloned(),

        // Presto-specific
        presto_version: params.get("presto_version").cloned(),

        // Iceberg-specific
        iceberg_table_type: params.get("table_type").cloned(),
        iceberg_metadata_location: params.get("metadata_location").cloned(),

        // Delta-specific
        delta_version: params.get("delta.minReaderVersion").cloned(),
    }
}
```

---

## 9. Implementation Roadmap

### Phase 1: MVP - Basic HMS Connectivity (Weeks 1-2)

**Goal:** Read-only access to databases and tables

**Tasks:**
- [x] Set up `sail-catalog-hms` crate structure
- [ ] Download and integrate Thrift definitions
- [ ] Implement basic async Thrift client
- [ ] Implement version detection
- [ ] Implement capability registry
- [ ] Implement database operations (get, list)
- [ ] Implement table operations (get, list)
- [ ] Basic type conversions (HMS ↔ Sail)
- [ ] Error handling and mapping
- [ ] Unit tests with mocks

**Deliverables:**
- Working HMS catalog provider
- Read-only database and table metadata
- ~60% code coverage

### Phase 2: Full CRUD and Configuration (Weeks 3-4)

**Goal:** Complete CatalogProvider implementation

**Tasks:**
- [ ] Implement create_database, drop_database
- [ ] Implement create_table, drop_table
- [ ] Implement view operations
- [ ] Add HMS configuration to CatalogType
- [ ] Update session initialization
- [ ] Implement connection pooling
- [ ] Implement metadata caching
- [ ] Integration tests with Docker Compose
- [ ] Documentation

**Deliverables:**
- Full CRUD operations
- Configuration integration
- Connection pooling
- ~75% code coverage

### Phase 3: ACID and Advanced Features (Weeks 5-6)

**Goal:** Support transactional tables and partitions

**Tasks:**
- [ ] Implement ACID metadata detection
- [ ] Implement transaction client (read-only)
- [ ] Implement lock client
- [ ] Partition operations (create, add, drop, list)
- [ ] Table statistics support
- [ ] Constraints support (if HMS 3.1+)
- [ ] Advanced type conversions (nested types, decimals)
- [ ] Compatibility tests across HMS versions

**Deliverables:**
- Read ACID tables
- Partition support
- ~80% code coverage

### Phase 4: Performance and Production Hardening (Weeks 7-8)

**Goal:** Production-ready implementation

**Tasks:**
- [ ] Optimize caching strategies
- [ ] Implement bulk operations
- [ ] Add retry logic with exponential backoff
- [ ] Handle network failures gracefully
- [ ] Add comprehensive logging
- [ ] Add metrics/telemetry
- [ ] Security: Kerberos authentication (if needed)
- [ ] Performance benchmarks
- [ ] Stress testing

**Deliverables:**
- Production-grade error handling
- Performance optimization
- Security hardening
- ~85% code coverage

### Phase 5: Write ACID Support (Weeks 9-10)

**Goal:** Write operations for transactional tables

**Tasks:**
- [ ] Implement transaction lifecycle (begin, commit, abort)
- [ ] Implement lock acquisition for writes
- [ ] Implement write ID allocation
- [ ] Integrate with table format writers (Iceberg, Delta)
- [ ] Handle delta files
- [ ] Compaction integration (optional)
- [ ] End-to-end ACID tests

**Deliverables:**
- Full ACID write support
- Transaction management
- ~90% code coverage

---

## 10. Success Criteria

### 10.1 Functional Requirements

- [ ] **Database Operations:**
  - Create, drop, get, list databases
  - Support for database comments and properties
  - Handle namespace hierarchies correctly

- [ ] **Table Operations:**
  - Create, drop, get, list tables
  - Support for various table types (managed, external, partitioned, bucketed)
  - Correct type mappings for all common data types
  - Handle table properties and options

- [ ] **View Operations:**
  - Create, drop, get, list views
  - Distinguish between tables and views

- [ ] **Partition Operations (Phase 3):**
  - Add, drop, list partitions
  - Handle partition spec parsing
  - Support for dynamic partitioning

- [ ] **ACID Support (Phase 3-5):**
  - Detect transactional tables
  - Read from ACID tables
  - Write to ACID tables (Phase 5)
  - Transaction lifecycle management

### 10.2 Non-Functional Requirements

- [ ] **Version Compatibility:**
  - Support HMS 2.3.x (partial)
  - Support HMS 3.0.x (full)
  - Support HMS 3.1.x (full)
  - Support HMS 4.0.x (full)
  - Graceful degradation for unsupported features

- [ ] **Performance:**
  - Metadata caching reduces HMS round-trips by >70%
  - Connection pooling maintains <10ms connection acquisition
  - Bulk operations used when available
  - Handle >10,000 tables per database

- [ ] **Reliability:**
  - Automatic retry with exponential backoff
  - Handle network failures gracefully
  - Connection health checks
  - Proper timeout handling

- [ ] **Test Coverage:**
  - Unit tests: >85% coverage
  - Integration tests: >80% coverage
  - Compatibility tests: All supported HMS versions
  - Edge case coverage for known gotchas

- [ ] **Documentation:**
  - API documentation for all public types
  - Architecture overview
  - Configuration guide
  - Compatibility matrix
  - Migration guide from other catalogs

- [ ] **Code Quality:**
  - No `unwrap()` or `expect()` in production code
  - All errors properly typed and handled
  - Follows Rust best practices
  - Passes all Clippy lints

### 10.3 Acceptance Criteria

**MVP (Phase 1-2):**
- [ ] Can connect to HMS 3.1
- [ ] Can list all databases
- [ ] Can create and drop databases
- [ ] Can list all tables in a database
- [ ] Can get table metadata with correct schema
- [ ] Can create and drop tables
- [ ] Integration tests pass against Docker HMS
- [ ] Configuration works in LakeSail session

**Full Release (Phase 3-4):**
- [ ] Supports HMS 2.3, 3.0, 3.1, 4.0
- [ ] Can read ACID tables
- [ ] Can work with partitioned tables
- [ ] Caching reduces latency by >70%
- [ ] Handles all known gotchas
- [ ] Comprehensive error messages
- [ ] Complete documentation

**Production Ready (Phase 5):**
- [ ] Can write to ACID tables
- [ ] Transaction management works correctly
- [ ] No memory leaks under stress
- [ ] >90% code coverage
- [ ] Performance benchmarks meet targets
- [ ] Security audit passed (if applicable)

---

## Appendix A: Dependencies

```toml
# crates/sail-catalog-hms/Cargo.toml

[package]
name = "sail-catalog-hms"
version = "0.1.0"
edition = "2021"

[dependencies]
# LakeSail dependencies
sail-catalog = { workspace = true }
sail-common = { workspace = true }

# Async runtime
tokio = { workspace = true, features = ["full"] }
async-trait = { workspace = true }

# Thrift
thrift = "0.17"

# Connection pooling
bb8 = "0.8"

# Caching
moka = { workspace = true, features = ["future"] }

# Error handling
thiserror = { workspace = true }
anyhow = "1.0"

# Serialization
serde = { workspace = true }

# URL parsing
url = "2.5"
urlencoding = "2.1"

# Date/time
chrono = "0.4"

# Regex
regex = "1.10"

# Logging
tracing = { workspace = true }

[dev-dependencies]
# Testing
tokio-test = "0.4"
mockall = "0.12"
testcontainers = "0.15"
rstest = "0.18"

[build-dependencies]
thrift-compiler = "0.17"
```

---

## Appendix B: Open Questions

1. **Kerberos Authentication:** Should we support Kerberos in MVP or Phase 4?
   - **Recommendation:** Phase 4, as optional feature

2. **Partition Pruning:** Should we implement partition pruning at catalog level or leave to query optimizer?
   - **Recommendation:** Leave to optimizer; catalog provides metadata

3. **Schema Registry Support:** HMS 3.0+ has schema registry; should we integrate?
   - **Recommendation:** Future work; focus on core catalog first

4. **Multi-cluster Support:** Support for federated HMS instances?
   - **Recommendation:** Future work; single HMS instance for now

5. **Caching Invalidation:** How to handle external HMS modifications?
   - **Recommendation:** TTL-based expiration; consider event-based invalidation in future

---

## Revision History

| Version | Date       | Author | Changes                      |
|---------|------------|--------|------------------------------|
| 1.0     | 2025-10-22 | Claude | Initial design document      |

