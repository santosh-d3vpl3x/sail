# sail-catalog-hms

Hive Metastore (HMS) catalog provider for LakeSail.

## Overview

This crate provides a `CatalogProvider` implementation that connects to Apache Hive Metastore via the Thrift protocol. It enables LakeSail to work with HMS-managed databases, tables, and metadata.

## Features

- **Multi-version Support**: Automatically detects and adapts to HMS versions 2.3.x through 4.0.x
- **Connection Pooling**: Efficient connection reuse with configurable pool sizes
- **Metadata Caching**: High-performance caching layer to reduce HMS round-trips
- **Async-First**: Built on Tokio for high-performance async I/O
- **Graceful Degradation**: Automatically falls back to compatible APIs for older HMS versions
- **ACID Support** (planned): Support for transactional tables in HMS 3.0+

## Usage

### Configuration

Add HMS catalog to your LakeSail configuration:

```yaml
catalog:
  default_catalog: "hive"
  default_database: ["default"]

  list:
    - name: "hive"
      type: "hive_metastore"
      uri: "thrift://localhost:9083"
      username: "hive"

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

### Programmatic Usage

```rust
use sail_catalog_hms::{HmsConfig, HmsProvider};
use sail_catalog::provider::CatalogProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = HmsConfig {
        name: "hive".to_string(),
        uri: "thrift://localhost:9083".to_string(),
        username: "hive".to_string(),
        ..Default::default()
    };

    let provider = HmsProvider::new(config).await?;

    // List databases
    let databases = provider.list_databases(None).await?;
    println!("Found {} databases", databases.len());

    // Get table metadata
    let table = provider.get_table(&namespace, "my_table").await?;
    println!("Table schema: {:?}", table);

    Ok(())
}
```

## Architecture

The HMS provider is structured in multiple layers:

```
┌─────────────────────────────────────┐
│      HmsProvider                    │  ← CatalogProvider implementation
│  (Cache + Connection Pool)          │
└───────────────┬─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│      HmsClient                      │  ← Async Thrift client wrapper
│  (Retry + Version Detection)        │
└───────────────┬─────────────────────┘
                │
                ▼
┌─────────────────────────────────────┐
│      Thrift Protocol                │  ← Generated from HMS Thrift IDL
│  (Binary/Compact)                   │
└─────────────────────────────────────┘
```

## Compatibility Matrix

| HMS Version | Support Level | Features                                   |
|-------------|---------------|--------------------------------------------|
| 2.0-2.2     | Partial       | Basic tables, partitions                   |
| 2.3.x       | Full          | + Statistics, bulk operations              |
| 3.0.x       | Full          | + ACID (read), materialized views          |
| 3.1.x       | Full          | + Constraints, schema registry             |
| 4.0.x       | Full          | + Native Iceberg/Delta support             |

## Implementation Status

### ✅ Completed

- [x] Core crate structure
- [x] Configuration management
- [x] Error handling
- [x] Version detection framework
- [x] Capability registry
- [x] Connection pooling infrastructure
- [x] Metadata caching layer
- [x] Type conversion utilities
- [x] CatalogProvider trait skeleton

### 🚧 In Progress

- [ ] Thrift client implementation
- [ ] Database operations (create, drop, get, list)
- [ ] Table operations (create, drop, get, list)
- [ ] Partition operations
- [ ] View operations

### 📋 Planned

- [ ] ACID transaction support (Phase 3)
- [ ] Lock service integration (Phase 3)
- [ ] Table statistics (Phase 3)
- [ ] Constraints support (Phase 3)
- [ ] Kerberos authentication (Phase 4)
- [ ] Performance optimizations (Phase 4)

## Development

### Prerequisites

- Rust 1.87.0 or later
- Docker (for running test HMS instances)
- Apache Thrift compiler (optional, for regenerating bindings)

### Running Tests

```bash
# Unit tests
cargo test --package sail-catalog-hms

# Integration tests (requires Docker)
docker-compose -f crates/sail-catalog-hms/docker/docker-compose.yml up -d
cargo test --package sail-catalog-hms --features integration-tests

# Clean up
docker-compose -f crates/sail-catalog-hms/docker/docker-compose.yml down
```

### Testing Against Different HMS Versions

The docker-compose setup provides multiple HMS versions:

```bash
# Test against HMS 2.3
HMS_VERSION=2.3 cargo test --package sail-catalog-hms --features integration-tests

# Test against HMS 3.1
HMS_VERSION=3.1 cargo test --package sail-catalog-hms --features integration-tests

# Test against HMS 4.0
HMS_VERSION=4.0 cargo test --package sail-catalog-hms --features integration-tests
```

## Contributing

Contributions are welcome! Please see the main [LakeSail contributing guide](../../CONTRIBUTING.md).

### Areas for Contribution

- Thrift client implementation
- ACID transaction support
- Performance optimization
- Additional HMS version testing
- Documentation improvements

## Resources

- [Hive Metastore Documentation](https://cwiki.apache.org/confluence/display/Hive/Design)
- [Thrift Protocol](https://thrift.apache.org/docs/)
- [LakeSail Documentation](https://docs.lakesail.com/)

## License

Apache License 2.0 - See [LICENSE](../../LICENSE) for details.
