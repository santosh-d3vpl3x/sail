# HMS Kerberos Testing Environment

This directory contains a complete Kerberos-enabled Hive Metastore testing environment for validating the HMS catalog provider with authentication.

## Overview

The test environment includes:

- **MIT Kerberos KDC** - Key Distribution Center for authentication
- **Kerberized HMS 3.1.3** - Hive Metastore with SASL/GSSAPI enabled
- **PostgreSQL 15** - HMS metadata backend
- **MinIO** - S3-compatible object storage
- **PySpark Client** - Interactive testing client with Kerberos support
- **Test Runner** - Automated test execution container

## Quick Start

### 1. Start the Environment

```bash
cd crates/sail-catalog-hms/docker
docker-compose -f docker-compose-kerberos.yml up -d
```

### 2. Wait for Services (2-3 minutes)

```bash
# Watch logs for all services
docker-compose -f docker-compose-kerberos.yml logs -f

# Or check specific service
docker logs hms-kdc -f
docker logs hms-kerberized -f
```

### 3. Run Automated Tests

```bash
# Run all PySpark tests
docker-compose -f docker-compose-kerberos.yml run --rm test-runner

# View test results
ls -lh test-results/
```

### 4. Interactive Testing

```bash
# Start interactive PySpark shell
docker-compose -f docker-compose-kerberos.yml run --rm pyspark-client

# Inside the container:
kinit -kt /etc/security/keytabs/client.keytab client@EXAMPLE.COM
pyspark

# Then in PySpark:
spark.sql("SHOW DATABASES").show()
spark.sql("CREATE DATABASE test_db")
spark.sql("USE test_db")

data = [(1, "Alice", 30), (2, "Bob", 25)]
df = spark.createDataFrame(data, ["id", "name", "age"])
df.write.saveAsTable("employees")

spark.table("employees").show()
```

### 5. Cleanup

```bash
docker-compose -f docker-compose-kerberos.yml down -v
```

## Test Coverage

The test suite validates the following PySpark APIs against Kerberized HMS:

### Basic Operations (`test_basic_operations.py`)
- `spark.sql("CREATE DATABASE ...")` - Database creation
- `spark.sql("SHOW DATABASES")` - Database listing
- `spark.table(name)` - Table reading
- `spark.sql("DESCRIBE ...")` - Schema inspection
- `spark.sql("DROP TABLE ...")` - Table deletion
- SQL SELECT queries with filters
- SQL aggregation queries (GROUP BY, AVG, COUNT)

### Write Operations (`test_write_operations.py`)
- `df.write.saveAsTable(name)` - Create/overwrite tables
- `df.write.mode("append").saveAsTable(name)` - Append to tables
- `df.write.insertInto(name)` - Insert into existing tables
- `df.write.mode("overwrite").insertInto(name)` - Overwrite via insertInto
- Partitioned table writes
- CREATE TABLE AS SELECT (CTAS)
- Multiple sequential inserts
- Write options (format, compression)

### Partition Operations (`test_partition_operations.py`)
- `df.write.partitionBy("col").saveAsTable(...)` - Create partitioned tables
- `spark.sql("SHOW PARTITIONS ...")` - List partitions
- Partition pruning and filter pushdown
- Multi-level partitioning (year/month)
- `spark.sql("ALTER TABLE ... DROP PARTITION ...")` - Drop partitions
- Dynamic partition insertion
- Partition statistics

### Schema Operations (`test_schema_operations.py`)
- Complex types: Arrays, Maps, Structs
- Nested complex types (array of structs)
- `spark.sql("ALTER TABLE ... ADD COLUMNS ...")` - Add columns
- Decimal/numeric types
- Timestamp types
- NULL value handling
- `spark.sql("DESCRIBE FORMATTED ...")` - Detailed schema

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network                            │
│                  (hms-kerb-network)                          │
│                                                              │
│  ┌──────────────┐    ┌─────────────────┐                   │
│  │     KDC      │    │    PostgreSQL   │                   │
│  │ (Kerberos)   │    │   (Metadata)    │                   │
│  │              │    │                 │                   │
│  │ - Principals │    │ DB: metastore   │                   │
│  │ - Keytabs    │    │ User: hive      │                   │
│  └──────┬───────┘    └────────┬────────┘                   │
│         │                     │                             │
│         │  ┌──────────────────┴──────────────┐             │
│         │  │                                  │             │
│         ▼  ▼                                  │             │
│  ┌─────────────────┐                         │             │
│  │  HMS (Kerb)     │                         │             │
│  │  Port: 9083     │                         │             │
│  │                 │                         │             │
│  │ - SASL enabled  │                         │             │
│  │ - hive.keytab   │                         │             │
│  └────────┬────────┘                         │             │
│           │                                  │             │
│           │  ┌───────────────────────────────┘             │
│           │  │                                             │
│           ▼  ▼                                             │
│  ┌──────────────────┐        ┌──────────────┐             │
│  │  PySpark Client  │        │    MinIO     │             │
│  │                  │        │  (S3 Storage)│             │
│  │ - client.keytab  │        │              │             │
│  │ - Test suite     │        │ Port: 9000   │             │
│  └──────────────────┘        └──────────────┘             │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Kerberos Configuration

### Realm Configuration

- **Realm**: `EXAMPLE.COM`
- **KDC**: `kdc.example.com:88`
- **Admin Server**: `kdc.example.com:749`

### Principals Created

1. **HMS Service Principal**
   - Principal: `hive/hms.example.com@EXAMPLE.COM`
   - Keytab: `/etc/security/keytabs/hive.keytab`
   - Used by: HMS server

2. **Client Principal**
   - Principal: `client@EXAMPLE.COM`
   - Keytab: `/etc/security/keytabs/client.keytab`
   - Used by: PySpark client, test suite

3. **Test User Principal**
   - Principal: `testuser@EXAMPLE.COM`
   - Password: `testpass`
   - Keytab: `/etc/security/keytabs/testuser.keytab`
   - Used by: Interactive testing

### SASL Configuration

- **Mechanism**: GSSAPI (Kerberos)
- **QoP**: `auth` (authentication only)
- **Mutual Auth**: Enabled

## Files

```
docker/
├── docker-compose-kerberos.yml    # Docker Compose configuration
├── Dockerfile.pyspark-kerb         # PySpark + Kerberos image
├── kerberos-init.sh                # KDC initialization script
├── krb5.conf                       # Kerberos client config
├── hive-site-kerberos.xml          # HMS Kerberos config
└── pyspark-tests/                  # Test suite
    ├── conftest.py                 # PyTest fixtures
    ├── test_basic_operations.py    # Basic table ops
    ├── test_write_operations.py    # Write/insert ops
    ├── test_partition_operations.py # Partition ops
    ├── test_schema_operations.py   # Schema/types ops
    └── run-all-tests.sh            # Test runner script
```

## Running Specific Tests

```bash
# Run only basic operations tests
docker-compose -f docker-compose-kerberos.yml run --rm pyspark-client \
  pytest /tests/test_basic_operations.py -v

# Run specific test
docker-compose -f docker-compose-kerberos.yml run --rm pyspark-client \
  pytest /tests/test_basic_operations.py::test_spark_table_api -v

# Run with more verbose output
docker-compose -f docker-compose-kerberos.yml run --rm pyspark-client \
  pytest /tests -v -s
```

## Debugging

### Check Kerberos Status

```bash
# List principals
docker exec hms-kdc kadmin.local -q "listprincs"

# Check keytabs
docker exec hms-kdc ls -lh /keytabs/
docker exec hms-kdc klist -kte /keytabs/hive.keytab

# Verify client can authenticate
docker-compose -f docker-compose-kerberos.yml run --rm pyspark-client bash
kinit -kt /etc/security/keytabs/client.keytab client@EXAMPLE.COM
klist
```

### Check HMS Status

```bash
# HMS logs
docker logs hms-kerberized

# Check HMS port
docker exec hms-kerberized netstat -tlnp | grep 9083

# Test Thrift connection (without auth)
docker exec hms-kerberized nc -zv localhost 9083
```

### Check PostgreSQL

```bash
# Connect to PostgreSQL
docker exec -it hms-postgres-kerb psql -U hive -d metastore

# List tables
\dt

# Check databases in HMS
SELECT * FROM "DBS";
```

### Common Issues

#### KDC Initialization Failed

```bash
# Check KDC logs
docker logs hms-kdc

# Restart KDC
docker-compose -f docker-compose-kerberos.yml restart kdc

# Wait for health check
docker-compose -f docker-compose-kerberos.yml ps
```

#### HMS Connection Refused

```bash
# Check if HMS started successfully
docker logs hms-kerberized | grep -i "started"

# Check SASL configuration
docker exec hms-kerberized cat /opt/hive/conf/hive-site.xml | grep -A2 "sasl.enabled"

# Verify keytab is mounted
docker exec hms-kerberized ls -lh /etc/security/keytabs/hive.keytab
```

#### Authentication Failed

```bash
# Verify krb5.conf is correct
docker exec pyspark-client-kerb cat /etc/krb5.conf

# Check if KDC is reachable
docker exec pyspark-client-kerb nc -zv kdc.example.com 88

# Try manual kinit
docker exec pyspark-client-kerb kinit -kt /etc/security/keytabs/client.keytab client@EXAMPLE.COM
docker exec pyspark-client-kerb klist
```

## CI/CD Integration

The GitHub Actions workflow (`.github/workflows/hms-kerberos-tests.yml`) runs these tests automatically on:

- Push to `main` branch
- Push to `claude/**` branches
- Pull requests affecting HMS code
- Manual workflow dispatch

### Workflow Steps

1. Build Docker images with layer caching
2. Start Kerberos + HMS environment
3. Wait for services to be healthy
4. Verify Kerberos setup (principals, keytabs)
5. Run PySpark test suite
6. Upload test results and JUnit reports
7. Publish test report summary

### Viewing Test Results

Test results are available as workflow artifacts:
- `hms-kerberos-test-results` - Full HTML report
- `junit-results` - JUnit XML for CI integration

## Performance Considerations

### Container Resource Requirements

- **KDC**: 256MB RAM, minimal CPU
- **HMS**: 2GB RAM, 1 CPU
- **PostgreSQL**: 512MB RAM, 1 CPU
- **PySpark Client**: 2GB RAM, 2 CPUs

### Startup Times

- KDC: ~10 seconds
- PostgreSQL: ~5 seconds
- HMS: ~60-90 seconds (includes schema initialization)
- Total: ~2-3 minutes for full environment

### Test Execution Times

- Basic operations: ~30 seconds
- Write operations: ~45 seconds
- Partition operations: ~60 seconds
- Schema operations: ~45 seconds
- **Total suite**: ~3-5 minutes

## Security Notes

⚠️ **This environment is for testing only!**

- Default realm: `EXAMPLE.COM`
- Default passwords: `krbpass`, `testpass`
- Network: Docker bridge (not exposed to host)
- Keytabs: Stored in Docker volumes

**Do NOT use this configuration in production!**

For production Kerberos setup, see: `docs/kerberos-setup.md`

## Next Steps

1. **Validate Rust HMS Client**: Test the Rust HMS catalog provider against this environment
2. **Add ACID Tests**: Test transactional tables (HMS 3.0+)
3. **Add Delegation Token Tests**: Test token-based auth
4. **Performance Benchmarks**: Measure query performance with/without Kerberos
5. **Multi-Version Testing**: Test against HMS 2.3, 3.1, 4.0

## References

- [Apache Hive Security](https://cwiki.apache.org/confluence/display/Hive/SettingUpHiveServer2#SettingUpHiveServer2-Kerberos)
- [MIT Kerberos Documentation](https://web.mit.edu/kerberos/krb5-latest/doc/)
- [PySpark Hive Integration](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html)
- [SASL/GSSAPI](https://www.ietf.org/rfc/rfc2222.txt)
