# Lakesail JDBC Reader

High-performance JDBC database reading for PySpark using Arrow-native backends.

## Overview

The Lakesail JDBC Reader provides distributed database reading capabilities using high-performance backends:

- **ConnectorX**: Rust-based, fastest option for most databases
- **ADBC**: Arrow Database Connectivity standard, good compatibility
- **Fallback**: pyodbc-based fallback (slow, for testing only)

Key features:
- ✅ Arrow-native data transfer (zero-copy when possible)
- ✅ Distributed execution via Spark's `mapPartitions`
- ✅ Credential masking in logs and errors
- ✅ Range-based and predicate-based partitioning
- ✅ Type preservation (DECIMAL, TIMESTAMP, etc.)
- ✅ Comprehensive error handling

## Installation

### Install with ConnectorX backend (recommended)

```bash
pip install pysail[database-connectorx]
```

### Install with ADBC backend

```bash
pip install pysail[database-adbc]
```

### Install all backends

```bash
pip install pysail[database-all]
```

## Quick Start

```python
from pyspark.sql import SparkSession
from pysail.read import read_jdbc

spark = SparkSession.builder.appName("myapp").getOrCreate()

# Basic read
df = read_jdbc(
    spark,
    url="jdbc:postgresql://localhost:5432/mydb",
    dbtable="orders",
    user="admin",
    password="secret"
)

df.show()
```

## Usage Examples

### Basic Table Read

```python
df = read_jdbc(
    spark,
    url="jdbc:postgresql://localhost:5432/mydb",
    dbtable="orders",
    user="admin",
    password="secret",
    engine="connectorx"  # Default
)
```

### Custom Query

```python
df = read_jdbc(
    spark,
    url="jdbc:mysql://localhost:3306/mydb",
    query="SELECT * FROM orders WHERE created_at > '2024-01-01'",
    user="root",
    password="password"
)
```

### Distributed Read with Partitioning

```python
# Range-based partitioning
df = read_jdbc(
    spark,
    url="jdbc:postgresql://localhost:5432/mydb",
    dbtable="orders",
    user="admin",
    password="secret",
    partition_column="order_id",
    lower_bound=1,
    upper_bound=1000000,
    num_partitions=10  # 10 parallel tasks
)
```

### Predicate-based Partitioning

```python
# Explicit predicates (one per partition)
df = read_jdbc(
    spark,
    url="jdbc:postgresql://localhost:5432/mydb",
    dbtable="orders",
    predicates="status='active',status='pending',status='completed'"
)
```

### Using ADBC Backend

```python
df = read_jdbc(
    spark,
    url="jdbc:postgresql://localhost:5432/mydb",
    dbtable="orders",
    user="admin",
    password="secret",
    engine="adbc"  # Use ADBC instead of ConnectorX
)
```

### Subquery as Table

```python
df = read_jdbc(
    spark,
    url="jdbc:postgresql://localhost:5432/mydb",
    dbtable="(SELECT * FROM orders WHERE amount > 100) AS high_value_orders"
)
```

## Configuration Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `url` | String | Yes | - | JDBC URL (e.g., `jdbc:postgresql://host/db`) |
| `dbtable` | String | No* | - | Table name or `(SELECT ...) AS alias` |
| `query` | String | No* | - | SQL query (alternative to `dbtable`) |
| `user` | String | No | - | Database user (can also be in URL) |
| `password` | String | No | - | Database password (can also be in URL) |
| `engine` | String | No | `connectorx` | Backend: `connectorx`, `adbc`, or `fallback` |
| `partition_column` | String | No | - | Column for range partitioning |
| `lower_bound` | Int | No | - | Partition range lower bound |
| `upper_bound` | Int | No | - | Partition range upper bound |
| `num_partitions` | Int | No | 1 | Number of partitions for parallelism |
| `predicates` | String | No | - | Comma-separated WHERE predicates |
| `fetch_size` | Int | No | 10000 | Batch size for fetching |

*Either `dbtable` or `query` must be specified (not both)

## Supported Databases

### ConnectorX Backend
- PostgreSQL
- MySQL/MariaDB
- SQL Server
- Oracle
- SQLite
- Redshift
- ClickHouse

### ADBC Backend
- PostgreSQL
- SQLite
- Snowflake

### Fallback Backend
- Any database with ODBC driver (slow, not recommended for production)

## Performance Tips

1. **Use ConnectorX for best performance**: It's 10-100x faster than fallback
2. **Enable partitioning for large tables**: Use `partition_column` with appropriate bounds
3. **Adjust `fetch_size`**: Larger values reduce round trips but use more memory
4. **Use predicate pushdown**: Filter data at the source when possible

## Security

All credentials are automatically masked in logs and error messages:

```python
# Input: jdbc:postgresql://user:password@localhost:5432/mydb
# Logged as: jdbc:postgresql://***:***@localhost:5432/mydb
```

Session initialization statements are validated to prevent DML:

```python
# ✅ Allowed
read_jdbc(..., session_init_statement="SET timezone='UTC'")

# ❌ Rejected
read_jdbc(..., session_init_statement="INSERT INTO audit VALUES (1)")
```

## Type Mapping

Arrow types are preserved from the database with no casting:

| Database Type | Arrow Type | Spark SQL Type |
|---------------|------------|----------------|
| INTEGER | int32 | IntegerType |
| BIGINT | int64 | LongType |
| DECIMAL(p,s) | decimal128(p,s) | DecimalType(p,s) |
| VARCHAR | utf8 | StringType |
| TIMESTAMP | timestamp[us] | TimestampType |
| DATE | date32 | DateType |
| BOOLEAN | bool | BooleanType |
| BYTEA | binary | BinaryType |

## Logging

Enable detailed logging:

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lakesail.jdbc")
logger.setLevel(logging.DEBUG)
```

Log output includes:
- Connection info (with masked credentials)
- Partition metrics (rows, bytes, wall time)
- Query execution details
- Error messages with full context

## Error Handling

Common errors and solutions:

### `BackendNotAvailableError`

```
Backend 'connectorx' not installed.
```

**Solution**: Install the backend: `pip install pysail[database-connectorx]`

### `InvalidJDBCUrlError`

```
Invalid JDBC URL: must start with 'jdbc:'
```

**Solution**: Ensure URL starts with `jdbc:` prefix

### `InvalidOptionsError`

```
Either 'dbtable' or 'query' option must be specified
```

**Solution**: Provide either `dbtable` or `query` (not both)

### `SchemaInferenceError`

```
Query returned no results for schema inference
```

**Solution**: Ensure the table/query returns at least one row

## Advanced Usage

### Custom Connection String

For databases with non-standard JDBC URLs:

```python
df = read_jdbc(
    spark,
    url="jdbc:sqlserver://localhost:1433;database=mydb;encrypt=true",
    dbtable="orders"
)
```

### Multiple Predicates

```python
# Read data in chunks by date
df = read_jdbc(
    spark,
    url="jdbc:postgresql://localhost:5432/mydb",
    dbtable="events",
    predicates=",".join([
        "event_date='2024-01-01'",
        "event_date='2024-01-02'",
        "event_date='2024-01-03'",
    ])
)
```

### With Spark SQL

```python
from pysail.read import read_jdbc

# Read into temp view
df = read_jdbc(spark, url="...", dbtable="orders")
df.createOrReplaceTempView("orders")

# Query with Spark SQL
result = spark.sql("""
    SELECT customer_id, SUM(amount) as total
    FROM orders
    GROUP BY customer_id
""")
```

## Architecture

```
User Code
    ↓
read_jdbc()
    ↓
Parse & Validate Options
    ↓
Plan Partitions
    ↓
mapPartitions (distributed execution)
    ├─ Partition 0 → Backend → Arrow Batches
    ├─ Partition 1 → Backend → Arrow Batches
    └─ Partition N → Backend → Arrow Batches
    ↓
Arrow Schema Inference
    ↓
Spark DataFrame (Arrow-backed)
```

## Troubleshooting

### Slow Performance

1. Check backend: Use `engine="connectorx"` (fastest)
2. Enable partitioning: Set `partition_column` and `num_partitions`
3. Increase fetch size: Try `fetch_size=50000`

### Memory Issues

1. Reduce `fetch_size` to use less memory per batch
2. Increase `num_partitions` to split work across more executors
3. Use `query` to filter data at source

### Connection Errors

1. Verify JDBC URL format: `jdbc:driver://host:port/database`
2. Check credentials: Ensure user/password are correct
3. Test connectivity: Try connecting with native database client

## Contributing

Issues and PRs welcome at: https://github.com/lakehq/sail/issues

## License

Apache License 2.0
