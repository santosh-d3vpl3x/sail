# Lakesail JDBC Reader - Native Spark API Integration

Complete guide to using Lakesail's JDBC reader with native Spark APIs.

## Overview

The Lakesail JDBC Reader integrates seamlessly with PySpark's standard APIs:

- ✅ **`spark.read.jdbc()`** - Standard Spark JDBC method
- ✅ **`spark.read.format("jdbc")`** - DataSource V2 style
- ✅ **High-performance backends** - ConnectorX (10-100x faster than standard JDBC)
- ✅ **Arrow-native** - Zero-copy data transfer
- ✅ **Distributed execution** - Automatic partitioning

## Installation & Setup

### 1. Install Lakesail with JDBC backend

```bash
# Install with ConnectorX (recommended - fastest)
pip install pysail[database-connectorx]

# Or with ADBC
pip install pysail[database-adbc]

# Or all backends
pip install pysail[database-all]
```

### 2. Install JDBC reader in your Spark session

```python
from pyspark.sql import SparkSession
from pysail.read import install_jdbc_reader

# Create Spark session
spark = SparkSession.builder.appName("myapp").getOrCreate()

# Install JDBC reader (one-time setup per session)
install_jdbc_reader(spark)
```

That's it! Now you can use standard Spark JDBC APIs.

## Usage

### Method 1: `spark.read.jdbc()` (Recommended)

This mimics PySpark's native `spark.read.jdbc()` signature:

```python
# Basic read
df = spark.read.jdbc(
    url="jdbc:postgresql://localhost:5432/mydb",
    table="orders",
    properties={"user": "admin", "password": "secret"}
)

# With partitioning for parallel reads
df = spark.read.jdbc(
    url="jdbc:postgresql://localhost:5432/mydb",
    table="orders",
    column="order_id",  # Partition on this column
    lowerBound=1,
    upperBound=1000000,
    numPartitions=10,  # 10 parallel tasks
    properties={"user": "admin", "password": "secret"}
)

# With custom predicates
df = spark.read.jdbc(
    url="jdbc:postgresql://localhost:5432/mydb",
    table="orders",
    predicates=["status='active'", "status='pending'", "status='completed'"],
    properties={"user": "admin"}
)
```

### Method 2: `spark.read.format("jdbc")` (DataSource V2 Style)

This uses Spark's DataSource V2 API:

```python
# Basic read
df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
    .option("dbtable", "orders") \\
    .option("user", "admin") \\
    .option("password", "secret") \\
    .load()

# With partitioning
df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
    .option("dbtable", "orders") \\
    .option("partitionColumn", "order_id") \\
    .option("lowerBound", "1") \\
    .option("upperBound", "1000000") \\
    .option("numPartitions", "10") \\
    .option("user", "admin") \\
    .load()

# With custom query
df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:mysql://localhost:3306/mydb") \\
    .option("query", "SELECT * FROM orders WHERE created_at > '2024-01-01'") \\
    .option("user", "root") \\
    .load()
```

## Complete Examples

### Example 1: PostgreSQL with Partitioning

```python
from pyspark.sql import SparkSession
from pysail.read import install_jdbc_reader

spark = SparkSession.builder.appName("postgres_example").getOrCreate()
install_jdbc_reader(spark)

# Read 10M rows in parallel using 20 partitions
df = spark.read.jdbc(
    url="jdbc:postgresql://prod-db:5432/analytics",
    table="user_events",
    column="event_id",
    lowerBound=1,
    upperBound=10000000,
    numPartitions=20,
    properties={
        "user": "readonly_user",
        "password": "secret123",
        "engine": "connectorx"  # Lakesail-specific: use ConnectorX backend
    }
)

# Now use standard Spark operations
df.filter(df.event_type == "purchase") \\
  .groupBy("user_id") \\
  .count() \\
  .show()
```

### Example 2: MySQL with Custom Query

```python
# Read with custom filtering at source
df = spark.read.jdbc(
    url="jdbc:mysql://localhost:3306/ecommerce",
    table="(SELECT * FROM orders WHERE status = 'shipped' AND created_at > '2024-01-01') AS recent_orders",
    properties={"user": "app_user", "password": "password"}
)

df.select("order_id", "customer_id", "total").show(10)
```

### Example 3: Date-Based Partitioning

```python
# Partition by date ranges (manual predicates)
df = spark.read.jdbc(
    url="jdbc:postgresql://localhost:5432/logs",
    table="application_logs",
    predicates=[
        "log_date = '2024-01-01'",
        "log_date = '2024-01-02'",
        "log_date = '2024-01-03'",
        "log_date = '2024-01-04'",
    ],
    properties={"user": "logger"}
)

df.groupBy("log_level").count().show()
```

### Example 4: Integration with Spark SQL

```python
# Read into temp view
orders_df = spark.read.jdbc(
    url="jdbc:postgresql://localhost/sales",
    table="orders",
    column="id",
    lowerBound=1,
    upperBound=1000000,
    numPartitions=10,
    properties={"user": "analyst"}
)

orders_df.createOrReplaceTempView("orders")

# Query with Spark SQL
result = spark.sql("""
    SELECT customer_id, COUNT(*) as order_count, SUM(total) as revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY customer_id
    HAVING revenue > 1000
    ORDER BY revenue DESC
    LIMIT 100
""")

result.show()
```

## Configuration Options

### Standard Spark JDBC Options

All standard Spark JDBC options are supported:

| Option | Description | Example |
|--------|-------------|---------|
| `url` | JDBC URL | `jdbc:postgresql://host/db` |
| `dbtable` / `table` | Table name or subquery | `orders` or `(SELECT ...) AS t` |
| `user` | Database user | `admin` |
| `password` | Database password | `secret` |
| `column` / `partitionColumn` | Column for range partitioning | `id` |
| `lowerBound` | Partition range lower bound | `1` |
| `upperBound` | Partition range upper bound | `1000000` |
| `numPartitions` | Number of partitions | `10` |
| `predicates` | List of WHERE predicates | `["status='A'", "status='B'"]` |
| `fetchsize` | Batch size for fetching | `10000` |

### Lakesail-Specific Options

| Option | Description | Default |
|--------|-------------|---------|
| `engine` | Backend engine (`connectorx`, `adbc`, `fallback`) | `connectorx` |

Specify via `properties` dict:

```python
df = spark.read.jdbc(
    url="...",
    table="...",
    properties={
        "user": "admin",
        "engine": "connectorx"  # Lakesail-specific
    }
)
```

## Performance Tips

### 1. Use ConnectorX Backend

ConnectorX is 10-100x faster than standard JDBC:

```python
properties = {
    "user": "admin",
    "password": "secret",
    "engine": "connectorx"  # Fastest option
}
```

### 2. Enable Partitioning

For large tables, use partitioning:

```python
df = spark.read.jdbc(
    url="...",
    table="large_table",
    column="id",  # Partition on this column
    lowerBound=1,
    upperBound=10000000,
    numPartitions=20  # Adjust based on cluster size
)
```

### 3. Push Filters to Database

Use subqueries to filter at the source:

```python
# Good: Filter at database
df = spark.read.jdbc(
    url="...",
    table="(SELECT * FROM orders WHERE created_at > '2024-01-01') AS recent"
)

# Less efficient: Filter after loading
df = spark.read.jdbc(url="...", table="orders")
df = df.filter("created_at > '2024-01-01'")  # Loads all data first
```

### 4. Adjust Fetch Size

For memory-constrained environments:

```python
df = spark.read \\
    .format("jdbc") \\
    .option("fetchsize", "5000") \\  # Smaller batches
    .option("url", "...") \\
    .load()
```

## Supported Databases

### ConnectorX Backend
- ✅ PostgreSQL
- ✅ MySQL / MariaDB
- ✅ SQL Server
- ✅ Oracle
- ✅ SQLite
- ✅ Redshift
- ✅ ClickHouse

### ADBC Backend
- ✅ PostgreSQL
- ✅ SQLite
- ✅ Snowflake

## Migration from Standard PySpark JDBC

If you're using PySpark's built-in JDBC reader:

### Before (Standard PySpark JDBC)

```python
df = spark.read \\
    .format("jdbc") \\
    .option("url", "jdbc:postgresql://localhost/db") \\
    .option("dbtable", "orders") \\
    .option("user", "admin") \\
    .option("password", "secret") \\
    .option("driver", "org.postgresql.Driver") \\  # Need driver JAR
    .load()
```

### After (Lakesail JDBC with ConnectorX)

```python
from pysail.read import install_jdbc_reader

install_jdbc_reader(spark)

# Same API, no driver JAR needed, 10-100x faster!
df = spark.read.jdbc(
    url="jdbc:postgresql://localhost/db",
    table="orders",
    properties={"user": "admin", "password": "secret"}
)
```

**Benefits:**
- ✅ No JDBC driver JARs needed
- ✅ 10-100x faster (via ConnectorX)
- ✅ Arrow-native data transfer
- ✅ Same familiar API

## Troubleshooting

### "JDBC reader not installed"

```python
# Make sure to install before use
from pysail.read import install_jdbc_reader
install_jdbc_reader(spark)
```

### Slow performance

```python
# Use ConnectorX backend (fastest)
properties = {"user": "...", "engine": "connectorx"}

# Enable partitioning
df = spark.read.jdbc(..., column="id", numPartitions=10, ...)
```

### Memory issues

```python
# Reduce fetch size
.option("fetchsize", "1000")

# Increase number of partitions
.option("numPartitions", "20")
```

## See Also

- [Full JDBC Reader Documentation](README.md)
- [ArrowBatchDataSource Architecture](arrow_datasource.py)
- [Backend Implementations](backends/)
