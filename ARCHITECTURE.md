# Lakesail JDBC Reader - Framework-Agnostic Architecture

## Overview

The Lakesail JDBC Reader is built with a **clean separation of concerns**:
- **Core layer**: Pure Python, only depends on Arrow
- **Adapter layer**: Framework-specific integration (Spark, Polars, etc.)

This architecture allows:
- ✅ Using JDBC reader without any framework
- ✅ Extending Lakesail with new data sources
- ✅ Integrating with multiple frameworks via adapters
- ✅ Testing core functionality without Spark/other frameworks

## Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│  Core Layer (Pure Python + Arrow only)                      │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────────────────────────────────────────┐   │
│  │  ArrowBatchDataSource (abstract base class)          │   │
│  │  - infer_schema() → pa.Schema                        │   │
│  │  - plan_partitions() → List[Dict]                    │   │
│  │  - read_partition() → Iterator[pa.RecordBatch]       │   │
│  │  - read_all() → Iterator[pa.RecordBatch]             │   │
│  │  - to_arrow_table() → pa.Table                       │   │
│  └──────────────────────────────────────────────────────┘   │
│                           ▲                                  │
│                           │ extends                          │
│                           │                                  │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  JDBCArrowDataSource                                 │   │
│  │  PostgreSQLDataSource (future)                       │   │
│  │  RESTApiDataSource (future)                          │   │
│  │  S3DataSource (future)                               │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
│  Supporting Modules (no framework dependencies):            │
│  - backends/ (ConnectorX, ADBC, fallback)                   │
│  - jdbc_options.py (options normalization)                  │
│  - jdbc_url_parser.py (URL parsing)                         │
│  - partition_planner.py (partition generation)              │
│  - query_builder.py (safe query construction)               │
│  - utils.py (credential masking)                            │
│  - exceptions.py (error types)                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Adapter Layer (Framework-specific)                         │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────┐  │
│  │  spark_adapter   │  │  polars_adapter  │  │ duckdb   │  │
│  │  (implemented)   │  │    (future)      │  │ (future) │  │
│  └──────────────────┘  └──────────────────┘  └──────────┘  │
│           │                      │                  │        │
│           ▼                      ▼                  ▼        │
│  Spark DataFrame        Polars DataFrame     DuckDB Table   │
└─────────────────────────────────────────────────────────────┘
```

## Core Abstraction: ArrowBatchDataSource

The `ArrowBatchDataSource` is Lakesail's core abstraction for data sources.

### Key Design Principles:

1. **Framework-Agnostic**: No PySpark, Polars, or other framework dependencies
2. **Arrow-Native**: All data exchange via Arrow RecordBatches
3. **Serializable**: Partition specs are JSON-compatible dicts
4. **Extensible**: Easy to implement new data sources
5. **Testable**: Can be tested without any framework

### Interface:

```python
from abc import ABC, abstractmethod
import pyarrow as pa

class ArrowBatchDataSource(ABC):
    @abstractmethod
    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        """Return Arrow schema for the data."""
        pass

    @abstractmethod
    def plan_partitions(self, options: Dict[str, str]) -> List[Dict[str, Any]]:
        """Return partition specifications (JSON-serializable)."""
        pass

    @abstractmethod
    def read_partition(
        self,
        partition_spec: Dict[str, Any],
        options: Dict[str, str]
    ) -> Iterator[pa.RecordBatch]:
        """Read Arrow batches for one partition."""
        pass

    # Convenience methods (provided by base class)
    def read_all(self, options) -> Iterator[pa.RecordBatch]:
        """Read all partitions sequentially."""

    def to_arrow_table(self, options) -> pa.Table:
        """Read all data as single Arrow Table."""
```

## Usage Patterns

### Pattern 1: Pure Arrow (No Framework)

```python
from pysail.read.arrow_datasource import JDBCArrowDataSource

# Create datasource
datasource = JDBCArrowDataSource()
options = {
    'url': 'jdbc:postgresql://localhost:5432/mydb',
    'dbtable': 'orders',
    'user': 'admin',
    'password': 'secret'
}

# Get Arrow Table
table = datasource.to_arrow_table(options)

# Convert to pandas/polars/etc
df = table.to_pandas()
print(df)
```

**Dependencies**: Only PyArrow (no Spark, no Polars)

### Pattern 2: With Spark (Via Adapter)

```python
from pyspark.sql import SparkSession
from pysail.read.arrow_datasource import JDBCArrowDataSource
from pysail.read.spark_adapter import to_spark_dataframe

spark = SparkSession.builder.getOrCreate()

# Create datasource (no Spark dependency)
datasource = JDBCArrowDataSource()
options = {'url': '...', 'dbtable': '...'}

# Convert to Spark DataFrame (via adapter)
df = to_spark_dataframe(spark, datasource, options)
df.show()
```

**Dependencies**: PyArrow + PySpark

### Pattern 3: Convenience API

```python
from pyspark.sql import SparkSession
from pysail.read import read_jdbc

spark = SparkSession.builder.getOrCreate()

# Convenience function (internally uses adapter)
df = read_jdbc(
    spark,
    url='jdbc:postgresql://localhost:5432/mydb',
    dbtable='orders',
    user='admin',
    password='secret'
)
df.show()
```

**Dependencies**: PyArrow + PySpark

### Pattern 4: Spark Native API (Convenience Wrapper)

```python
from pyspark.sql import SparkSession
from pysail.read import install_jdbc_reader

spark = SparkSession.builder.getOrCreate()

# Install wrapper (one-time setup)
install_jdbc_reader(spark)

# Now use standard Spark API
df = spark.read.jdbc(
    url='jdbc:postgresql://localhost:5432/mydb',
    table='orders',
    properties={'user': 'admin', 'password': 'secret'}
)
df.show()
```

**Note**: This is a thin convenience wrapper around `read_jdbc()`.

## Extending Lakesail with New Data Sources

### Example: REST API Data Source

```python
from pysail.read.arrow_datasource import ArrowBatchDataSource
import pyarrow as pa
import requests

class RESTApiDataSource(ArrowBatchDataSource):
    def infer_schema(self, options):
        # Fetch first record to infer schema
        url = options['url']
        response = requests.get(f"{url}?limit=1")
        data = response.json()
        # Convert to Arrow schema
        return pa.schema([...])

    def plan_partitions(self, options):
        # Partition by page
        total_pages = int(options.get('total_pages', 10))
        return [
            {'partition_id': i, 'page': i}
            for i in range(total_pages)
        ]

    def read_partition(self, partition_spec, options):
        # Fetch one page
        url = options['url']
        page = partition_spec['page']
        response = requests.get(f"{url}?page={page}")
        data = response.json()
        # Convert to Arrow batch
        yield pa.RecordBatch.from_pylist(data)

# Use directly
datasource = RESTApiDataSource()
table = datasource.to_arrow_table({'url': 'https://api.example.com/data'})

# Or with Spark
from pysail.read.spark_adapter import to_spark_dataframe
df = to_spark_dataframe(spark, datasource, options)
```

### Example: S3 Parquet Data Source

```python
class S3ParquetDataSource(ArrowBatchDataSource):
    def infer_schema(self, options):
        # Read schema from first file
        import pyarrow.parquet as pq
        path = options['path']
        return pq.read_schema(path)

    def plan_partitions(self, options):
        # One partition per file
        import boto3
        s3 = boto3.client('s3')
        bucket = options['bucket']
        prefix = options['prefix']
        files = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [
            {'partition_id': i, 'key': obj['Key']}
            for i, obj in enumerate(files['Contents'])
        ]

    def read_partition(self, partition_spec, options):
        # Read one file
        import pyarrow.parquet as pq
        bucket = options['bucket']
        key = partition_spec['key']
        table = pq.read_table(f"s3://{bucket}/{key}")
        for batch in table.to_batches():
            yield batch
```

## Spark Integration Details

The Spark adapter (`spark_adapter.py`) handles:

1. **Schema Conversion**: Arrow schema → Spark SQL types
2. **Distributed Execution**: Using `mapPartitions`
3. **Serialization**: Broadcasts config dicts (not objects)
4. **Data Conversion**: Arrow RecordBatch → Spark Rows

### How It Works:

```
1. User calls: to_spark_dataframe(spark, datasource, options)
                      ↓
2. Adapter infers schema from datasource
                      ↓
3. Adapter plans partitions
                      ↓
4. Adapter broadcasts config (NOT objects)
                      ↓
5. mapPartitions recreates datasource on each executor
                      ↓
6. Each executor reads its partition(s)
                      ↓
7. Arrow batches → Pandas → Spark Rows
                      ↓
8. Spark combines into DataFrame
```

### Why This Approach Works:

- ✅ **No object serialization issues**: Only JSON-serializable configs are broadcast
- ✅ **Clean separation**: Core logic has no Spark dependencies
- ✅ **Testable**: Can test datasource without Spark
- ✅ **Extensible**: Easy to add new framework adapters

## Dependencies

### Core Layer:
- `pyarrow` (required for Arrow operations)
- `connectorx` (optional, for JDBC backend)
- `adbc-driver-manager` (optional, for ADBC backend)
- `pyodbc` (optional, for fallback backend)

### Adapter Layer:
- `pyspark` (required for Spark adapter)
- `polars` (required for Polars adapter - future)
- `duckdb` (required for DuckDB adapter - future)

## Testing Strategy

### Unit Tests (No Framework):
```python
# Test datasource directly
datasource = JDBCArrowDataSource()
schema = datasource.infer_schema(options)
assert len(schema) > 0

partitions = datasource.plan_partitions(options)
assert len(partitions) == expected_count

for batch in datasource.read_all(options):
    assert batch.num_rows > 0
```

### Integration Tests (With Spark):
```python
# Test Spark integration
from pysail.read.spark_adapter import to_spark_dataframe

df = to_spark_dataframe(spark, datasource, options)
assert df.count() > 0
```

## Future Extensions

### Planned Adapters:
1. **Polars Adapter** (`polars_adapter.py`)
   ```python
   def to_polars_dataframe(datasource, options) -> pl.DataFrame
   ```

2. **DuckDB Adapter** (`duckdb_adapter.py`)
   ```python
   def to_duckdb_table(datasource, options) -> duckdb.DuckDBPyConnection
   ```

3. **Pandas Adapter** (`pandas_adapter.py`)
   ```python
   def to_pandas_dataframe(datasource, options) -> pd.DataFrame
   ```

### Planned Data Sources:
1. **REST API** - Read from REST APIs with pagination
2. **S3/Cloud Storage** - Read Parquet/CSV from cloud
3. **MongoDB** - Read from document databases
4. **Elasticsearch** - Read from search engines
5. **Kafka** - Read from streaming sources

## Benefits of This Architecture

1. **No Vendor Lock-in**: Core logic independent of any framework
2. **Easy Testing**: Test without framework overhead
3. **Reusable**: Same data source works with multiple frameworks
4. **Extensible**: Add new data sources easily
5. **Maintainable**: Clear separation of concerns
6. **Performant**: Arrow-native data transfer

## Migration Path

### From Old Architecture:
```python
# OLD (mixed dependencies)
from pysail.read import read_jdbc  # Had PySpark imports at module level
```

### To New Architecture:
```python
# NEW (clean separation)
# Option 1: Pure Arrow
from pysail.read.arrow_datasource import JDBCArrowDataSource
datasource = JDBCArrowDataSource()
table = datasource.to_arrow_table(options)

# Option 2: With Spark
from pysail.read import read_jdbc  # Lazy-loads Spark only when called
df = read_jdbc(spark, ...)
```

## Conclusion

The refactored architecture provides:
- ✅ Clean separation between core and adapters
- ✅ No framework dependencies in core
- ✅ Easy to extend with new data sources
- ✅ Easy to integrate with new frameworks
- ✅ Better testability
- ✅ Better maintainability

This makes Lakesail's data source abstraction truly framework-agnostic and extensible!
