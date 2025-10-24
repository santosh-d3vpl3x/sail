# Generic Python Data Source for Lakesail

## Overview

The `sail-python-datasource` crate provides a generic bridge that allows any Python code returning Apache Arrow data to be used as a Lakesail data source. This enables users to implement custom data sources in Python while leveraging Lakesail's distributed query engine for execution.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Lakesail (Rust)                           │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ PythonDataSourceFormat (TableFormat)                  │  │
│  │  - Registered as "python" format                      │  │
│  └────────────────────┬─────────────────────────────────┘  │
│                       │                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ PythonTableProvider (TableProvider)                   │  │
│  │  - Calls Python to infer schema                       │  │
│  │  - Calls Python to plan partitions                    │  │
│  └────────────────────┬─────────────────────────────────┘  │
│                       │                                      │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ PythonExec (ExecutionPlan)                            │  │
│  │  - Executes partitions in parallel                    │  │
│  │  - Zero-copy Arrow transfer via FFI                   │  │
│  └────────────────────┬─────────────────────────────────┘  │
│                       │ PyO3 + Arrow C Data Interface       │
└───────────────────────┼──────────────────────────────────────┘
                        │
┌───────────────────────▼──────────────────────────────────────┐
│                  Python Data Source                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ User-Defined Python Class                            │  │
│  │  - infer_schema(options) -> pa.Schema                │  │
│  │  - plan_partitions(options) -> List[dict]            │  │
│  │  - read_partition(spec, options) -> Iterator[Batch]  │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Python Interface

Any Python class can serve as a data source by implementing three methods:

```python
import pyarrow as pa
from typing import Dict, List, Iterator, Any

class CustomDataSource:
    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        """
        Return the Arrow schema for the data.

        Args:
            options: User-provided options (e.g., url, table, credentials)

        Returns:
            PyArrow Schema describing the data structure
        """
        pass

    def plan_partitions(self, options: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Plan how to partition data for parallel reading.

        Args:
            options: User-provided options

        Returns:
            List of partition specifications (JSON-serializable dicts)
            Each partition spec will be passed to read_partition()
        """
        pass

    def read_partition(
        self,
        partition_spec: Dict[str, Any],
        options: Dict[str, str]
    ) -> Iterator[pa.RecordBatch]:
        """
        Read one partition and yield Arrow RecordBatches.

        Args:
            partition_spec: Specification from plan_partitions()
            options: User-provided options

        Yields:
            PyArrow RecordBatches containing the data
        """
        pass
```

## Usage

### Using the Generic Python Format

```python
# Example 1: JDBC data source
df = spark.read.format("python") \
    .option("python_module", "pysail.jdbc.datasource") \
    .option("python_class", "JDBCArrowDataSource") \
    .option("url", "jdbc:postgresql://localhost/mydb") \
    .option("dbtable", "orders") \
    .option("user", "admin") \
    .option("password", "secret") \
    .load()

# Example 2: Custom REST API data source
df = spark.read.format("python") \
    .option("python_module", "my_package.api_source") \
    .option("python_class", "RestAPIDataSource") \
    .option("endpoint", "https://api.example.com/data") \
    .option("api_key", "...") \
    .load()

# Example 3: S3 with custom logic
df = spark.read.format("python") \
    .option("python_module", "my_package.s3_source") \
    .option("python_class", "S3DataSource") \
    .option("bucket", "my-bucket") \
    .option("prefix", "data/2024/") \
    .load()
```

### Creating a Custom Data Source

```python
import pyarrow as pa
import requests
from typing import Dict, List, Iterator, Any

class RestAPIDataSource:
    """Example: Read paginated REST API data"""

    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        # Fetch sample to determine schema
        endpoint = options["endpoint"]
        response = requests.get(f"{endpoint}?limit=1").json()

        # Build schema from response
        return pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("created_at", pa.timestamp("us")),
        ])

    def plan_partitions(self, options: Dict[str, str]) -> List[Dict[str, Any]]:
        # Partition by pages
        total_pages = 10  # Get from API metadata
        return [
            {"page": i} for i in range(total_pages)
        ]

    def read_partition(
        self,
        partition_spec: Dict[str, Any],
        options: Dict[str, str]
    ) -> Iterator[pa.RecordBatch]:
        endpoint = options["endpoint"]
        page = partition_spec["page"]

        # Fetch page data
        response = requests.get(f"{endpoint}?page={page}").json()

        # Convert to Arrow and yield
        table = pa.Table.from_pylist(response["items"])
        for batch in table.to_batches():
            yield batch
```

## Implementation Details

### Zero-Copy Arrow Transfer

The implementation uses the Arrow C Data Interface for zero-copy data transfer between Python and Rust:

```rust
// In PythonExec::read_partition_from_python()
let py_batch = /* PyArrow RecordBatch from Python */;

// Zero-copy conversion via FFI
let batch = arrow::pyarrow::PyArrowType::<RecordBatch>::try_from(py_batch)?.0;
```

Both PyArrow (Python) and arrow-rs (Rust) share the same underlying Arrow memory layout, so data is transferred via pointer sharing with no copying.

### Distributed Execution

DataFusion's ExecutionPlan trait handles parallel partition execution:

1. `PythonTableProvider.scan()` calls Python `plan_partitions()`
2. `PythonExec` is created with N partitions
3. DataFusion calls `execute(partition_idx)` in parallel
4. Each call invokes Python `read_partition()` for that partition
5. Arrow batches flow back via zero-copy FFI

### Error Handling

Python exceptions are captured and converted to DataFusion errors:

```rust
Python::with_gil(|py| {
    datasource.call_method1("read_partition", (spec, options))
        .map_err(|e| PythonDataSourceError::ExecutionError(e.to_string()))
})
```

## Benefits

1. **Extensibility**: Users implement data sources in Python without touching Rust
2. **Performance**: Zero-copy Arrow FFI, parallel execution via DataFusion
3. **Ecosystem**: Leverage Python libraries (requests, boto3, pandas, etc.)
4. **Flexibility**: Works with any data source that can return Arrow data
5. **Simplicity**: Clean three-method interface

## Example Use Cases

- **Databases**: JDBC, ODBC, NoSQL databases
- **Cloud Storage**: S3, GCS, Azure Blob with custom partitioning
- **APIs**: REST APIs, GraphQL endpoints
- **File Formats**: Custom or proprietary file formats
- **Data Warehouses**: Snowflake, BigQuery, Redshift
- **Streaming**: Batch reads from Kafka, Kinesis
- **Machine Learning**: Model predictions as data source

## Testing

The implementation includes comprehensive tests:

- `python/pysail/tests/jdbc/test_python_datasource.py`: Interface demonstration
- `python/pysail/tests/jdbc/test_jdbc_backends.py`: Backend integration tests with real SQLite database
- `python/pysail/tests/jdbc/test_postgresql_support.py`: PostgreSQL URL parsing and interface validation

All tests pass with ConnectorX as the primary backend for JDBC/database access.

## JDBC Module

The JDBC functionality is organized in the `pysail.jdbc` module:

- `pysail.jdbc.datasource.JDBCArrowDataSource`: Main data source class implementing the Python interface
- `pysail.jdbc.read_jdbc()`: Convenience function for direct API usage
- `pysail.jdbc.backends`: Backend implementations (ConnectorX, ADBC, fallback)

See `python/pysail/jdbc/` for the full JDBC module.
