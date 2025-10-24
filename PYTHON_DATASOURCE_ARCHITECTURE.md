# Generic Python Data Source Architecture

## Overview

The `sail-python-datasource` crate provides a **generic bridge** between Lakesail (Rust) and Python-based data sources. This allows users to implement data sources in Python and leverage Lakesail's distributed query engine for execution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Lakesail (Rust)                              │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ PythonDataSourceFormat (TableFormat)                      │  │
│  │  - Registered as "python" format                          │  │
│  │  - Extracts python_module and python_class from options   │  │
│  └────────────────────┬─────────────────────────────────────┘  │
│                       │ creates                                  │
│                       ▼                                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ PythonTableProvider (TableProvider)                       │  │
│  │  - Calls Python to infer schema                           │  │
│  │  - Calls Python to plan partitions                        │  │
│  └────────────────────┬─────────────────────────────────────┘  │
│                       │ creates                                  │
│                       ▼                                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ PythonExec (ExecutionPlan)                                │  │
│  │  - DataFusion calls execute(partition_idx)                │  │
│  │  - Calls Python read_partition() for each partition       │  │
│  └────────────────────┬─────────────────────────────────────┘  │
│                       │ PyO3 + Arrow FFI                         │
└───────────────────────┼──────────────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Python Data Source                           │
│                                                                   │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Any Python Class (e.g., JDBCArrowDataSource)             │  │
│  │                                                            │  │
│  │  def infer_schema(options: dict) -> pa.Schema             │  │
│  │      ↳ Return Arrow schema                                │  │
│  │                                                            │  │
│  │  def plan_partitions(options: dict) -> List[dict]         │  │
│  │      ↳ Return partition specifications                    │  │
│  │                                                            │  │
│  │  def read_partition(partition_spec: dict, options: dict)  │  │
│  │      -> Iterator[pa.RecordBatch]                          │  │
│  │      ↳ Yield Arrow batches for one partition              │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Key Design Principles

### 1. Generic, Not Domain-Specific

**Before**: Had JDBC-specific Rust code (`sail-jdbc` crate with JDBCExec, JDBCTableProvider, etc.)
- Problem: Duplicates logic (partition planning exists in both Python and Rust)
- Problem: Not extensible (need new Rust crate for each data source type)

**After**: Generic Python bridge (`sail-python-datasource`)
- Solution: Logic exists once in Python
- Solution: Users can add ANY data source in Python
- Solution: Simple Rust bridge just transfers Arrow data

### 2. Zero-Copy Data Transfer

Uses **Arrow C Data Interface** for zero-copy transfer between Python and Rust:

```rust
// In Rust (PythonExec::read_partition_from_python)
let py_batch = /* get PyArrow RecordBatch from Python */;

// Zero-copy conversion via FFI!
let batch = arrow::pyarrow::PyArrowType::<RecordBatch>::try_from(py_batch)?.0;
```

Both Python's PyArrow and Rust's arrow-rs use the same Arrow memory layout, so data is just shared via pointers - no copying!

### 3. Leverage Existing Ecosystems

- **Python side**: Use battle-tested libraries (ConnectorX, ADBC, requests, boto3, etc.)
- **Rust side**: Use DataFusion's distributed execution engine
- **Bridge**: Minimal glue code, maximum reuse

## Usage

### For Users: Using Existing Data Sources

Use the existing JDBC data source:

```python
df = spark.read.format("python") \
    .option("python_module", "pysail.read.arrow_datasource") \
    .option("python_class", "JDBCArrowDataSource") \
    .option("url", "jdbc:postgresql://localhost/mydb") \
    .option("dbtable", "users") \
    .option("user", "admin") \
    .option("password", "secret") \
    .load()
```

### For Developers: Creating New Data Sources

Create a Python class that implements the interface:

```python
import pyarrow as pa
from typing import Dict, List, Iterator, Any

class MyCustomDataSource:
    """
    Custom data source - can read from anywhere!
    (REST APIs, cloud storage, databases, files, etc.)
    """

    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        """Infer and return the Arrow schema."""
        # Example: fetch schema from API, database, or first file
        return pa.schema([
            ("id", pa.int64()),
            ("name", pa.string()),
            ("timestamp", pa.timestamp("us")),
        ])

    def plan_partitions(self, options: Dict[str, str]) -> List[Dict[str, Any]]:
        """Plan how to partition the data for parallel reading."""
        # Example: partition by date range, file list, API pages, etc.
        return [
            {"partition_id": 0, "start_date": "2024-01-01", "end_date": "2024-01-31"},
            {"partition_id": 1, "start_date": "2024-02-01", "end_date": "2024-02-29"},
            {"partition_id": 2, "start_date": "2024-03-01", "end_date": "2024-03-31"},
        ]

    def read_partition(
        self,
        partition_spec: Dict[str, Any],
        options: Dict[str, str],
    ) -> Iterator[pa.RecordBatch]:
        """Read one partition and yield Arrow batches."""
        # Example: fetch data from API, database, or files
        start_date = partition_spec["start_date"]
        end_date = partition_spec["end_date"]

        # Fetch data for this partition
        data = self._fetch_data(start_date, end_date, options)

        # Yield Arrow batches (can be multiple batches per partition)
        yield pa.record_batch(data, schema=self.infer_schema(options))

    def _fetch_data(self, start_date, end_date, options):
        # Your custom logic here!
        pass
```

Then use it:

```python
df = spark.read.format("python") \
    .option("python_module", "my_package.my_source") \
    .option("python_class", "MyCustomDataSource") \
    .option("api_key", "...") \
    .option("endpoint", "https://api.example.com/data") \
    .load()
```

## Implementation Details

### Rust Side

**`crates/sail-python-datasource/src/format.rs`**
- Implements `TableFormat` trait
- Registered as "python" format in Lakesail
- Extracts `python_module` and `python_class` from options
- Creates `PythonTableProvider`

**`crates/sail-python-datasource/src/provider.rs`**
- Implements `TableProvider` trait
- Calls Python `infer_schema()` to get schema
- Calls Python `plan_partitions()` to get partition list
- Creates `PythonExec` execution plan

**`crates/sail-python-datasource/src/exec.rs`**
- Implements `ExecutionPlan` trait
- DataFusion calls `execute(partition_idx)` for parallel execution
- Calls Python `read_partition()` with partition spec
- Converts PyArrow RecordBatches to Rust RecordBatches (zero-copy!)

### Python Side

**`python/pysail/read/arrow_datasource.py`**
- Defines `ArrowBatchDataSource` abstract base class
- Framework-agnostic (no PySpark dependencies!)
- Concrete implementations:
  - `JDBCArrowDataSource` - JDBC/database reading
  - Users can create custom implementations

## Benefits

### 1. Simplicity
- ~200 lines of generic Rust code vs 925 lines of JDBC-specific code
- No logic duplication between Python and Rust
- Single source of truth for data source behavior

### 2. Extensibility
- Add new data sources in Python (no Rust required!)
- Leverage Python ecosystem (requests, boto3, pandas, etc.)
- Distributed execution automatically via DataFusion

### 3. Performance
- Zero-copy Arrow transfer via FFI
- Parallel partition reading via DataFusion
- Efficient columnar data format

### 4. Flexibility
- Works with ANY Python code that returns Arrow data
- No framework dependencies in core (works with any Python script)
- Easy to test and debug in Python

## Examples of Possible Data Sources

With this architecture, users can easily implement:

1. **REST APIs**: Read paginated API endpoints
2. **Cloud Storage**: S3, GCS, Azure Blob (custom partitioning)
3. **Databases**: Any database with Python driver
4. **File Formats**: Custom file format parsers
5. **Data Warehouses**: Snowflake, BigQuery, Redshift
6. **Streaming**: Kafka, Kinesis (batch reads)
7. **ML Models**: Model predictions as data source
8. **Custom**: Literally anything that can return Arrow data!

## Migration from Old JDBC Implementation

### Old Way (JDBC-Specific)
```python
df = spark.read.format("jdbc") \
    .option("url", "...") \
    .option("dbtable", "users") \
    .load()
```

### New Way (Generic Python)
```python
df = spark.read.format("python") \
    .option("python_module", "pysail.read.arrow_datasource") \
    .option("python_class", "JDBCArrowDataSource") \
    .option("url", "...") \
    .option("dbtable", "users") \
    .load()
```

The old `jdbc` format is deprecated but kept for backward compatibility. All new development should use the `python` format.

## Testing

Run the test script to verify the interface:

```bash
python python/test_python_datasource.py
```

This will:
1. Verify JDBCArrowDataSource implements the required interface
2. Show example usage
3. Document the interface for custom data sources

## Future Enhancements

1. **Write Support**: Implement `write_partition()` method
2. **Predicate Pushdown**: Pass filters to Python for optimization
3. **Schema Evolution**: Handle schema changes dynamically
4. **Caching**: Cache schema and partition info
5. **Metrics**: Expose Python-side metrics to Rust
