# The Correct JDBC Architecture (User's Insight)

## The User's Question

> Why can't we pass arrow from python to rust side? Why do we actually need to implement the rust JDBC specific implementations? I would expect rust just to be used for already existing logic in lakesail and distributed processing while python to provide pluggable interface.

**Answer**: You're absolutely right! We DON'T need JDBC-specific Rust code.

## The Correct Design

### What Rust Should Do (Generic)
```rust
// Generic Python data source format
impl TableFormat for PythonDataSourceFormat {
    async fn create_provider(...) -> Arc<dyn TableProvider> {
        // 1. Call Python function
        // 2. Receive Arrow RecordBatches
        // 3. Pass to DataFusion
        // That's it!
    }
}
```

### What Python Should Do (Pluggable)
```python
# User implements this interface
class DataSourcePlugin:
    def infer_schema(self, options: dict) -> pa.Schema:
        """Return Arrow schema"""

    def plan_partitions(self, options: dict) -> List[dict]:
        """Return partition specs"""

    def read_partition(self, partition_spec: dict, options: dict) -> Iterator[pa.RecordBatch]:
        """Return Arrow batches"""

# JDBC is just ONE implementation
class JDBCArrowDataSource(DataSourcePlugin):
    # We already have this!
    pass

# Users can add their own!
class S3DataSource(DataSourcePlugin):
    pass

class RESTAPIDataSource(DataSourcePlugin):
    pass
```

## How Arrow Passes Between Python and Rust

### The Arrow C Data Interface

Both PyArrow and arrow-rs implement the same C ABI:

```c
// Arrow C Data Interface (standard)
struct ArrowArray {
    int64_t length;
    int64_t null_count;
    const void* buffers[];
    // ...
};

struct ArrowSchema {
    const char* format;
    const char* name;
    // ...
};
```

**Zero-copy transfer**:
1. Python creates Arrow in memory
2. Exports pointers via C interface
3. Rust imports same pointers
4. No copying, same memory!

### Using PyO3 + Arrow FFI

```rust
use pyo3::prelude::*;
use arrow::ffi;
use arrow::array::RecordBatch;

fn read_from_python(py: Python) -> PyResult<RecordBatch> {
    // Import Python module
    let pysail = py.import("pysail.read.arrow_datasource")?;
    let datasource = pysail.getattr("JDBCArrowDataSource")?.call0()?;

    // Call Python method - returns PyArrow RecordBatch
    let py_batch = datasource.call_method1("read_partition", (spec, opts))?;

    // Convert PyArrow → Rust Arrow (zero-copy!)
    let batch = arrow::pyarrow::PyArrowType::<RecordBatch>::try_from(py_batch)?.0;

    Ok(batch)
}
```

**That's it!** The Arrow data is already in the right format.

## Simplified Implementation

### 1. Generic Python Bridge (Rust)

```rust
// crates/sail-python-datasource/src/lib.rs

pub struct PythonDataSourceFormat;

#[async_trait]
impl TableFormat for PythonDataSourceFormat {
    fn name(&self) -> &str {
        "python"
    }

    async fn create_provider(&self, ctx: &dyn Session, info: SourceInfo) -> Result<Arc<dyn TableProvider>> {
        let options = merge_options(info.options);

        // Get Python module and class from options
        let module = options.get("python_module").ok_or(...)?;
        let class = options.get("python_class").ok_or(...)?;

        Ok(Arc::new(PythonTableProvider::new(module, class, options)))
    }
}

pub struct PythonTableProvider {
    module: String,
    class: String,
    options: HashMap<String, String>,
    schema: SchemaRef,
}

impl PythonTableProvider {
    fn new(module: &str, class: &str, options: HashMap<String, String>) -> Self {
        // Call Python to infer schema
        let schema = Python::with_gil(|py| {
            let py_module = py.import(module)?;
            let py_class = py_module.getattr(class)?;
            let datasource = py_class.call0()?;

            let opts_dict = to_py_dict(py, &options)?;
            let py_schema = datasource.call_method1("infer_schema", (opts_dict,))?;

            // Convert PyArrow Schema → Rust Arrow Schema (zero-copy!)
            let schema = arrow::pyarrow::PyArrowType::<Schema>::try_from(py_schema)?.0;
            Ok(Arc::new(schema))
        })?;

        Self { module, class, options, schema }
    }
}

#[async_trait]
impl TableProvider for PythonTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(&self, ...) -> Result<Arc<dyn ExecutionPlan>> {
        // Get partitions from Python
        let partitions = Python::with_gil(|py| {
            let py_module = py.import(&self.module)?;
            let py_class = py_module.getattr(&self.class)?;
            let datasource = py_class.call0()?;

            let opts_dict = to_py_dict(py, &self.options)?;
            let py_partitions = datasource.call_method1("plan_partitions", (opts_dict,))?;

            // Convert to Vec<HashMap>
            py_partitions.extract::<Vec<HashMap<String, String>>>()
        })?;

        Ok(Arc::new(PythonExec::new(
            self.module.clone(),
            self.class.clone(),
            self.schema.clone(),
            partitions,
            self.options.clone(),
        )))
    }
}

pub struct PythonExec {
    module: String,
    class: String,
    schema: SchemaRef,
    partitions: Vec<HashMap<String, String>>,
    options: HashMap<String, String>,
}

impl ExecutionPlan for PythonExec {
    fn execute(&self, partition: usize, _context: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        let partition_spec = &self.partitions[partition];

        // Call Python to read this partition
        let batches = Python::with_gil(|py| {
            let py_module = py.import(&self.module)?;
            let py_class = py_module.getattr(&self.class)?;
            let datasource = py_class.call0()?;

            let spec_dict = to_py_dict(py, partition_spec)?;
            let opts_dict = to_py_dict(py, &self.options)?;

            let py_batches = datasource.call_method1("read_partition", (spec_dict, opts_dict))?;

            // Iterate over PyArrow batches, convert to Rust
            let mut batches = Vec::new();
            for py_batch in py_batches.iter()? {
                let batch = arrow::pyarrow::PyArrowType::<RecordBatch>::try_from(py_batch?)?.0;
                batches.push(batch);
            }

            Ok(batches)
        })?;

        // Create stream from batches
        Ok(Box::pin(MemoryStream::try_new(batches, self.schema.clone(), None)?))
    }
}
```

### 2. Usage (Python User Code)

```python
# JDBC example
spark.read.format("python")
  .option("python_module", "pysail.read.arrow_datasource")
  .option("python_class", "JDBCArrowDataSource")
  .option("url", "jdbc:postgresql://...")
  .option("dbtable", "orders")
  .option("partitionColumn", "id")
  .option("lowerBound", "0")
  .option("upperBound", "1000000")
  .option("numPartitions", "10")
  .load()

# S3 example (if user implements S3DataSource)
spark.read.format("python")
  .option("python_module", "my_package.s3_source")
  .option("python_class", "S3DataSource")
  .option("bucket", "my-bucket")
  .option("prefix", "data/")
  .load()

# REST API example
spark.read.format("python")
  .option("python_module", "my_package.rest_source")
  .option("python_class", "RESTAPIDataSource")
  .option("endpoint", "https://api.example.com/data")
  .load()
```

### 3. Even Simpler: Direct Registration

```python
# Python side
from pysail.read import JDBCArrowDataSource

# Register directly with Lakesail
server.register_datasource(
    name="jdbc",
    datasource=JDBCArrowDataSource()
)

# Usage
spark.read.format("jdbc")  # Calls registered Python datasource!
  .option("url", "...")
  .load()
```

## Comparison

### My Overcomplicated Approach ❌
- **Rust JDBC code**: 925 lines
- **Python bridge**: Complex PyO3 for every method
- **Duplication**: Partition logic in both languages
- **Rigid**: Need new Rust code for each data source

### Your Proposed Approach ✅
- **Rust code**: ~200 lines (generic Python bridge)
- **Python bridge**: Simple Arrow FFI (already exists!)
- **No duplication**: All logic in Python
- **Pluggable**: Users add data sources in Python!

## Benefits

### 1. Separation of Concerns
- **Rust**: Distribution, execution, optimization (Lakesail's strength)
- **Python**: Domain logic, data sources, integrations (Python's strength)

### 2. Pluggable Architecture
```python
# User writes:
class MyCustomDataSource:
    def infer_schema(self, options): ...
    def plan_partitions(self, options): ...
    def read_partition(self, spec, options): ...

# Register:
server.register_datasource("custom", MyCustomDataSource())

# Use:
spark.read.format("custom").option(...).load()
```

### 3. Zero-Copy Performance
- Arrow C Data Interface: Zero-copy between Python and Rust
- Same memory, different language views
- No serialization overhead

### 4. Leverage Existing Ecosystems
- Python: JDBC (ConnectorX, ADBC), S3 (boto3), REST (requests)
- Rust: Just handles distribution
- Best of both worlds!

## What We Should Actually Build

### Phase 1: Generic Python Bridge (Minimal)
1. Create `PythonDataSourceFormat` in Rust
2. Calls Python code that implements interface
3. Receives Arrow via FFI
4. ~200 lines of Rust code

### Phase 2: Register Existing Python Code
1. Use existing `JDBCArrowDataSource`
2. No changes needed!
3. Just register it with Lakesail

### Phase 3: Documentation
1. Document the interface
2. Show examples (JDBC, S3, REST)
3. Let users extend!

## Conclusion

You asked the right question:
> "Why can't we pass arrow from python to rust side?"

**Answer**: We CAN and SHOULD! The Arrow C Data Interface makes this trivial.

The architecture should be:
1. **Rust**: Generic Python data source executor
2. **Python**: Pluggable data source implementations
3. **Bridge**: Arrow FFI (already exists!)

This is:
- Simpler (~200 lines vs 925 lines)
- More flexible (users add sources)
- Better separation of concerns
- Zero-copy performance

Let me rebuild this the right way!
