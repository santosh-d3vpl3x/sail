# Python DataSource Write Support — Implementation Plan

## Context: How Read Was Added (PR #1291 + follow-ups)

Commit `0e4aa6d` added the Python DataSource API for **read** operations (4,145 lines across 34 files), with two follow-up commits:
- `95f64ad` — switched to `pyspark.cloudpickle` and used pickle for reader
- `987ead6` — moved data source discovery script and refactored module structure

### Read Pipeline (for reference)

```
spark.read.format("custom").load()
  → PythonTableFormat::create_provider()
    → PythonDataSource (pickled instance)
    → PythonTableProvider (with InProcessExecutor)
      → executor.get_partitions()
        → Python: datasource.reader(schema).partitions()
        → Python: reader.pushFilters(filters)
        → Pickles reader + partitions
      → PythonDataSourceExec (ExecutionPlan, source node)
        → executor.execute_read(pickled_reader, partition, schema)
          → PythonDataSourceStream (spawn_blocking thread)
            → Python: reader.read(partition) → yields RecordBatch/tuple
```

Key files:
- `table_format.rs` — `TableFormat` impl, entry point
- `executor.rs` — `PythonExecutor` trait + `InProcessExecutor`
- `exec.rs` — `PythonDataSourceExec` (DataFusion `ExecutionPlan`)
- `stream.rs` — `PythonDataSourceStream` (RecordBatch streaming)
- `python_datasource.rs` — `PythonDataSource` wrapper
- `python_table_provider.rs` — DataFusion `TableProvider` impl

---

## PySpark DataSourceWriter API (Target)

The PySpark 4.x API we need to support:

```python
class DataSource:
    def writer(self, schema: StructType, overwrite: bool) -> DataSourceWriter:
        """Returns a writer instance. Called on driver."""

class DataSourceWriter(ABC):
    @abstractmethod
    def write(self, iterator: Iterator[Row]) -> WriterCommitMessage:
        """Called once per executor/partition. Receives data, returns commit msg."""

    def commit(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        """Called on driver when ALL tasks succeed. Receives all commit messages."""

    def abort(self, messages: List[Optional[WriterCommitMessage]]) -> None:
        """Called on driver when ANY task fails. Receives collected commit messages."""

class WriterCommitMessage:
    """Must be picklable. Returned by write(), passed to commit/abort."""
```

User-facing API:
```python
df.write.format("my_custom_source").mode("append").save()
```

---

## Implementation Design

### Write Pipeline (mirror of read)

```
df.write.format("custom").mode("append").save()
  → PythonTableFormat::create_writer(ctx, SinkInfo)    [IMPLEMENT]
    → PythonDataSource (reuse existing)
    → Get writer from Python: datasource.writer(schema, overwrite)
    → Pickle the writer instance
    → PythonDataSourceWriteExec (ExecutionPlan, sink node)  [NEW]
      → execute(partition, context)
        → Execute child input plan to get RecordBatch stream
        → PythonDataSourceWriteSink (spawn_blocking thread)  [NEW]
          → Python: writer.write(row_iterator) → WriterCommitMessage
          → Pickle commit message, return as RecordBatch
      → After all partitions complete:
        → Collect commit messages on driver
        → Python: writer.commit(messages) or writer.abort(messages)
```

### New Files

| File | Purpose |
|------|---------|
| `write_exec.rs` | `PythonDataSourceWriteExec` — DataFusion ExecutionPlan for write |
| `write_sink.rs` | `PythonDataSourceWriteSink` — Sends RecordBatches to Python writer in blocking thread |

### Modified Files

| File | Change |
|------|--------|
| `table_format.rs` | Implement `create_writer()` |
| `executor.rs` | Add `get_writer()` and `execute_write()` methods to `PythonExecutor` trait |
| `mod.rs` | Export new modules |
| `error.rs` | Add write-specific error context (e.g., "write", "commit", "abort" operations) |
| `physical.proto` | Add `PythonDataSourceWriteExecNode` message |
| `codec.rs` | Encode/decode for `PythonDataSourceWriteExec` |
| `arrow_utils.rs` | Add `rust_record_batch_to_py()` for Rust→Python batch conversion |
| Test file | Add write tests mirroring read tests |

---

## Detailed Component Design

### 1. `table_format.rs` — `create_writer()` Implementation

```rust
async fn create_writer(
    &self,
    _ctx: &dyn Session,
    info: SinkInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    let SinkInfo { input, path, mode, options, .. } = info;

    // Reuse existing: create PythonDataSource from options
    let datasource = self.create_datasource(&options)?;
    let schema = input.schema();

    // Determine overwrite flag from mode
    let overwrite = matches!(mode, PhysicalSinkMode::Overwrite);

    // Call Python: datasource.writer(schema, overwrite)
    // Pickle the writer instance → pickled_writer bytes
    let executor: Arc<dyn PythonExecutor> = Arc::new(InProcessExecutor::new());
    let writer_plan = executor.get_writer(
        datasource.command(),
        &schema,
        overwrite,
    ).await?;

    // Create write execution plan
    Ok(Arc::new(PythonDataSourceWriteExec::new(
        input,
        writer_plan.pickled_writer,
        writer_plan.pickled_datasource,
        schema,
        overwrite,
    )))
}
```

### 2. `executor.rs` — New Trait Methods

```rust
/// Result of writer planning.
#[derive(Debug, Clone)]
pub struct WriterPlan {
    /// Pickled Python DataSourceWriter instance
    pub pickled_writer: Vec<u8>,
    /// Pickled Python DataSource instance (for commit/abort on driver)
    pub pickled_datasource: Vec<u8>,
}

#[async_trait]
pub trait PythonExecutor: Send + Sync + std::fmt::Debug {
    // ... existing methods ...

    /// Get a writer for the Python datasource.
    ///
    /// Calls Python `DataSource.writer(schema, overwrite)`.
    /// Returns a WriterPlan with the pickled writer.
    async fn get_writer(
        &self,
        command: &[u8],
        schema: &SchemaRef,
        overwrite: bool,
    ) -> Result<WriterPlan>;

    /// Execute a write for a specific partition.
    ///
    /// Takes a pickled writer and a stream of RecordBatches.
    /// Calls Python writer.write(iterator) and returns the pickled
    /// WriterCommitMessage.
    async fn execute_write(
        &self,
        pickled_writer: &[u8],
        input_stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Result<Option<Vec<u8>>>; // Returns pickled WriterCommitMessage or None

    /// Commit the write operation.
    ///
    /// Calls Python writer.commit(messages) on the driver.
    async fn commit_write(
        &self,
        pickled_writer: &[u8],
        commit_messages: Vec<Option<Vec<u8>>>,
    ) -> Result<()>;

    /// Abort the write operation.
    ///
    /// Calls Python writer.abort(messages) on the driver.
    async fn abort_write(
        &self,
        pickled_writer: &[u8],
        commit_messages: Vec<Option<Vec<u8>>>,
    ) -> Result<()>;
}
```

### 3. `write_exec.rs` — `PythonDataSourceWriteExec`

This is a **sink node** (has one child: the input data plan). Following the Delta pattern:

```rust
pub struct PythonDataSourceWriteExec {
    /// Input execution plan (data to write)
    input: Arc<dyn ExecutionPlan>,
    /// Pickled Python DataSourceWriter instance
    pickled_writer: Vec<u8>,
    /// Pickled Python DataSource instance (for commit/abort)
    pickled_datasource: Vec<u8>,
    /// Schema of the data being written
    schema: SchemaRef,
    /// Whether this is an overwrite operation
    overwrite: bool,
    /// Execution plan properties
    properties: PlanProperties,
}
```

The `execute()` method:
1. Executes the child input plan to get a `SendableRecordBatchStream`
2. Creates an `InProcessExecutor` lazily
3. Calls `executor.execute_write()` which:
   - Spawns a blocking thread
   - Converts RecordBatches to Python Row iterator
   - Calls `writer.write(iterator)`
   - Returns pickled `WriterCommitMessage`
4. Returns a single-row RecordBatch with the pickled commit message

After all partitions complete, a **commit/abort** step collects messages and calls the appropriate Python method. This can be modeled as a separate `PythonDataSourceCommitExec` (like Delta's `DeltaCommitExec`) or handled inline.

### 4. `write_sink.rs` — `PythonDataSourceWriteSink`

Analogous to `PythonDataSourceStream` but in reverse:

```rust
pub struct PythonDataSourceWriteSink;

impl PythonDataSourceWriteSink {
    /// Run the Python writer in a dedicated blocking thread.
    ///
    /// Receives RecordBatches via channel, converts to Python Row iterator,
    /// calls writer.write(iterator), returns WriterCommitMessage.
    fn run_python_writer(
        pickled_writer: Vec<u8>,
        rx: mpsc::Receiver<RecordBatch>,
        schema: SchemaRef,
        tx_result: oneshot::Sender<Result<Option<Vec<u8>>>>,
    ) {
        // spawn_blocking + Python::attach
        // 1. Deserialize writer
        // 2. Create a Python iterator that pulls from rx channel
        // 3. Call writer.write(iterator)
        // 4. Pickle the WriterCommitMessage result
        // 5. Send back via tx_result
    }
}
```

**Data flow (Rust → Python)**:
- **Arrow path**: Convert `RecordBatch` → `pyarrow.RecordBatch` via Arrow C Data Interface (reverse of read)
- **Row path**: Convert `RecordBatch` rows → Python `Row` objects (PySpark Row type)

The PySpark API specifies `Iterator[Row]` as input to `write()`. We need to convert Arrow RecordBatches to PySpark Row objects. Two approaches:
1. **Direct**: Convert RecordBatch → PyArrow → PySpark Row (per-row overhead)
2. **Batch**: Pass RecordBatches to a Python adapter that yields Rows (more efficient)

**Recommended**: Create a small Python helper class that wraps a RecordBatch iterator and yields Row objects:

```python
class RecordBatchRowIterator:
    def __init__(self, batches):
        self._batches = batches
    def __iter__(self):
        for batch in self._batches:
            for row in batch.to_pydict():  # or batch.to_pandas().itertuples()
                yield Row(**row_dict)
```

### 5. `arrow_utils.rs` — Rust→Python Conversion

Add the reverse conversion (currently only Python→Rust exists):

```rust
/// Convert a Rust RecordBatch to a Python pyarrow.RecordBatch.
///
/// Uses Arrow C Data Interface for zero-copy transfer.
pub fn rust_record_batch_to_py<'py>(
    py: Python<'py>,
    batch: &RecordBatch,
) -> Result<Bound<'py, PyAny>> {
    // Export via Arrow C FFI
    // Import into pyarrow
}
```

### 6. `physical.proto` — New Message

```protobuf
message PythonDataSourceWriteExecNode {
    bytes input = 1;
    bytes pickled_writer = 2;
    bytes pickled_datasource = 3;
    bytes schema = 4;
    bool overwrite = 5;
}
```

### 7. Commit/Abort Orchestration

Two design options:

**Option A: Two-phase execution plan (like Delta)**
```
PythonDataSourceWriteExec (per-partition writer)
    → CoalescePartitionsExec (gather commit messages)
        → PythonDataSourceCommitExec (calls commit/abort)
```

**Option B: Single execution plan with internal commit**
- `PythonDataSourceWriteExec` handles both writing and commit/abort internally
- Simpler but less composable

**Recommendation: Option A** — follows the Delta pattern and separates concerns cleanly.

---

## Test Plan

Mirror the read tests in `test_python_datasource.py`:

1. **Basic write** — `df.write.format("custom").save()`
2. **Overwrite mode** — `df.write.format("custom").mode("overwrite").save()`
3. **Commit message** — Verify commit messages are collected and passed
4. **Abort on failure** — Verify abort is called when a task fails
5. **Round-trip** — Write then read back, verify data integrity
6. **Schema validation** — Write with mismatched schema
7. **Multi-partition write** — Verify parallel write across partitions
8. **Empty write** — Write empty DataFrame
9. **Error propagation** — Python exceptions in write/commit/abort

---

## Implementation Order

1. **`arrow_utils.rs`** — Add `rust_record_batch_to_py()` (needed by write sink)
2. **`executor.rs`** — Add `get_writer()`, `execute_write()`, `commit_write()`, `abort_write()` to trait + `InProcessExecutor`
3. **`write_sink.rs`** — Implement Python writer thread (data flow Rust→Python)
4. **`write_exec.rs`** — Implement `PythonDataSourceWriteExec` execution plan
5. **`table_format.rs`** — Implement `create_writer()` entry point
6. **`mod.rs`** — Export new modules
7. **`physical.proto` + `codec.rs`** — Serialization support
8. **`error.rs`** — Write-specific error contexts
9. **Tests** — Comprehensive write test suite
