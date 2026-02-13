# Design: Python DataSource Write Support

## Architecture Overview

```
df.write.format("my_source").mode("append").save()
    │
    ▼
PythonTableFormat::create_writer(ctx, SinkInfo)
    │
    ├── create_datasource(options)       ← reuse existing
    ├── executor.get_writer(command, schema, overwrite)
    │       │
    │       ▼ Python: datasource.writer(schema, overwrite)
    │       ▼ isinstance check: DataSourceArrowWriter vs DataSourceWriter
    │       ▼ pickle writer → WriterPlan { pickled_writer, is_arrow }
    │
    └── return PythonDataSourceWriteExec(input, pickled_writer, schema, is_arrow)
            │
            ▼
    PythonDataSourceWriteExec::execute(partition, ctx)
            │
            ├── Run input plan → stream of RecordBatch
            ├── spawn_blocking → Python thread
            │       │
            │       ▼ deserialize writer
            │       ▼ if is_arrow:
            │       │   writer.write(Iterator[RecordBatch])  ← zero-copy
            │       ▼ else:
            │       │   writer.write(Iterator[Row])          ← convert batches to rows
            │       ▼ return WriterCommitMessage (pickled)
            │
            └── After ALL partitions complete:
                    ├── success: writer.commit([msg0, msg1, ...])
                    └── failure: writer.abort([msg0, None, ...])
```

## New Types

### WriterPlan (executor.rs)

```rust
/// Result of writer planning, containing the pickled writer and metadata.
#[derive(Debug, Clone)]
pub struct WriterPlan {
    /// Pickled Python DataSourceWriter instance
    pub pickled_writer: Vec<u8>,
    /// Whether writer is a DataSourceArrowWriter (true) or DataSourceWriter (false)
    pub is_arrow: bool,
}
```

### WriteResult (executor.rs)

```rust
/// Result of a single partition write.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// Pickled WriterCommitMessage (or empty if None)
    pub commit_message: Option<Vec<u8>>,
}
```

## Modified Files

### 1. executor.rs — Add Write Methods to PythonExecutor Trait

```rust
#[async_trait]
pub trait PythonExecutor: Send + Sync + std::fmt::Debug {
    // ... existing read methods ...

    /// Get a writer from the Python datasource.
    ///
    /// Calls datasource.writer(schema, overwrite), checks isinstance,
    /// pickles the writer, and returns WriterPlan.
    async fn get_writer(
        &self,
        command: &[u8],
        schema: &SchemaRef,
        overwrite: bool,
    ) -> Result<WriterPlan>;

    /// Execute a write for a single partition.
    ///
    /// Deserializes the writer, feeds RecordBatches (converting to Rows if needed),
    /// calls writer.write(iterator), and returns the pickled commit message.
    async fn execute_write(
        &self,
        pickled_writer: &[u8],
        is_arrow: bool,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<WriteResult>;

    /// Commit a successful write.
    ///
    /// Calls writer.commit(messages) on the driver.
    async fn commit_write(
        &self,
        pickled_writer: &[u8],
        commit_messages: Vec<Option<Vec<u8>>>,
    ) -> Result<()>;

    /// Abort a failed write.
    ///
    /// Calls writer.abort(messages) on the driver.
    async fn abort_write(
        &self,
        pickled_writer: &[u8],
        commit_messages: Vec<Option<Vec<u8>>>,
    ) -> Result<()>;
}
```

### 2. table_format.rs — Implement create_writer()

Replace `not_impl_err!` with:

```rust
async fn create_writer(
    &self,
    _ctx: &dyn Session,
    info: SinkInfo,
) -> Result<Arc<dyn ExecutionPlan>> {
    let SinkInfo {
        input,
        mode,
        partition_by,
        options,
        ..
    } = info;

    // Warn about unsupported partitionBy (PySpark compat: silently ignored)
    if !partition_by.is_empty() {
        log::warn!(
            "partitionBy is not supported for Python datasource '{}' and will be ignored. \
             Handle partitioning in your DataSourceWriter.write() method.",
            self.name
        );
    }

    // Map save mode to overwrite bool (PySpark convention)
    let overwrite = matches!(mode, PhysicalSinkMode::Overwrite);

    // Create datasource and get writer
    let datasource = self.create_datasource(&options)?;
    let executor: Arc<dyn PythonExecutor> = Arc::new(InProcessExecutor::new());
    let schema = input.schema();

    let writer_plan = executor
        .get_writer(datasource.command(), &schema, overwrite)
        .await?;

    Ok(Arc::new(PythonDataSourceWriteExec::new(
        input,
        writer_plan.pickled_writer,
        schema,
        writer_plan.is_arrow,
    )))
}
```

### 3. arrow_utils.rs — Add Rust RecordBatch to Python Conversion

```rust
/// Convert a Rust Arrow RecordBatch to a Python PyArrow RecordBatch.
///
/// Uses Arrow C Data Interface for zero-copy transfer.
pub fn rust_record_batch_to_py(
    py: Python<'_>,
    batch: &RecordBatch,
) -> Result<PyObject> {
    use arrow_pyarrow::ToPyArrow;

    batch.to_pyarrow(py).map_err(|e| {
        DataFusionError::External(Box::new(std::io::Error::other(format!(
            "Failed to convert RecordBatch to PyArrow: {}", e
        ))))
    })
}

/// Convert a Rust Arrow RecordBatch to a list of PySpark Row objects.
///
/// Used for DataSourceWriter (non-Arrow) path.
pub fn record_batch_to_py_rows(
    py: Python<'_>,
    batch: &RecordBatch,
) -> Result<Vec<PyObject>> {
    // Convert batch to PyArrow, then use .to_pylist() to get dicts,
    // then wrap each dict as a PySpark Row
    let pa_batch = rust_record_batch_to_py(py, batch)?;
    let py_list = pa_batch.call_method0(py, "to_pylist")?;
    let row_class = py.import("pyspark.sql")?.getattr("Row")?;

    // Convert each dict to a Row
    let rows: Vec<PyObject> = ...;  // iterate py_list, call Row(**dict)
    Ok(rows)
}
```

### 4. mod.rs — Add New Exports

```rust
mod write_exec;
pub use write_exec::PythonDataSourceWriteExec;
```

### 5. error.rs — Add Write Error Contexts

Add `"write"`, `"commit"`, `"abort"` as valid operation strings in `PythonDataSourceContext`. No structural changes needed — the existing context system supports these as string literals.

## New Files

### write_exec.rs — PythonDataSourceWriteExec

This is the core new file. It implements `ExecutionPlan` as a sink node:

```rust
/// Execution plan for writing to a Python datasource.
///
/// This is a sink node (one child: the input data plan) that:
/// 1. Executes the input plan to get RecordBatch streams
/// 2. Feeds batches to Python writer.write() per partition
/// 3. Collects WriterCommitMessages from all partitions
/// 4. Calls writer.commit() or writer.abort() on the driver
#[derive(Debug)]
pub struct PythonDataSourceWriteExec {
    /// Input execution plan (data to write)
    input: Arc<dyn ExecutionPlan>,
    /// Pickled Python DataSourceWriter instance
    pickled_writer: Vec<u8>,
    /// Schema of the data being written
    schema: SchemaRef,
    /// Whether writer is DataSourceArrowWriter
    is_arrow: bool,
    /// Execution plan properties
    properties: PlanProperties,
}
```

**Key design decisions:**

1. **Single ExecutionPlan** — Unlike Delta's two-node approach (WriteExec → CommitExec), we use a single node that handles both write and commit/abort. This is simpler and sufficient for the Python datasource case where there's no complex transaction protocol.

2. **Collect-then-commit** — The `execute()` method for partition 0 runs ALL partitions, collects all commit messages, then calls commit/abort. Other partitions return empty streams. This matches how PySpark orchestrates it.

3. **Empty schema output** — The write exec returns an empty schema (no rows), consistent with how write operations work in DataFusion.

**execute() flow:**

```
fn execute(&self, partition: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
    if partition != 0 {
        return Ok(empty_stream());
    }

    // Create async stream that orchestrates the write
    let stream = async move {
        let num_partitions = input.output_partitioning().partition_count();
        let executor = InProcessExecutor::new();

        // Phase 1: Execute writes in parallel
        let mut handles = Vec::new();
        for p in 0..num_partitions {
            let input_stream = input.execute(p, ctx.clone())?;
            // Collect all batches from this partition
            let batches = collect_batches(input_stream).await?;
            // Execute write
            handles.push(executor.execute_write(
                &pickled_writer, is_arrow, schema.clone(), batches
            ));
        }

        // Phase 2: Collect results
        let results = futures::future::join_all(handles).await;
        let mut commit_messages = Vec::new();
        let mut had_failure = false;
        for result in results {
            match result {
                Ok(write_result) => commit_messages.push(write_result.commit_message),
                Err(_) => { had_failure = true; commit_messages.push(None); }
            }
        }

        // Phase 3: Commit or abort
        if had_failure {
            executor.abort_write(&pickled_writer, commit_messages).await?;
            return Err(...);
        } else {
            executor.commit_write(&pickled_writer, commit_messages).await?;
        }

        Ok(empty_batch())
    };

    Ok(Box::pin(RecordBatchStreamAdapter::new(empty_schema, stream)))
}
```

## Data Flow: Row vs Arrow Path

### Arrow Path (DataSourceArrowWriter)

```
Rust RecordBatch
    │
    ▼ rust_record_batch_to_py() — Arrow C Data Interface, zero-copy
    │
    ▼ Python pyarrow.RecordBatch
    │
    ▼ Yielded via Python iterator to writer.write()
```

### Row Path (DataSourceWriter)

```
Rust RecordBatch
    │
    ▼ rust_record_batch_to_py() — Arrow C Data Interface
    │
    ▼ Python pyarrow.RecordBatch
    │
    ▼ batch.to_pylist() → list of dicts
    │
    ▼ Row(**dict) for each dict → PySpark Row objects
    │
    ▼ Yielded via Python iterator to writer.write()
```

## Save Mode Mapping

| PySpark Mode | PhysicalSinkMode | `overwrite` param |
|---|---|---|
| `"append"` | `Append` | `False` |
| `"overwrite"` | `Overwrite` | `True` |
| `"error"` / `"errorifexists"` | `ErrorIfExists` | `False` |
| `"ignore"` | `IgnoreIfExists` | `False` |

Note: `ErrorIfExists` and `IgnoreIfExists` are handled by Spark/Sail BEFORE reaching the Python writer. The writer only sees `overwrite=True/False`.

## Serialization (Proto/Codec)

Add `PythonDataSourceWriteExecNode` to `physical.proto`:

```protobuf
message PythonDataSourceWriteExecNode {
    bytes pickled_writer = 1;
    sail.common.Schema schema = 2;
    bool is_arrow = 3;
}
```

And corresponding encode/decode in `codec.rs`.

## Testing Strategy

Tests go in `python/pysail/tests/spark/test_python_datasource.py`:

1. **test_basic_write** — Row-based writer, verify data arrives
2. **test_arrow_write** — Arrow-based writer, verify RecordBatch path
3. **test_write_overwrite_mode** — Check `overwrite=True` is passed
4. **test_write_append_mode** — Check `overwrite=False` is passed
5. **test_write_commit** — Verify commit() called with all messages
6. **test_write_abort** — Verify abort() called on failure
7. **test_write_multi_partition** — Multi-partition write
8. **test_write_empty_df** — Empty DataFrame write
9. **test_write_no_writer** — Error when writer() not implemented
10. **test_write_unpicklable_writer** — Error on pickle failure
