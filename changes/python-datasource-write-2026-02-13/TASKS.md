# Tasks: Python DataSource Write Support

## Task Dependency Graph

```
T1 (types) ──┬──→ T3 (executor trait)
              │         │
T2 (arrow) ───┤         ▼
              │    T4 (InProcessExecutor impl)
              │         │
              │         ▼
              └──→ T5 (write_exec.rs) ──→ T6 (table_format) ──→ T7 (mod.rs)
                                                                     │
                                                                     ▼
                                                               T8 (proto/codec)
                                                                     │
                                                                     ▼
                                                               T9 (tests)
                                                                     │
                                                                     ▼
                                                               T10 (cleanup)
```

---

## Checklist

### T1: Add Write Types to executor.rs
- [ ] Add `WriterPlan` struct (pickled_writer, is_arrow)
- [ ] Add `WriteResult` struct (commit_message: Option<Vec<u8>>)
- **File**: `crates/sail-data-source/src/formats/python/executor.rs`
- **Depends on**: nothing

### T2: Add rust_record_batch_to_py() to arrow_utils.rs
- [ ] Add `rust_record_batch_to_py(py, batch) -> Result<PyObject>` using `ToPyArrow`
- [ ] Add `record_batch_to_py_rows(py, batch) -> Result<Vec<PyObject>>` for Row path
- [ ] Add unit tests for both functions
- **File**: `crates/sail-data-source/src/formats/python/arrow_utils.rs`
- **Depends on**: nothing

### T3: Extend PythonExecutor Trait with Write Methods
- [ ] Add `get_writer(&self, command, schema, overwrite) -> Result<WriterPlan>`
- [ ] Add `execute_write(&self, pickled_writer, is_arrow, schema, batches) -> Result<WriteResult>`
- [ ] Add `commit_write(&self, pickled_writer, commit_messages) -> Result<()>`
- [ ] Add `abort_write(&self, pickled_writer, commit_messages) -> Result<()>`
- **File**: `crates/sail-data-source/src/formats/python/executor.rs`
- **Depends on**: T1

### T4: Implement Write Methods in InProcessExecutor
- [ ] Implement `get_writer()`: deserialize datasource, call `datasource.writer(schema, overwrite)`, isinstance check, pickle writer
- [ ] Implement `execute_write()`: spawn_blocking, deserialize writer, build Python iterator (Arrow or Row), call `writer.write(iterator)`, pickle commit message
- [ ] Implement `commit_write()`: spawn_blocking, deserialize writer + messages, call `writer.commit(messages)`
- [ ] Implement `abort_write()`: spawn_blocking, deserialize writer + messages, call `writer.abort(messages)`, log but don't propagate abort errors
- **File**: `crates/sail-data-source/src/formats/python/executor.rs`
- **Depends on**: T2, T3

### T5: Create PythonDataSourceWriteExec (write_exec.rs)
- [ ] Create `PythonDataSourceWriteExec` struct with fields: input, pickled_writer, schema, is_arrow, properties
- [ ] Implement `ExecutionPlan` trait:
  - `name()` → "PythonDataSourceWriteExec"
  - `children()` → vec![&self.input]
  - `with_new_children()` → clone with new input
  - `properties()` → single partition, empty schema, Final emission, Bounded
  - `execute(partition=0)` → orchestrate write/commit/abort
  - `execute(partition!=0)` → empty stream
- [ ] Implement `DisplayAs` trait
- [ ] Add accessor methods: `pickled_writer()`, `is_arrow()`, `input()`
- [ ] Add unit tests for construction and properties
- **File**: `crates/sail-data-source/src/formats/python/write_exec.rs` (NEW)
- **Depends on**: T3, T4

### T6: Implement create_writer() in table_format.rs
- [ ] Replace `not_impl_err!` with actual implementation
- [ ] Log warning if `partition_by` is non-empty
- [ ] Map `PhysicalSinkMode` to `overwrite: bool`
- [ ] Call `create_datasource()`, `executor.get_writer()`, construct `PythonDataSourceWriteExec`
- **File**: `crates/sail-data-source/src/formats/python/table_format.rs`
- **Depends on**: T5

### T7: Update mod.rs Exports
- [ ] Add `mod write_exec;`
- [ ] Add `pub use write_exec::PythonDataSourceWriteExec;`
- **File**: `crates/sail-data-source/src/formats/python/mod.rs`
- **Depends on**: T5

### T8: Add Proto/Codec Serialization
- [ ] Add `PythonDataSourceWriteExecNode` message to `physical.proto`
- [ ] Add encode case in `codec.rs` for `PythonDataSourceWriteExec`
- [ ] Add decode case in `codec.rs` to reconstruct `PythonDataSourceWriteExec`
- **File**: `crates/sail-execution/proto/sail/plan/physical.proto`, `crates/sail-execution/src/codec.rs`
- **Depends on**: T7

### T9: Add Python Tests
- [ ] Add `InMemoryWriter(DataSourceWriter)` test helper class (Row-based)
- [ ] Add `InMemoryArrowWriter(DataSourceArrowWriter)` test helper class (Arrow-based)
- [ ] Add `WritableDataSource(DataSource)` that returns the above writers
- [ ] `test_basic_write` — Row path, verify data arrives via commit message
- [ ] `test_arrow_write` — Arrow path, verify RecordBatch data
- [ ] `test_write_overwrite_mode` — Verify `overwrite=True` passed
- [ ] `test_write_append_mode` — Verify `overwrite=False` passed
- [ ] `test_write_commit` — Verify commit() called with all partition messages
- [ ] `test_write_abort_on_failure` — Writer.write() raises → abort() called
- [ ] `test_write_multi_partition` — Repartitioned DataFrame write
- [ ] `test_write_empty_dataframe` — Empty DataFrame write
- [ ] `test_write_no_writer_implemented` — Error when writer() not overridden
- **File**: `python/pysail/tests/spark/test_python_datasource.py`
- **Depends on**: T8

### T10: Cleanup and Verify
- [ ] Run full test suite (existing read tests + new write tests)
- [ ] Verify `cargo clippy` passes
- [ ] Verify `cargo fmt` passes
- [ ] Remove PLAN.md if it was a scratch file
- **Depends on**: T9
