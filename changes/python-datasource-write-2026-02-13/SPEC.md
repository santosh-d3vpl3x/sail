# Spec: Python DataSource Write Support

## Overview

Add write support for Python DataSources in Sail, fully compatible with PySpark 4.1's `DataSourceWriter` and `DataSourceArrowWriter` APIs.

---

## Scenarios

### S1: Basic Row-Based Write (DataSourceWriter)

**Given** a Python DataSource that returns a `DataSourceWriter` from `writer(schema, overwrite)`
**When** the user calls `df.write.format("my_source").mode("append").save()`
**Then** Sail calls `writer.write(iterator)` on each partition with `Iterator[Row]`
**And** collects `WriterCommitMessage` from each partition
**And** calls `writer.commit(messages)` on the driver

### S2: Arrow-Based Write (DataSourceArrowWriter)

**Given** a Python DataSource that returns a `DataSourceArrowWriter` from `writer(schema, overwrite)`
**When** the user calls `df.write.format("my_source").mode("append").save()`
**Then** Sail calls `writer.write(iterator)` on each partition with `Iterator[RecordBatch]`
**And** uses Arrow C Data Interface for zero-copy transfer (same as read path)
**And** collects `WriterCommitMessage` and calls `writer.commit(messages)`

### S3: Overwrite Mode

**Given** a Python DataSource with a writer
**When** the user calls `df.write.format("my_source").mode("overwrite").save()`
**Then** `DataSource.writer(schema, overwrite=True)` is called
**And** the write proceeds as in S1/S2

### S4: Append Mode

**Given** a Python DataSource with a writer
**When** the user calls `df.write.format("my_source").mode("append").save()`
**Then** `DataSource.writer(schema, overwrite=False)` is called

### S5: Error/ErrorIfExists Mode

**Given** a Python DataSource with a writer
**When** the user calls `df.write.format("my_source").mode("error").save()`
**Then** `DataSource.writer(schema, overwrite=False)` is called
**And** the error-if-exists check is handled by Spark/Sail before reaching the Python writer
(Note: PySpark maps "error" mode to `overwrite=False` — the Python writer does not distinguish between "append" and "error" modes)

### S6: Ignore Mode

**Given** a Python DataSource with a writer
**When** the user calls `df.write.format("my_source").mode("ignore").save()`
**Then** `DataSource.writer(schema, overwrite=False)` is called
**And** the ignore-if-exists check is handled by Spark/Sail before reaching the Python writer

### S7: Commit on Success

**Given** a write operation where all partition tasks succeed
**When** all `writer.write(iterator)` calls return `WriterCommitMessage` objects
**Then** `writer.commit(messages)` is called on the driver with a `List[Optional[WriterCommitMessage]]`
**And** the write operation completes successfully

### S8: Abort on Failure

**Given** a write operation where one or more partition tasks fail
**When** a `writer.write(iterator)` call raises an exception
**Then** `writer.abort(messages)` is called on the driver
**And** failed partitions have `None` in the messages list
**And** the original error is propagated to the user

### S9: Multi-Partition Write

**Given** a DataFrame with N partitions
**When** writing to a Python DataSource
**Then** `writer.write(iterator)` is called N times (once per partition)
**And** each call receives only the rows/batches for that partition
**And** all calls can execute in parallel (subject to GIL constraints in InProcessExecutor)

### S10: Writer Must Be Picklable

**Given** a Python DataSource
**When** `datasource.writer(schema, overwrite)` returns a writer
**Then** Sail pickles the writer using `pyspark.cloudpickle`
**And** deserializes it on each executor before calling `write()`
**And** if pickling fails, a clear error is returned

### S11: Writer Type Detection

**Given** a Python DataSource
**When** `datasource.writer(schema, overwrite)` returns a writer
**Then** Sail checks `isinstance(writer, DataSourceArrowWriter)` first
**And** if true, uses the Arrow path (Iterator[RecordBatch])
**And** if false, checks `isinstance(writer, DataSourceWriter)`
**And** if true, uses the Row path (Iterator[Row])
**And** if neither, returns an error

### S12: partitionBy Is Ignored (PySpark Compat)

**Given** a user calls `df.write.format("my_source").partitionBy("col").save()`
**When** the write reaches `PythonTableFormat::create_writer()`
**Then** `partition_by` is ignored (not passed to Python)
**And** a warning is logged: "partitionBy is not supported for Python datasource '...' and will be ignored"
**And** data is passed through as-is (matching PySpark behavior)

### S13: DataSource Without Writer

**Given** a Python DataSource that does NOT override `writer()`
**When** the user calls `df.write.format("my_source").save()`
**Then** PySpark's default `writer()` raises `NOT_IMPLEMENTED`
**And** Sail catches the Python exception and returns a clear error

### S14: Empty DataFrame Write

**Given** a DataFrame with 0 rows
**When** writing to a Python DataSource
**Then** `writer.write(iterator)` is called with an empty iterator
**And** `WriterCommitMessage` (or None) is returned
**And** `writer.commit([msg])` is called normally

### S15: Options Passed Through

**Given** a user calls `df.write.format("my_source").option("key", "val").save()`
**When** the DataSource is instantiated
**Then** the options dict includes `{"key": "val"}`
**And** the writer receives the configured datasource instance

---

## Edge Cases

1. **Writer returns None as commit message**: Treated as valid — `None` is included in the messages list passed to `commit()`
2. **commit() raises exception**: Error propagated to user; write is considered failed
3. **abort() raises exception**: Error is logged but the original write failure is still propagated
4. **Python writer not installed**: Clear error message about PySpark requirement
5. **Schema with unsupported types for Row path**: Only MVP types (Int32, Int64, Float32, Float64, Utf8, Boolean, Date32, Timestamp) supported for Row conversion; Arrow path supports all types

---

## Acceptance Criteria

- [ ] `DataSourceWriter` (Row-based) write works end-to-end
- [ ] `DataSourceArrowWriter` (Arrow-based) write works end-to-end
- [ ] Save modes: append and overwrite work correctly
- [ ] Two-phase commit: commit() called on success, abort() on failure
- [ ] Multi-partition writes execute correctly
- [ ] Writer pickling/unpickling works
- [ ] partitionBy is silently ignored with a warning log
- [ ] Error messages are clear and include Python tracebacks
- [ ] Existing read tests continue to pass
- [ ] New write tests cover all scenarios
