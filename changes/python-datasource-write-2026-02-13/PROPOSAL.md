# Proposal: Python DataSource Write Support

## Problem Statement

Sail's Python DataSource integration currently supports only **read** operations. When a user calls `df.write.format("my_python_source").save()`, the Rust layer returns `not_impl_err!` from `PythonTableFormat::create_writer()`. This blocks any PySpark user who has a custom Python DataSource with write capabilities.

## Goals

1. **100% PySpark 4.1 API compatibility** — support `DataSourceWriter` (row-based) and `DataSourceArrowWriter` (arrow-based) with identical semantics
2. **Two-phase commit** — implement the full write lifecycle: `write()` per partition, then `commit()` or `abort()` on the driver
3. **Zero-copy Arrow path** — for `DataSourceArrowWriter`, pass `RecordBatch` via Arrow C Data Interface without row-level conversion
4. **Pass-through partitioning** — match PySpark behavior where `partitionBy` is silently ignored for Python DataSources (log a warning)

## Non-Goals

- Streaming writers (`DataSourceStreamWriter`, `DataSourceStreamArrowWriter`) — deferred to a future change
- Rust-side repartitioning for `partitionBy` — PySpark doesn't do this, neither will we
- Schema evolution during writes

## Success Criteria

- `df.write.format("python_ds").mode("append").save()` works end-to-end
- `df.write.format("python_ds").mode("overwrite").save()` works end-to-end
- Both `DataSourceWriter` (Row iterator) and `DataSourceArrowWriter` (RecordBatch iterator) are supported
- Commit messages are collected and passed to `commit()` on success
- On any partition failure, `abort()` is called with collected messages (failed partitions have `None`)
- Existing read tests continue to pass
- New write tests cover: basic write, arrow write, multi-partition, commit, abort, error handling

## High-Level Approach

Extend the existing Python DataSource infrastructure (executor, table_format, arrow_utils) with write methods, and create two new execution plan nodes: one for per-partition writing and one for driver-side commit/abort orchestration. Follow the same patterns used by the read path (pickle serialization, spawn_blocking for Python calls, Arrow C Data Interface for zero-copy).

## References

- PySpark DataSource API: `pyspark.sql.datasource` module
- Existing read implementation: `crates/sail-data-source/src/formats/python/`
- PySpark source: `apache/spark` master — `PythonWrite.scala`, `PythonTable.scala`, `datasource.py`
- Spark does NOT pass `partitionBy` to Python writers (confirmed from source)
