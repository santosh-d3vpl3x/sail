# Change: python-datasource-write

**Created**: 2026-02-13
**Workflow**: Fast-Forward (all artifacts created at once)

## Status

- ✓ PROPOSAL.md - COMPLETED
- ✓ SPEC.md - COMPLETED
- ✓ DESIGN.md - COMPLETED
- ✓ TASKS.md - COMPLETED
- ○ Implementation - PENDING

## Summary

Add write support for Python DataSources in Sail, matching PySpark 4.1 API:
- `DataSourceWriter` (Row-based, `Iterator[Row]`)
- `DataSourceArrowWriter` (Arrow-based, `Iterator[RecordBatch]`)
- Two-phase commit (write → commit/abort)
- partitionBy silently ignored (PySpark compat)

## Next Step

Use `/opsx:apply` to implement tasks T1-T10.
