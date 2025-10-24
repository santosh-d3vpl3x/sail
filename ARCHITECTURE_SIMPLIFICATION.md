# Architecture Simplification Proposal

## Current Problem

We have a complex adapter layer (`spark_adapter.py`, `spark_integration.py`) with PySpark dependencies, but most users don't need it!

## Proposed Simplification

### Tier 1: Pure Arrow (NO dependencies)
```python
from pysail.read import JDBCArrowDataSource

datasource = JDBCArrowDataSource()
arrow_table = datasource.to_arrow_table({'url': '...', 'dbtable': '...'})

# Use anywhere
polars_df = pl.from_arrow(arrow_table)
duckdb_df = duckdb.from_arrow(arrow_table)
pandas_df = arrow_table.to_pandas()
```

**Dependencies**: Only PyArrow + backend (ConnectorX/ADBC)

### Tier 2: Simple Spark (Built-in integration)
```python
arrow_table = datasource.to_arrow_table(options)
df = spark.createDataFrame(arrow_table.to_pandas())  # Spark native
```

**Dependencies**: Spark Connect client (thin) + PyArrow

### Tier 3: Distributed Spark (Our adapter - optional!)
```python
from pysail.read.spark_adapter import to_spark_dataframe  # Explicit import

df = to_spark_dataframe(spark, datasource, {
    'url': '...',
    'dbtable': 'huge_table',
    'numPartitions': 100  # Parallel distributed reads
})
```

**Dependencies**: Spark Connect client + our adapter
**When needed**: 100M+ rows, need to parallelize across executors

## Benefits

1. **Most users never import Spark adapter**
   - Pure Arrow workflows (Polars, DuckDB, pandas)
   - Simple Spark via native integration

2. **Clear separation**
   - Core: Framework-agnostic
   - Adapters: Optional power-user features

3. **Spark Connect friendly**
   - Tier 1 & 2: NO custom adapter needed
   - Tier 3: Only for massive scale

## Recommended Changes

### 1. Simplify `read_jdbc()` in `__init__.py`

**Current** (always uses adapter):
```python
def read_jdbc(spark, url, dbtable, ...):
    from .spark_adapter import to_spark_dataframe
    datasource = JDBCArrowDataSource()
    return to_spark_dataframe(spark, datasource, options)  # Always distributed
```

**Proposed** (use simple path by default):
```python
def read_jdbc(spark, url, dbtable, ..., distributed=False):
    """
    Args:
        distributed: If False (default), use simple Spark integration.
                    If True, use distributed mapPartitions (for huge tables).
    """
    datasource = JDBCArrowDataSource()
    arrow_table = datasource.to_arrow_table(options)

    if distributed:
        # Only import if needed
        from .spark_adapter import to_spark_dataframe
        return to_spark_dataframe(spark, datasource, options)
    else:
        # Use Spark's native Arrow support (simpler!)
        return spark.createDataFrame(arrow_table.to_pandas())
```

### 2. Update Documentation

Emphasize:
- **Default path**: Pure Arrow or simple Spark
- **Advanced path**: Distributed adapter (explicit opt-in)

### 3. Consider Deprecating `spark_integration.py`

The monkey-patching of `spark.read.jdbc()` adds complexity. Better:
```python
# Direct, explicit
from pysail.read import JDBCArrowDataSource
datasource = JDBCArrowDataSource()
arrow_table = datasource.to_arrow_table(options)
df = spark.createDataFrame(arrow_table.to_pandas())
```

## Question for User

Do you want to:

**Option A**: Keep adapter as-is (optional advanced feature)
- Pro: Supports massive distributed reads
- Con: Adds PySpark dependency complexity

**Option B**: Simplify to Arrow-only, remove adapters
- Pro: Zero framework dependencies
- Con: Users handle Spark integration themselves

**Option C**: Middle ground (recommended)
- Core: Pure Arrow (current)
- Simple helper: `spark.createDataFrame(arrow_table.to_pandas())`
- Advanced adapter: Available but not default

What's your preference?
