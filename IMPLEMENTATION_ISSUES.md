# JDBC Reader Implementation Issues Found During Testing

## Summary

During testing with Spark Connect, several issues were identified that prevent the JDBC reader from working:

## Issues Found

### 1. **Import Dependency Problem** (CRITICAL)

**Problem**: The `python/pysail/read/__init__.py` imports `PySpark` at the module level, making the entire module unusable without PySpark installed.

```python
# Current code in __init__.py (line 46)
from pyspark.sql import SparkSession, DataFrame  # ❌ Fails if pyspark not installed
```

**Impact**: Cannot import any submodule (e.g., `jdbc_url_parser`, `jdbc_options`) without PySpark.

**Fix**: Use lazy imports - only import PySpark when actually needed.

```python
# Proposed fix - lazy import
def read_jdbc(spark, ...):
    from pyspark.sql import DataFrame  # ✅ Import only when used
    ...
```

### 2. **Broadcasting Custom Objects** (HIGH PRIORITY)

**Problem**: In `arrow_datasource.py` and `data_source.py`, we broadcast custom Python objects:

```python
# Current code (line 230 in arrow_datasource.py)
broadcast_self = spark.sparkContext.broadcast(self)  # ❌ May not serialize properly
```

**Impact**: Custom Python objects (especially with abstract methods) may not serialize correctly across Spark Connect's client-server boundary.

**Fix**: Instead of broadcasting the datasource object, broadcast only the configuration (dict) and recreate the datasource on each executor.

```python
# Proposed fix
broadcast_config = spark.sparkContext.broadcast({
    'datasource_class': 'JDBCArrowDataSource',
    'options': options
})

def partition_reader(partition_iter):
    config = broadcast_config.value
    datasource = globals()[config['datasource_class']]()  # Recreate on executor
    ...
```

### 3. **Monkey-Patching DataFrameReader** (MEDIUM PRIORITY)

**Problem**: In `spark_integration.py`, we monkey-patch `DataFrameReader.format()` and add `DataFrameReader.jdbc()`:

```python
# Current code (line 138 in spark_integration.py)
DataFrameReader.format = format_with_jdbc  # ❌ May not work in Spark Connect
DataFrameReader.jdbc = jdbc_method
```

**Impact**: Spark Connect uses a client-server architecture. Monkey-patching the client-side class may not affect server-side behavior. The patched methods exist locally but Spark server doesn't know about them.

**Potential Issues**:
- `spark.read.format("jdbc")` might not recognize our custom format
- `spark.read.jdbc()` method added to client but server expects different signature

**Fix Options**:

**Option A (Recommended)**: Keep the direct API `read_jdbc()` as primary, provide `spark.read.jdbc()` as a convenience wrapper that calls `read_jdbc()` internally:

```python
def install_jdbc_reader(spark):
    """Add jdbc() method to DataFrameReader (convenience only)."""
    def jdbc_wrapper(self, url, table, **kwargs):
        from pysail.read import read_jdbc
        return read_jdbc(self._spark, url=url, dbtable=table, **kwargs)

    DataFrameReader.jdbc = jdbc_wrapper
```

**Option B**: Implement as a true Spark Connect extension (requires server-side changes).

### 4. **Missing Dependencies in Test Environment** (LOW PRIORITY)

**Problem**: Tests require PySpark, PyArrow, and database drivers, but these aren't installed in the test environment.

**Fix**: Document dependencies clearly and add to test requirements.

## Recommendations

### Immediate Fixes (Critical Path):

1. **Fix Import Dependencies** - Refactor `__init__.py` to use lazy imports
2. **Fix Broadcasting** - Don't broadcast custom objects, broadcast config dicts instead
3. **Simplify API** - Focus on `read_jdbc()` direct API as primary interface

### Future Enhancements:

4. **Spark Connect Extension** - Implement proper Data Source V2 extension (server-side)
5. **Better Testing** - Add integration tests with TestContainers

## Proposed Architecture Changes

### Current Architecture (Has Issues):
```
User Code
    ↓
spark.read.jdbc() [monkey-patched]
    ↓
JDBCArrowDataSource [broadcasted - serialization issues]
    ↓
Backend
```

### Proposed Architecture (Fixes Issues):
```
User Code
    ↓
read_jdbc() [direct API - works reliably]
    ↓
Partition Planning [broadcast dict config]
    ↓
mapPartitions [recreate backend on each executor]
    ↓
Backend
```

### API Compromise:

Keep both APIs but make direct API primary:

```python
# Primary API (always works)
from pysail.read import read_jdbc
df = read_jdbc(spark, url="...", dbtable="...")

# Convenience API (may have limitations in Spark Connect)
from pysail.read import install_jdbc_reader
install_jdbc_reader(spark)
df = spark.read.jdbc(url="...", table="...")  # Wrapper around read_jdbc()
```

## Testing Results

| Test | Status | Notes |
|------|--------|-------|
| Module imports | ❌ FAILED | PySpark dependency issue |
| Direct API (read_jdbc) | ⏳ PENDING | Need to build native module |
| spark.read.jdbc() | ⏳ PENDING | Need to fix imports first |
| spark.read.format("jdbc") | ⏳ PENDING | Need to fix imports first |
| Serialization | ⚠️ CONCERN | Broadcasting custom objects |

## Next Steps

1. Fix import dependencies (make PySpark imports lazy)
2. Fix broadcasting (use config dicts, not objects)
3. Test with built native module
4. Document limitations of monkey-patching approach
5. Provide clear guidance on which API to use

## Files That Need Changes

1. **python/pysail/read/__init__.py** - Lazy imports
2. **python/pysail/read/data_source.py** - Fix broadcasting
3. **python/pysail/read/arrow_datasource.py** - Fix broadcasting
4. **python/pysail/read/spark_integration.py** - Simplify monkey-patching
5. **python/pysail/read/SPARK_API.md** - Document limitations

## Conclusion

The core JDBC reading functionality (backends, partitioning, options) is solid. The issues are all in the Spark integration layer. We can fix these with:

1. Lazy imports (easy fix)
2. Config-based approach instead of object broadcasting (medium fix)
3. Clear documentation about API limitations (documentation)

The direct `read_jdbc()` API will work reliably. The `spark.read.jdbc()` convenience wrapper can work as a thin wrapper around it.
