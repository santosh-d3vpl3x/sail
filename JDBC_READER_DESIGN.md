# JDBC Reader for Lakesail - Proper Design

## What We Discovered

**Lakesail uses Apache Arrow DataFusion as its query engine.**

Looking at `crates/sail-common-datafusion/src/datasource.rs`:

```rust
#[async_trait]
pub trait TableFormat: Send + Sync {
    fn name(&self) -> &str;

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>>;
}
```

This is the plugin API! Lakesail already has formats like:
- Parquet
- CSV
- Delta Lake
- Iceberg
- JSON

We need to add: **JDBC**

## What We Want to Achieve (No Spark Terminology)

```
User writes:
  spark.read.format("jdbc")
    .option("url", "postgresql://...")
    .option("dbtable", "orders")
    .load()

Lakesail Server:
  1. Receives request for JDBC format
  2. Creates JDBCTableProvider
  3. Plans partitions (split work)
  4. Executes reads in parallel
  5. Returns Arrow data to client

Database:
  ├─ Executor 1: SELECT * FROM orders WHERE id >= 0 AND id < 1000000
  ├─ Executor 2: SELECT * FROM orders WHERE id >= 1000000 AND id < 2000000
  └─ Executor N: SELECT * FROM orders WHERE id >= N*1000000 AND id < (N+1)*1000000
```

## The Lakesail Way (Using DataFusion)

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Lakesail Spark Connect Server (Rust)                      │
│  ┌────────────────────────────────────────────────────────┐ │
│  │  Apache Arrow DataFusion Engine                        │ │
│  │                                                          │ │
│  │  Table Formats (Registered):                            │ │
│  │  ├─ ParquetFormat                                       │ │
│  │  ├─ CsvFormat                                           │ │
│  │  ├─ DeltaFormat                                         │ │
│  │  ├─ IcebergFormat                                       │ │
│  │  └─ JDBCFormat  ← NEW!                                  │ │
│  │                                                          │ │
│  │  Each format provides:                                  │ │
│  │  - TableProvider (how to read)                          │ │
│  │  - ExecutionPlan (how to execute)                       │ │
│  │  - Partitioning (how to distribute)                     │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### What We Need to Build (Rust)

**1. JDBCFormat (TableFormat implementation)**

```rust
// crates/sail-jdbc/src/format.rs

pub struct JDBCFormat;

#[async_trait]
impl TableFormat for JDBCFormat {
    fn name(&self) -> &str {
        "jdbc"
    }

    async fn create_provider(
        &self,
        ctx: &dyn Session,
        info: SourceInfo,
    ) -> Result<Arc<dyn TableProvider>> {
        // Parse JDBC URL from options
        let url = info.options.get("url")?;
        let table = info.options.get("dbtable")?;

        // Create provider
        Ok(Arc::new(JDBCTableProvider::new(url, table, info.options)))
    }
}
```

**2. JDBCTableProvider (TableProvider implementation)**

```rust
// crates/sail-jdbc/src/provider.rs

pub struct JDBCTableProvider {
    url: String,
    table: String,
    schema: SchemaRef,
    options: HashMap<String, String>,
}

#[async_trait]
impl TableProvider for JDBCTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        session: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Create execution plan for JDBC read
        Ok(Arc::new(JDBCExec::new(
            self.url.clone(),
            self.table.clone(),
            self.schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
            self.options.clone(),
        )))
    }
}
```

**3. JDBCExec (ExecutionPlan implementation)**

```rust
// crates/sail-jdbc/src/exec.rs

pub struct JDBCExec {
    url: String,
    table: String,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    partitions: Vec<JDBCPartition>,
}

impl ExecutionPlan for JDBCExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // This is where the actual JDBC read happens!
        // Called once per partition, runs in parallel

        let partition_spec = &self.partitions[partition];

        // Build SQL query for this partition
        let sql = build_partition_query(
            &self.table,
            partition_spec,
            &self.filters,
            self.limit,
        );

        // Read from database using Arrow-JDBC
        let stream = jdbc_read_arrow(
            &self.url,
            &sql,
            self.schema.clone(),
        )?;

        Ok(stream)
    }
}
```

**4. JDBC Arrow Reader (Backend)**

```rust
// crates/sail-jdbc/src/reader.rs

use arrow::array::RecordBatch;
use arrow_jdbc::{JdbcAccessor, JdbcDataSource};  // Hypothetical Arrow JDBC crate

pub fn jdbc_read_arrow(
    url: &str,
    query: &str,
    schema: SchemaRef,
) -> Result<impl RecordBatchStream> {
    // Use Arrow-native JDBC library (like ConnectorX but in Rust)
    let accessor = JdbcAccessor::connect(url)?;

    accessor
        .query(query)?
        .into_arrow_stream(schema)
}
```

## How Distribution Works

DataFusion handles it automatically:

1. **Partition Planning**: `JDBCExec` creates N `JDBCPartition` specs:
   ```rust
   partitions: [
       JDBCPartition { predicate: "id >= 0 AND id < 1000000" },
       JDBCPartition { predicate: "id >= 1000000 AND id < 2000000" },
       ...
   ]
   ```

2. **Execution**: DataFusion calls `execute(partition_idx)` in parallel:
   ```rust
   // These all run in parallel across DataFusion's execution threads
   execute(0) -> reads "id >= 0 AND id < 1000000"
   execute(1) -> reads "id >= 1000000 AND id < 2000000"
   execute(N) -> reads "id >= N*1000000 AND id < (N+1)*1000000"
   ```

3. **No Spark RDDs Needed**: DataFusion has its own parallelism!

## What About Our Python Code?

Our existing Python `JDBCArrowDataSource` can be used:

**Option A**: Rust calls Python (PyO3)

```rust
// Use PyO3 to call Python datasource from Rust
use pyo3::prelude::*;

fn jdbc_read_partition_via_python(
    partition_spec: &JDBCPartition,
    options: &HashMap<String, String>,
) -> Result<RecordBatch> {
    Python::with_gil(|py| {
        let datasource = py.import("pysail.read.arrow_datasource")?
            .getattr("JDBCArrowDataSource")?
            .call0()?;

        let batch = datasource
            .call_method1("read_partition", (partition_spec, options))?;

        // Convert PyArrow to Rust Arrow
        batch.extract()
    })
}
```

**Option B**: Pure Rust (Better Performance)

Use existing Rust crates:
- `arrow-jdbc` (if it exists)
- `postgres` crate + manual Arrow conversion
- Port our Python backend logic to Rust

## Registration with Lakesail

Need to find where Lakesail registers formats:

```rust
// Somewhere in Lakesail's initialization
let format_registry = FormatRegistry::new();
format_registry.register("parquet", ParquetFormat);
format_registry.register("csv", CsvFormat);
format_registry.register("delta", DeltaFormat);
format_registry.register("jdbc", JDBCFormat);  // NEW!
```

## Summary: The Right Architecture

**NOT**: Try to use Spark RDD APIs (doesn't work with Spark Connect)

**YES**: Implement DataFusion's `TableFormat` trait:
1. Register JDBC format with Lakesail's engine
2. Implement `TableProvider` for JDBC
3. Implement `ExecutionPlan` that reads Arrow from DB
4. DataFusion handles distribution automatically

**Key Insight**: Lakesail IS the computation engine. We extend Lakesail, not Spark.

## Next Steps

1. Find where Lakesail registers table formats
2. Create `crates/sail-jdbc` crate
3. Implement `TableFormat`, `TableProvider`, `ExecutionPlan`
4. Use existing Rust JDBC + Arrow libraries
5. (Optional) Bridge to Python backends via PyO3

This is the proper distributed architecture, no Spark primitives needed!
