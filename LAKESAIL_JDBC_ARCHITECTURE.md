# JDBC Reader for Lakesail - Proper Architecture

## The Real Question (Stepping Back)

"How do we make JDBC tables available to Lakesail's Spark Connect Server?"

Forget Spark primitives. Focus on what Lakesail needs.

## What Lakesail Is

Looking at `python/pysail/spark/__init__.py`:

```python
class SparkConnectServer:
    """The Spark Connect server that uses Sail as the computation engine."""

    def __init__(self, ip: str = "127.0.0.1", port: int = 0):
        self._inner = _native.spark.SparkConnectServer(ip, port)
```

**Key insight**: Lakesail IS the computation engine. It has its own:
- Rust-based Spark Connect Server
- Query execution engine
- Data source integration

## What We Want to Achieve

Without Spark terminology:

```
Client                 Lakesail Server              Database
┌──────┐              ┌───────────────┐            ┌────────┐
│      │  SQL query   │               │  JDBC      │        │
│ User │ ──────────>  │   Lakesail    │ ────────>  │  Postgres
│      │              │   Engine      │  parallel  │        │
│      │  <──────────  │               │  <──────── │        │
│      │  DataFrame   │               │  Arrow     │        │
└──────┘              └───────────────┘            └────────┘
                            │
                            ├─ Partition 1 reads rows 1-1M
                            ├─ Partition 2 reads rows 1M-2M
                            └─ Partition N reads rows NM-(N+1)M
```

**Goals**:
1. User writes SQL: `SELECT * FROM jdbc_table WHERE ...`
2. Lakesail server executes query (distributed)
3. Lakesail reads from database in parallel
4. Returns results to client

## How Lakesail Works (What We Know)

From the codebase:
1. **Native engine**: `_native` module (Rust)
2. **Spark Connect Server**: Built into Lakesail
3. **Data sources**: Iceberg, files, etc. registered with engine
4. **Distribution**: Handled by Lakesail internally

## The Architecture Question

How do other data sources integrate with Lakesail?

Let's look at what Lakesail needs:
- Schema information (what columns?)
- Partition metadata (how to split work?)
- Read function (get data for partition X)

This is EXACTLY what `ArrowBatchDataSource` provides!

```python
class ArrowBatchDataSource(ABC):
    def infer_schema(self, options) -> pa.Schema:
        """What columns/types does this data have?"""

    def plan_partitions(self, options) -> List[Dict]:
        """How should we split the work?"""

    def read_partition(self, partition_spec, options) -> Iterator[pa.RecordBatch]:
        """Get Arrow data for partition X"""
```

## Proposed Architecture (Lakesail-Native)

### Step 1: Register JDBC Data Source with Lakesail Engine

Instead of using Spark RDDs, register with Lakesail's engine:

```python
# Somewhere in Lakesail's native layer (Rust)
// lakesail_core/src/datasources/registry.rs

pub struct DataSourceRegistry {
    sources: HashMap<String, Box<dyn DataSource>>
}

impl DataSourceRegistry {
    pub fn register_jdbc(&mut self, name: &str, config: JDBCConfig) {
        self.sources.insert(name, Box::new(JDBCDataSource::new(config)));
    }
}
```

### Step 2: Python Interface

```python
# Python side
from pysail.spark import SparkConnectServer
from pysail.read import JDBCArrowDataSource

# Start Lakesail server
server = SparkConnectServer("127.0.0.1", 15002)
server.start()

# Register JDBC source with Lakesail engine (NOT Spark!)
server.register_datasource(
    name="my_postgres",
    datasource=JDBCArrowDataSource(),
    options={
        'url': 'jdbc:postgresql://localhost:5432/mydb',
        'dbtable': 'orders'
    }
)

# Client connects
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Query uses Lakesail's registered data source
df = spark.sql("SELECT * FROM my_postgres WHERE status = 'active'")
```

### Step 3: Execution Flow

```
1. Client sends: "SELECT * FROM my_postgres WHERE ..."
   │
   v
2. Lakesail Server receives query
   │
   v
3. Lakesail looks up "my_postgres" in registry
   │
   v
4. Lakesail calls: datasource.plan_partitions(options)
   Returns: [partition_1, partition_2, ..., partition_N]
   │
   v
5. Lakesail distributes work (uses its own scheduler, NOT Spark's)
   - Worker 1: read_partition(partition_1)
   - Worker 2: read_partition(partition_2)
   - Worker N: read_partition(partition_N)
   │
   v
6. Workers return Arrow RecordBatches
   │
   v
7. Lakesail sends result to client
```

## What We Need to Build

### Option A: Native Rust Integration (Proper)

Extend Lakesail's Rust core:

```rust
// lakesail_core/src/datasources/jdbc.rs

pub struct JDBCDataSource {
    url: String,
    table: String,
    // Uses arrow-jdbc or similar
}

impl DataSource for JDBCDataSource {
    fn schema(&self) -> Schema { ... }
    fn plan_partitions(&self) -> Vec<PartitionSpec> { ... }
    fn read_partition(&self, spec: PartitionSpec) -> ArrowBatch { ... }
}
```

Register in Lakesail's engine, not Spark.

### Option B: Python Extension (Interim)

If Lakesail supports Python-based data sources:

```python
# Register Python datasource with Lakesail server
class LakesailJDBCSource:
    def __init__(self, datasource: ArrowBatchDataSource):
        self.datasource = datasource

    def to_lakesail_source(self):
        """Convert to format Lakesail engine understands"""
        return {
            'type': 'python_callback',
            'schema': self.datasource.infer_schema(...),
            'partitions': self.datasource.plan_partitions(...),
            'reader': self.datasource.read_partition
        }
```

## Questions We Need Answered

1. **How does Lakesail register data sources today?**
   - Look at how Iceberg is integrated
   - Is it Rust-native or Python-callable?

2. **Does Lakesail have a data source plugin API?**
   - Can we extend it?
   - Or do we need to modify core?

3. **How does Lakesail distribute work?**
   - Does it have its own scheduler?
   - Or does it still use Spark's executor model?

4. **Can Lakesail call Python code from Rust workers?**
   - If yes: Register Python datasource
   - If no: Must implement in Rust

## Next Step

Let me investigate Lakesail's data source architecture:
- How is Iceberg integrated?
- Is there a plugin API?
- Can we register custom sources?

This is the RIGHT way to think about it - not as "how do we use Spark RDDs" but "how do we extend Lakesail's engine."
