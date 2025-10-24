---
title: Database Connectivity (JDBC)
rank: 8
---

# Database Connectivity (JDBC)

Sail can ingest data directly from popular SQL databases without a custom
build. The Python API mirrors Spark's ``read.jdbc`` ergonomics while supporting
optional [ConnectorX](https://github.com/sfu-db/connector-x) and
[ADBC](https://arrow.apache.org/adbc/) backends.

```python
from lakesail import read

orders = read.jdbc(
    url="jdbc:postgresql://localhost:5432/shop",
    table="public.orders",
    properties={"user": "shop", "password": "secret"},
    column="order_id",
    lowerBound=0,
    upperBound=5_000_000,
    numPartitions=32,
)
```

## Backend selection

* ``backend="auto"`` (default) prefers ADBC when a matching driver is
  installed, falling back to ConnectorX otherwise.
* ``backend="adbc"`` enforces the ADBC path and surfaces actionable errors when
  the requested driver is missing.
* ``backend="connectorx"`` forces ConnectorX and uses the Arrow return path for
  zero-copy hand-off.

Install the ``pysail[db]`` extra to pull in both backends:

```bash
pip install "pysail[db]"
```

## Partitioning and predicates

Spark-compatible partition parameters are available:

```python
read.jdbc(
    url="jdbc:mysql://localhost:3306/metrics",
    query="SELECT * FROM metrics WHERE day >= '2024-01-01'",
    properties={"user": "root", "password": "secret"},
    column="id",
    lowerBound=0,
    upperBound=10_000,
    numPartitions=8,
    predicates=["kind = 'click'", "kind = 'view'"],
)
```

Each partition is dispatched as an independent query, allowing distributed
execution and partition-level retries.

## Configuration

Tune defaults with environment variables or YAML configuration:

```yaml
lakesail.db:
  default_backend: auto
  fetch_chunk_rows: 100000
  retries: 3
  retry_backoff_sec: 1.5
```

Environment variables override YAML values. For example,
``LAKESAIL_DB_FETCH_CHUNK_ROWS=50000`` halves the fetch size.

## Extensibility

Register custom database handlers to integrate bespoke drivers without
modifying Sail's core:

```python
from lakesail import read

@read.register_reader("trino")
def read_trino(plan):
    import trino
    # ... build a table from ``plan.partitions``
    return arrow_table
```

The registry receives the resolved execution plan, including partition SQL,
connection URL, and retry hints.
