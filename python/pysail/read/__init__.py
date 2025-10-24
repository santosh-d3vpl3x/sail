"""Lakesail JDBC Reader - Framework-agnostic database reading with Arrow.

This module provides database reading capabilities with NO framework dependencies
in the core. Framework integration (Spark, Polars, etc.) is via separate adapters.

Core Layer (NO framework dependencies):
    ├── ArrowBatchDataSource (pure abstraction)
    ├── JDBCArrowDataSource (JDBC implementation)
    ├── Backends (ConnectorX, ADBC, fallback)
    └── Utilities (URL parsing, options, partitioning)

Adapter Layer (framework-specific):
    ├── spark_adapter → Spark DataFrame
    ├── polars_adapter → Polars DataFrame (future)
    └── duckdb_adapter → DuckDB (future)

Usage - Pure Arrow (NO framework required):
    from pysail.read.arrow_datasource import JDBCArrowDataSource

    datasource = JDBCArrowDataSource()
    options = {
        'url': 'jdbc:postgresql://localhost:5432/mydb',
        'dbtable': 'orders',
        'user': 'admin',
        'password': 'secret'
    }

    # Get Arrow Table
    table = datasource.to_arrow_table(options)
    print(table.to_pandas())

Usage - With Spark:
    from pyspark.sql import SparkSession
    from pysail.read import read_jdbc  # or use_arrow_datasource

    spark = SparkSession.builder.getOrCreate()

    # Option 1: Convenience function
    df = read_jdbc(
        spark,
        url='jdbc:postgresql://localhost:5432/mydb',
        dbtable='orders',
        user='admin',
        password='secret'
    )

    # Option 2: Direct adapter usage
    from pysail.read.arrow_datasource import JDBCArrowDataSource
    from pysail.read.spark_adapter import to_spark_dataframe

    datasource = JDBCArrowDataSource()
    df = to_spark_dataframe(spark, datasource, options)

Usage - With Spark Native API:
    from pysail.read import install_jdbc_reader

    install_jdbc_reader(spark)

    # Now use standard Spark API (as convenience wrapper)
    df = spark.read.jdbc(
        url='jdbc:postgresql://localhost:5432/mydb',
        table='orders',
        properties={'user': 'admin', 'password': 'secret'}
    )
"""

import logging
from typing import Dict, Optional

# Core exports (NO framework dependencies)
from .arrow_datasource import ArrowBatchDataSource, JDBCArrowDataSource
from .exceptions import (
    JDBCReaderError,
    InvalidJDBCUrlError,
    BackendNotAvailableError,
    UnsupportedDatabaseError,
    DatabaseError,
    SchemaInferenceError,
    InvalidOptionsError,
)

__all__ = [
    # Core abstractions (no framework dependencies)
    "ArrowBatchDataSource",
    "JDBCArrowDataSource",

    # Convenience functions (lazy-load frameworks)
    "read_jdbc",
    "install_jdbc_reader",

    # Exceptions
    "JDBCReaderError",
    "InvalidJDBCUrlError",
    "BackendNotAvailableError",
    "UnsupportedDatabaseError",
    "DatabaseError",
    "SchemaInferenceError",
    "InvalidOptionsError",
]

# Set up logging
logger = logging.getLogger("lakesail.jdbc")
logger.setLevel(logging.INFO)

# Add console handler if not already added
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def read_jdbc(
    spark,  # Type hint omitted to avoid PySpark import
    url: str,
    dbtable: Optional[str] = None,
    query: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    engine: str = "connectorx",
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    num_partitions: int = 1,
    predicates: Optional[str] = None,
    fetch_size: int = 10000,
    **kwargs
):
    """
    Read from JDBC database using Arrow backends, return Spark DataFrame.

    This is a convenience function that:
    1. Creates JDBCArrowDataSource
    2. Uses Spark adapter to convert to DataFrame

    For more control, use the arrow_datasource and spark_adapter directly.

    Args:
        spark: SparkSession
        url: JDBC URL (e.g., jdbc:postgresql://localhost:5432/mydb)
        dbtable: Table name or subquery (e.g., "(SELECT * FROM orders) AS t")
        query: Full SQL query (alternative to dbtable)
        user: Database user (can also be in URL)
        password: Database password (can also be in URL)
        engine: Backend engine ('connectorx', 'adbc', or 'fallback')
        partition_column: Column for range partitioning
        lower_bound: Partition range lower bound
        upper_bound: Partition range upper bound
        num_partitions: Number of partitions for parallelism
        predicates: Comma-separated WHERE predicates (one per partition)
        fetch_size: Batch size for fetching
        **kwargs: Additional options

    Returns:
        Spark DataFrame

    Raises:
        InvalidOptionsError: If options are invalid
        InvalidJDBCUrlError: If JDBC URL is malformed
        BackendNotAvailableError: If backend library is not installed
        DatabaseError: If database operation fails

    Examples:
        # Basic read
        df = read_jdbc(
            spark,
            url="jdbc:postgresql://localhost:5432/mydb",
            dbtable="orders",
            user="admin",
            password="secret"
        )

        # With partitioning
        df = read_jdbc(
            spark,
            url="jdbc:postgresql://localhost:5432/mydb",
            dbtable="orders",
            partition_column="order_id",
            lower_bound=1,
            upper_bound=1000000,
            num_partitions=10
        )
    """
    # Lazy import Spark adapter (only when actually using Spark)
    from .spark_adapter import to_spark_dataframe

    # Build options
    options = {
        "url": url,
        "engine": engine,
        "fetchsize": str(fetch_size),
        "numpartitions": str(num_partitions),
    }

    if dbtable:
        options["dbtable"] = dbtable
    if query:
        options["query"] = query
    if user:
        options["user"] = user
    if password:
        options["password"] = password
    if partition_column:
        options["partitioncolumn"] = partition_column
    if lower_bound is not None:
        options["lowerbound"] = str(lower_bound)
    if upper_bound is not None:
        options["upperbound"] = str(upper_bound)
    if predicates:
        options["predicates"] = predicates

    # Add any additional kwargs
    options.update(kwargs)

    # Create datasource
    datasource = JDBCArrowDataSource()

    # Convert to Spark DataFrame
    return to_spark_dataframe(spark, datasource, options)


def install_jdbc_reader(spark) -> None:
    """
    Install JDBC reader into Spark session.

    This adds spark.read.jdbc() method as a convenience wrapper
    around read_jdbc(). It's a thin convenience layer.

    Args:
        spark: SparkSession

    Example:
        from pyspark.sql import SparkSession
        from pysail.read import install_jdbc_reader

        spark = SparkSession.builder.getOrCreate()
        install_jdbc_reader(spark)

        # Now use standard Spark API
        df = spark.read.jdbc(
            url="jdbc:postgresql://localhost/db",
            table="orders",
            properties={"user": "admin", "password": "secret"}
        )
    """
    # Lazy import to avoid PySpark dependency
    from .spark_integration import install_jdbc_reader as _install
    _install(spark)


# Deprecated: Use install_jdbc_reader() instead
def register_jdbc_reader(spark) -> None:
    """
    Register JDBC reader (deprecated).

    Use install_jdbc_reader() instead.

    Args:
        spark: SparkSession
    """
    logger.warning(
        "register_jdbc_reader() is deprecated. Use install_jdbc_reader() instead."
    )
    install_jdbc_reader(spark)
