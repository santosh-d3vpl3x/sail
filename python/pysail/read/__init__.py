"""Lakesail JDBC Reader - Python-based database reading with Arrow backends.

This module provides JDBC database reading capabilities using high-performance
backends (ConnectorX, ADBC) with Arrow-native data transfer.

Usage (Native Spark API - Recommended):
    from pyspark.sql import SparkSession
    from pysail.read import install_jdbc_reader

    spark = SparkSession.builder.appName("myapp").getOrCreate()

    # Install JDBC reader (adds spark.read.jdbc() and spark.read.format("jdbc"))
    install_jdbc_reader(spark)

    # Now use standard Spark API!
    df = spark.read.jdbc(
        url="jdbc:postgresql://localhost:5432/mydb",
        table="orders",
        properties={"user": "admin", "password": "secret"}
    )

    # Or use DataSource V2 style
    df = spark.read \\
        .format("jdbc") \\
        .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
        .option("dbtable", "orders") \\
        .option("user", "admin") \\
        .load()

Alternative Usage (Direct API):
    from pysail.read import read_jdbc

    df = read_jdbc(
        spark,
        url="jdbc:postgresql://localhost:5432/mydb",
        dbtable="orders",
        user="admin",
        password="secret",
        engine="connectorx"  # or "adbc" or "fallback"
    )
"""

import logging
from typing import Dict, Optional

from pyspark.sql import SparkSession, DataFrame

from .data_source import read_jdbc as _read_jdbc_impl
from .spark_integration import install_jdbc_reader, jdbc as jdbc_reader
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
    # Main API functions
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
    spark: SparkSession,
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
) -> DataFrame:
    """
    Read from JDBC database using high-performance Arrow backends.

    This is the direct API. For a more Spark-native experience, use
    install_jdbc_reader() and then spark.read.jdbc().

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

        # With custom query
        df = read_jdbc(
            spark,
            url="jdbc:postgresql://localhost:5432/mydb",
            query="SELECT * FROM orders WHERE created_at > '2024-01-01'",
            engine="adbc"
        )
    """
    # Build options dictionary
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

    return _read_jdbc_impl(spark, options)


# Deprecated: Use install_jdbc_reader() instead
def register_jdbc_reader(spark: SparkSession) -> None:
    """
    Register JDBC reader (deprecated).

    Use install_jdbc_reader() instead for better integration.

    Args:
        spark: SparkSession
    """
    logger.warning(
        "register_jdbc_reader() is deprecated. Use install_jdbc_reader() instead."
    )
    install_jdbc_reader(spark)
