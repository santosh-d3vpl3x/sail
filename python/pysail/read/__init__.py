"""Lakesail JDBC Reader - Python-based database reading with Arrow backends.

This module provides JDBC database reading capabilities using high-performance
backends (ConnectorX, ADBC) with Arrow-native data transfer.

Usage:
    from pyspark.sql import SparkSession
    from pysail.read import register_jdbc_reader

    spark = SparkSession.builder.appName("myapp").getOrCreate()

    # Register the JDBC reader (makes it available via spark.read.format("jdbc_sail"))
    register_jdbc_reader(spark)

    # Read from database
    df = spark.read \\
        .format("jdbc_sail") \\
        .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
        .option("dbtable", "orders") \\
        .option("user", "myuser") \\
        .option("password", "mypass") \\
        .load()

    # Or use the direct API
    from pysail.read import read_jdbc

    df = read_jdbc(
        spark,
        url="jdbc:postgresql://localhost:5432/mydb",
        dbtable="orders",
        user="myuser",
        password="mypass",
        engine="connectorx"  # or "adbc" or "fallback"
    )
"""

import logging
from typing import Dict, Optional

from pyspark.sql import SparkSession, DataFrame

from .data_source import read_jdbc as _read_jdbc_impl
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
    "read_jdbc",
    "register_jdbc_reader",
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


def register_jdbc_reader(spark: SparkSession) -> None:
    """
    Register JDBC reader as a Spark data source format.

    This allows using spark.read.format("jdbc_sail").option(...).load()

    Note: This is a placeholder. Full integration requires registering
    a custom DataSource V2 provider, which may require Scala/Java code.

    For now, users should use the read_jdbc() function directly.

    Args:
        spark: SparkSession
    """
    logger.warning(
        "register_jdbc_reader() is not yet fully implemented. "
        "Please use pysail.read.read_jdbc() directly for now."
    )

    # TODO: Implement custom DataSource V2 registration
    # This would require:
    # 1. Creating a DataSourceRegister in Scala/Java
    # 2. Registering it with Spark's DataSource registry
    # 3. Or using Python UDF-based approach (less efficient)

    # For now, we just log a warning
    logger.info(
        "To use JDBC reader, call: "
        "from pysail.read import read_jdbc; "
        "df = read_jdbc(spark, url='...', dbtable='...')"
    )
