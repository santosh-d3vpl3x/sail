"""
Spark API integration for JDBC Arrow data source.

This module provides native Spark API integration:
- spark.read.jdbc(url, table, ...) - Standard Spark JDBC reader
- spark.read.format("jdbc").option(...).load() - DataSource V2 style

Implementation Strategy:
Since Lakesail uses Spark Connect (server-side execution), we integrate by:
1. Extending spark.read with a jdbc() method
2. Creating Arrow-backed DataFrames that Spark can consume
3. Using existing JDBC backend infrastructure

This approach provides a native Spark API feel while leveraging our
high-performance Arrow backends (ConnectorX, ADBC).
"""

import logging
from typing import Optional, Dict, Any

# Lazy imports to avoid PySpark dependency at module level
logger = logging.getLogger("lakesail.jdbc")


class JDBCDataFrameReader:
    """
    JDBC-specific DataFrameReader that provides Spark-native API.

    This class wraps our Arrow-based JDBC implementation to provide
    the standard Spark spark.read.jdbc() interface.
    """

    def __init__(self, spark):
        """
        Initialize JDBC reader.

        Args:
            spark: SparkSession instance (no type hint to avoid PySpark import)
        """
        self.spark = spark
        self._options: Dict[str, str] = {}

    def option(self, key: str, value: Any) -> "JDBCDataFrameReader":
        """
        Add an option to the JDBC reader.

        Args:
            key: Option name
            value: Option value

        Returns:
            Self for method chaining
        """
        self._options[key] = str(value)
        return self

    def options(self, **options: Any) -> "JDBCDataFrameReader":
        """
        Add multiple options to the JDBC reader.

        Args:
            **options: Option key-value pairs

        Returns:
            Self for method chaining
        """
        for key, value in options.items():
            self._options[key] = str(value)
        return self

    def load(
        self,
        url: Optional[str] = None,
        table: Optional[str] = None,
        **kwargs
    ):
        """
        Load data from JDBC source.

        Args:
            url: JDBC URL (can also be set via option())
            table: Table name (can also be set via option("dbtable", ...))
            **kwargs: Additional options

        Returns:
            Spark DataFrame

        Example:
            df = spark.read.format("jdbc") \\
                .option("url", "jdbc:postgresql://localhost/db") \\
                .option("dbtable", "orders") \\
                .load()
        """
        # Lazy import to use new architecture
        from .spark_adapter import to_spark_dataframe
        from .arrow_datasource import JDBCArrowDataSource

        # Merge arguments into options
        if url:
            self._options["url"] = url
        if table:
            self._options["dbtable"] = table

        self._options.update(kwargs)

        # Use new architecture: datasource + adapter
        datasource = JDBCArrowDataSource()
        return to_spark_dataframe(self.spark, datasource, self._options)


def jdbc(
    spark_or_reader,
    url: str,
    table: str,
    column: Optional[str] = None,
    lowerBound: Optional[int] = None,
    upperBound: Optional[int] = None,
    numPartitions: Optional[int] = None,
    predicates: Optional[list] = None,
    properties: Optional[dict] = None,
):
    """
    Read from JDBC database using standard Spark API.

    This function mimics PySpark's native spark.read.jdbc() signature
    while using our high-performance Arrow backends.

    Args:
        spark_or_reader: SparkSession or DataFrameReader
        url: JDBC URL
        table: Table name or subquery as string
        column: Column for partitioning (optional)
        lowerBound: Lower bound for partitioning (optional)
        upperBound: Upper bound for partitioning (optional)
        numPartitions: Number of partitions (optional)
        predicates: List of WHERE predicates for manual partitioning (optional)
        properties: Connection properties dict (user, password, etc.) (optional)

    Returns:
        Spark DataFrame

    Examples:
        # Basic read
        df = spark.read.jdbc(
            url="jdbc:postgresql://localhost:5432/mydb",
            table="orders",
            properties={"user": "admin", "password": "secret"}
        )

        # With partitioning
        df = spark.read.jdbc(
            url="jdbc:postgresql://localhost:5432/mydb",
            table="orders",
            column="order_id",
            lowerBound=1,
            upperBound=1000000,
            numPartitions=10,
            properties={"user": "admin"}
        )

        # With predicates
        df = spark.read.jdbc(
            url="jdbc:postgresql://localhost:5432/mydb",
            table="orders",
            predicates=["status='active'", "status='pending'"],
            properties={"user": "admin"}
        )
    """
    # Lazy import PySpark types and new architecture
    from pyspark.sql.readwriter import DataFrameReader
    from .spark_adapter import to_spark_dataframe
    from .arrow_datasource import JDBCArrowDataSource

    # Extract SparkSession from DataFrameReader if needed
    if isinstance(spark_or_reader, DataFrameReader):
        spark = spark_or_reader._spark  # noqa: SLF001
    else:
        spark = spark_or_reader

    # Build options dictionary
    options = {
        "url": url,
        "dbtable": table,
    }

    # Add partitioning options
    if column:
        options["partitionColumn"] = column
    if lowerBound is not None:
        options["lowerBound"] = str(lowerBound)
    if upperBound is not None:
        options["upperBound"] = str(upperBound)
    if numPartitions is not None:
        options["numPartitions"] = str(numPartitions)

    # Add predicates
    if predicates:
        options["predicates"] = ",".join(predicates)

    # Add connection properties
    if properties:
        for key, value in properties.items():
            options[key] = str(value)

    # Use new architecture: datasource + adapter
    datasource = JDBCArrowDataSource()
    return to_spark_dataframe(spark, datasource, options)


def install_jdbc_reader(spark) -> None:
    """
    Install JDBC reader into Spark session.

    This adds:
    - spark.read.jdbc() method (standard Spark API)
    - spark.read.format("jdbc") support

    Args:
        spark: SparkSession to augment (no type hint to avoid PySpark import)

    Example:
        from pysail.read import install_jdbc_reader

        spark = SparkSession.builder.getOrCreate()
        install_jdbc_reader(spark)

        # Now you can use standard Spark API
        df = spark.read.jdbc(
            url="jdbc:postgresql://localhost/db",
            table="orders"
        )
    """
    # Lazy import PySpark types
    from pyspark.sql.readwriter import DataFrameReader

    # Monkey-patch DataFrameReader to add jdbc() method
    original_format = DataFrameReader.format

    def format_with_jdbc(self, source: str):
        """Extended format() that recognizes 'jdbc' format."""
        if source.lower() == "jdbc":
            # Return our JDBC reader
            reader = JDBCDataFrameReader(self._spark)  # noqa: SLF001
            # Copy any existing options
            if hasattr(self, '_options'):
                reader._options = self._options.copy()  # noqa: SLF001
            return reader
        else:
            # Delegate to original format()
            return original_format(self, source)

    DataFrameReader.format = format_with_jdbc

    # Add jdbc() method to DataFrameReader
    def jdbc_method(
        self,
        url: str,
        table: str,
        column: Optional[str] = None,
        lowerBound: Optional[int] = None,
        upperBound: Optional[int] = None,
        numPartitions: Optional[int] = None,
        predicates: Optional[list] = None,
        properties: Optional[dict] = None,
    ):
        """JDBC reader method for DataFrameReader."""
        return jdbc(
            self,
            url=url,
            table=table,
            column=column,
            lowerBound=lowerBound,
            upperBound=upperBound,
            numPartitions=numPartitions,
            predicates=predicates,
            properties=properties,
        )

    DataFrameReader.jdbc = jdbc_method

    logger.info("JDBC reader installed: spark.read.jdbc() and spark.read.format('jdbc') available")


# Auto-install on import (optional, can be disabled)
def _auto_install():
    """Auto-install JDBC reader when module is imported."""
    try:
        # Lazy import
        from pyspark.sql import SparkSession
        from pyspark.sql.readwriter import DataFrameReader

        # Try to get active SparkSession
        spark = SparkSession.getActiveSession()
        if spark and not hasattr(DataFrameReader, '_jdbc_installed'):
            install_jdbc_reader(spark)
            DataFrameReader._jdbc_installed = True  # noqa: SLF001
    except Exception as e:
        # If no active session, skip auto-install
        logger.debug(f"Skipping auto-install of JDBC reader: {e}")


# Uncomment to enable auto-install on import
# _auto_install()
