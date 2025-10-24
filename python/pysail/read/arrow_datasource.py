"""
ArrowBatchDataSource - Base class for Arrow-native Spark data sources.

This module provides a foundation for creating custom Spark data sources that:
- Return Arrow RecordBatches natively (zero-copy when possible)
- Support distributed reading via partitioning
- Integrate with Spark's native spark.read.format() API
- Provide an extensible base for multiple Arrow-backed data sources

Architecture:
    User Code
        ↓
    spark.read.format("jdbc").option(...).load()
        ↓
    JDBCArrowDataSource (extends ArrowBatchDataSource)
        ↓
    ArrowBatchReader (partition-aware)
        ↓
    Backend (ConnectorX/ADBC/Fallback)
        ↓
    Arrow RecordBatches → Spark DataFrame

Design Goals:
1. Native Spark API integration (no custom functions)
2. Arrow-first data exchange (no row-wise conversion)
3. Distributed execution (partition-aware reading)
4. Extensible base for other Arrow data sources (JDBC, REST APIs, etc.)
5. Type preservation from source to Spark

Usage:
    from pyspark.sql import SparkSession
    from pysail.read import install_jdbc_reader

    spark = SparkSession.builder.getOrCreate()

    # Install the JDBC reader
    install_jdbc_reader(spark)

    # Standard Spark API - works out of the box!
    df = spark.read.jdbc(
        url="jdbc:postgresql://localhost:5432/mydb",
        table="orders",
        properties={"user": "admin", "password": "secret"}
    )

    # Or DataSource V2 style
    df = spark.read \\
        .format("jdbc") \\
        .option("url", "jdbc:postgresql://localhost:5432/mydb") \\
        .option("dbtable", "orders") \\
        .load()

Extending ArrowBatchDataSource:
    To create a new Arrow-backed data source:

    1. Subclass ArrowBatchDataSource
    2. Implement infer_schema() to return Arrow schema
    3. Implement plan_partitions() to generate partition specs
    4. Implement read_partition() to read Arrow batches per partition
    5. Register with install_datasource(spark, format_name)

    Example:
        class MyArrowDataSource(ArrowBatchDataSource):
            def infer_schema(self, options):
                # Return pyarrow.Schema
                pass

            def plan_partitions(self, options):
                # Return list of partition specs (dicts)
                pass

            def read_partition(self, partition_spec, options):
                # Yield pyarrow.RecordBatch objects
                pass
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Iterator, Any
import logging

import pyarrow as pa
from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger("lakesail.arrow_datasource")


class ArrowBatchDataSource(ABC):
    """
    Abstract base class for Arrow-native Spark data sources.

    This class provides the foundation for creating data sources that:
    - Read data as Arrow RecordBatches
    - Support distributed reading via partitioning
    - Preserve data types without conversion
    - Integrate seamlessly with Spark

    Subclasses must implement:
    - infer_schema(): Return Arrow schema for the data
    - plan_partitions(): Return list of partition specifications
    - read_partition(): Read Arrow batches for a single partition
    """

    @abstractmethod
    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        """
        Infer Arrow schema from data source.

        This is called once before reading to determine the schema.

        Args:
            options: Dictionary of data source options

        Returns:
            pyarrow.Schema for the data

        Raises:
            Exception: If schema cannot be inferred
        """
        pass

    @abstractmethod
    def plan_partitions(self, options: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Plan partitions for distributed reading.

        Each partition spec is a dictionary that will be passed to
        read_partition(). The specs can contain any information needed
        to read that partition (e.g., ID ranges, file paths, predicates).

        Args:
            options: Dictionary of data source options

        Returns:
            List of partition specifications (dicts)

        Examples:
            # Range-based partitioning
            return [
                {"partition_id": 0, "lower": 0, "upper": 100},
                {"partition_id": 1, "lower": 100, "upper": 200},
            ]

            # Predicate-based partitioning
            return [
                {"partition_id": 0, "predicate": "status='active'"},
                {"partition_id": 1, "predicate": "status='pending'"},
            ]

            # Single partition
            return [{"partition_id": 0}]
        """
        pass

    @abstractmethod
    def read_partition(
        self,
        partition_spec: Dict[str, Any],
        options: Dict[str, str]
    ) -> Iterator[pa.RecordBatch]:
        """
        Read Arrow batches for a single partition.

        This is called on each executor to read data for one partition.

        Args:
            partition_spec: Partition specification from plan_partitions()
            options: Dictionary of data source options

        Yields:
            pyarrow.RecordBatch objects

        Raises:
            Exception: If read fails

        Example:
            def read_partition(self, partition_spec, options):
                # Extract partition info
                lower = partition_spec.get("lower", 0)
                upper = partition_spec.get("upper", 1000)

                # Read data (using your backend)
                data = self.backend.read_range(lower, upper)

                # Convert to Arrow batches
                for batch in data.to_batches():
                    yield batch
        """
        pass

    def to_dataframe(
        self,
        spark: SparkSession,
        options: Dict[str, str]
    ) -> DataFrame:
        """
        Convert this data source to a Spark DataFrame.

        This method orchestrates the entire reading process:
        1. Infer schema
        2. Plan partitions
        3. Distribute read_partition() across executors
        4. Combine results into DataFrame

        Args:
            spark: SparkSession
            options: Dictionary of data source options

        Returns:
            Spark DataFrame

        Raises:
            Exception: If reading fails
        """
        from .data_source import (
            _arrow_schema_to_spark_schema,
            _arrow_type_to_spark_type
        )

        # Infer schema
        arrow_schema = self.infer_schema(options)
        spark_schema = _arrow_schema_to_spark_schema(arrow_schema)

        # Plan partitions
        partitions = self.plan_partitions(options)

        logger.info(f"Reading from {self.__class__.__name__} with {len(partitions)} partition(s)")

        # Broadcast options and partition specs to executors
        broadcast_options = spark.sparkContext.broadcast(options)
        broadcast_self = spark.sparkContext.broadcast(self)

        def partition_reader(partition_iter: Iterator[int]) -> Iterator[pa.RecordBatch]:
            """Read partitions on executors."""
            for partition_idx in partition_iter:
                opts = broadcast_options.value
                datasource = broadcast_self.value
                partition_spec = partitions[partition_idx]

                # Read partition as Arrow batches
                for batch in datasource.read_partition(partition_spec, opts):
                    yield batch

        # Create RDD of partition indices
        rdd = spark.sparkContext.parallelize(
            range(len(partitions)), numSlices=len(partitions)
        )

        # Map each partition to read Arrow batches
        arrow_rdd = rdd.mapPartitions(partition_reader)

        # Convert Arrow batches to Spark rows
        from pyspark.sql import Row

        def batches_to_rows(batch_iter: Iterator[pa.RecordBatch]) -> Iterator[Row]:
            """Convert Arrow batches to Spark Rows."""
            for batch in batch_iter:
                # Convert batch to pandas (efficient)
                df_pandas = batch.to_pandas()

                # Convert pandas rows to Spark Rows
                for row_dict in df_pandas.to_dict('records'):
                    yield Row(**row_dict)

        row_rdd = arrow_rdd.mapPartitions(batches_to_rows)

        # Create DataFrame
        df = spark.createDataFrame(row_rdd, schema=spark_schema)

        logger.info(f"Successfully created DataFrame from {self.__class__.__name__}")

        return df


class JDBCArrowDataSource(ArrowBatchDataSource):
    """
    JDBC data source implementation using ArrowBatchDataSource.

    This class wraps the existing JDBC backend infrastructure to work
    with the ArrowBatchDataSource interface.
    """

    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        """Infer Arrow schema from JDBC source."""
        from .data_source import infer_arrow_schema
        from .jdbc_options import NormalizedJDBCOptions

        # Normalize options
        normalized_opts = NormalizedJDBCOptions.from_spark_options(options)
        normalized_opts.validate()

        # Infer schema using existing implementation
        return infer_arrow_schema(normalized_opts)

    def plan_partitions(self, options: Dict[str, str]) -> List[Dict[str, Any]]:
        """Plan JDBC partitions."""
        from .jdbc_options import NormalizedJDBCOptions
        from .partition_planner import PartitionPlanner

        # Normalize options
        normalized_opts = NormalizedJDBCOptions.from_spark_options(options)
        normalized_opts.validate()

        # Plan partitions
        planner = PartitionPlanner(
            partition_column=normalized_opts.partition_column,
            lower_bound=normalized_opts.lower_bound,
            upper_bound=normalized_opts.upper_bound,
            num_partitions=normalized_opts.num_partitions,
            predicates=normalized_opts.predicates,
        )

        predicates = planner.generate_predicates()

        # Convert to partition specs
        return [
            {
                "partition_id": i,
                "predicate": pred,
            }
            for i, pred in enumerate(predicates)
        ]

    def read_partition(
        self,
        partition_spec: Dict[str, Any],
        options: Dict[str, str]
    ) -> Iterator[pa.RecordBatch]:
        """Read JDBC partition as Arrow batches."""
        from .jdbc_options import NormalizedJDBCOptions
        from .jdbc_url_parser import parse_jdbc_url
        from .query_builder import build_query_for_partition
        from .backends import get_backend

        # Normalize options
        normalized_opts = NormalizedJDBCOptions.from_spark_options(options)

        # Get backend
        backend = get_backend(normalized_opts.engine)

        # Parse JDBC URL
        parsed = parse_jdbc_url(
            normalized_opts.url,
            normalized_opts.user,
            normalized_opts.password
        )

        # Build query for this partition
        predicate = partition_spec.get("predicate")
        partition_query = build_query_for_partition(
            dbtable=normalized_opts.dbtable,
            query=normalized_opts.query,
            predicate=predicate
        )

        # Read Arrow batches
        batches = backend.read_batches(
            connection_string=parsed.connection_string,
            query=partition_query,
            fetch_size=normalized_opts.fetch_size
        )

        # Yield batches
        for batch in batches:
            yield batch
