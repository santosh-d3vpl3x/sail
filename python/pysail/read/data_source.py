"""Main JDBC data source implementation with Spark integration."""

import logging
import time
from typing import Dict, List, Iterator

import pyarrow as pa
from pyspark.sql import SparkSession, DataFrame

from .jdbc_options import NormalizedJDBCOptions
from .jdbc_url_parser import parse_jdbc_url, validate_driver_supported
from .partition_planner import PartitionPlanner
from .query_builder import build_query_for_partition, build_schema_inference_query
from .backends import get_backend
from .metrics import MetricsCollector, PartitionMetrics
from .exceptions import SchemaInferenceError, DatabaseError
from .utils import mask_credentials

logger = logging.getLogger("lakesail.jdbc")


def infer_arrow_schema(options: NormalizedJDBCOptions) -> pa.Schema:
    """
    Infer Arrow schema by fetching a single batch from backend.

    Returns Arrow schema as-is (no type conversion).

    Args:
        options: Normalized JDBC options

    Returns:
        Arrow schema

    Raises:
        SchemaInferenceError: If schema cannot be inferred
    """
    backend = get_backend(options.engine)

    # Parse JDBC URL
    parsed = parse_jdbc_url(options.url, options.user, options.password)
    validate_driver_supported(parsed.driver)

    # Build LIMIT 1 query
    limit_query = build_schema_inference_query(
        dbtable=options.dbtable,
        query=options.query
    )

    logger.info(f"Inferring schema from {mask_credentials(parsed.connection_string)}")
    logger.debug(f"Schema inference query: {limit_query}")

    try:
        # Fetch single batch
        batches = backend.read_batches(
            connection_string=parsed.connection_string,
            query=limit_query,
            fetch_size=1
        )

        if not batches or len(batches) == 0:
            raise SchemaInferenceError("Query returned no results for schema inference")

        # Return Arrow schema directly (preserve types from database)
        schema = batches[0].schema
        logger.info(f"Inferred schema with {len(schema)} columns: {schema.names}")

        return schema

    except DatabaseError as e:
        raise SchemaInferenceError(f"Failed to infer schema: {e}") from e


def read_jdbc(spark: SparkSession, options: Dict[str, str]) -> DataFrame:
    """
    Read JDBC table using mapPartitions with Arrow backend.

    This is the main entry point for JDBC reading. It:
    1. Normalizes and validates options
    2. Plans partitions
    3. Uses mapPartitions to distribute reads across executors
    4. Returns Spark DataFrame backed by Arrow

    Args:
        spark: SparkSession
        options: Dictionary of JDBC options

    Returns:
        Spark DataFrame

    Raises:
        InvalidOptionsError: If options are invalid
        SchemaInferenceError: If schema cannot be inferred
        DatabaseError: If read fails
    """
    # Parse and validate options
    normalized_opts = NormalizedJDBCOptions.from_spark_options(options)
    normalized_opts.validate()

    logger.info(f"Reading JDBC with engine: {normalized_opts.engine}")

    # Plan partitions
    planner = PartitionPlanner(
        partition_column=normalized_opts.partition_column,
        lower_bound=normalized_opts.lower_bound,
        upper_bound=normalized_opts.upper_bound,
        num_partitions=normalized_opts.num_partitions,
        predicates=normalized_opts.predicates,
    )
    predicates = planner.generate_predicates()

    logger.info(f"Planned {len(predicates)} partitions")

    # Infer Arrow schema (before distributing work)
    arrow_schema = infer_arrow_schema(normalized_opts)

    # Metrics collector
    metrics_collector = MetricsCollector()

    # Broadcast config to executors
    broadcast_opts = spark.sparkContext.broadcast(normalized_opts.url)
    broadcast_user = spark.sparkContext.broadcast(normalized_opts.user)
    broadcast_password = spark.sparkContext.broadcast(normalized_opts.password)
    broadcast_engine = spark.sparkContext.broadcast(normalized_opts.engine)
    broadcast_dbtable = spark.sparkContext.broadcast(normalized_opts.dbtable)
    broadcast_query = spark.sparkContext.broadcast(normalized_opts.query)
    broadcast_fetch_size = spark.sparkContext.broadcast(normalized_opts.fetch_size)

    def partition_reader(partition_iter: Iterator[int]) -> Iterator[pa.RecordBatch]:
        """
        Run on each executor; read one partition as Arrow batches.

        Args:
            partition_iter: Iterator of partition indices

        Yields:
            Arrow RecordBatches
        """
        for partition_idx in partition_iter:
            start_time = time.time()

            # Get broadcast values
            url = broadcast_opts.value
            user = broadcast_user.value
            password = broadcast_password.value
            engine = broadcast_engine.value
            dbtable = broadcast_dbtable.value
            query = broadcast_query.value
            fetch_size = broadcast_fetch_size.value

            try:
                # Get backend
                backend = get_backend(engine)

                # Parse JDBC URL
                parsed = parse_jdbc_url(url, user, password)

                # Get predicate for this partition
                predicate = predicates[partition_idx] if partition_idx < len(predicates) else None

                # Build query for this partition
                partition_query = build_query_for_partition(
                    dbtable=dbtable,
                    query=query,
                    predicate=predicate
                )

                logger.info(f"Partition {partition_idx} reading with predicate: {predicate}")
                logger.debug(f"Partition {partition_idx} query: {partition_query}")

                # Fetch Arrow RecordBatches from backend (native format)
                batches = backend.read_batches(
                    connection_string=parsed.connection_string,
                    query=partition_query,
                    fetch_size=fetch_size
                )

                # Calculate metrics
                row_count = sum(batch.num_rows for batch in batches)
                byte_count = sum(batch.nbytes for batch in batches)
                wall_time_ms = (time.time() - start_time) * 1000

                # Log metrics (executor-side logging)
                logger.info(
                    f"Partition {partition_idx}: {row_count:,} rows, "
                    f"{byte_count:,} bytes, {wall_time_ms:.1f}ms"
                )

                # Yield Arrow batches directly (Spark can consume these)
                for batch in batches:
                    yield batch

            except Exception as e:
                wall_time_ms = (time.time() - start_time) * 1000
                logger.error(f"Partition {partition_idx} failed: {e}")

                # Record failure metric
                # Note: This won't be collected on driver since we're on executor
                # But we log it for observability

                # Re-raise to fail the task
                raise

    # Create RDD of partition indices
    rdd = spark.sparkContext.parallelize(
        range(len(predicates)), numSlices=len(predicates)
    )

    # Map each partition to fetch Arrow batches
    # Using mapPartitions to maintain partition boundaries
    arrow_rdd = rdd.mapPartitions(partition_reader)

    # Create Spark DataFrame from Arrow (zero-copy interop)
    # Note: We need to convert Arrow batches to rows for Spark
    # Spark's createDataFrame can work with Arrow tables via pandas
    try:
        # Convert RDD of Arrow batches to DataFrame
        # We'll use Arrow's integration with Spark
        from pyspark.sql.types import StructType
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

        # Infer Spark schema from Arrow schema
        spark_schema = _arrow_schema_to_spark_schema(arrow_schema)

        # Create DataFrame
        df = spark.createDataFrame(row_rdd, schema=spark_schema)

        logger.info("JDBC read completed successfully")

        return df

    except Exception as e:
        logger.error(f"Failed to create DataFrame: {e}")
        raise DatabaseError(f"Failed to create DataFrame from Arrow batches: {e}") from e


def _arrow_schema_to_spark_schema(arrow_schema: pa.Schema):
    """
    Convert Arrow schema to Spark SQL schema.

    Args:
        arrow_schema: PyArrow schema

    Returns:
        Spark SQL StructType schema
    """
    from pyspark.sql.types import StructType, StructField
    import pyarrow.types as pat

    fields = []
    for field in arrow_schema:
        spark_type = _arrow_type_to_spark_type(field.type)
        spark_field = StructField(field.name, spark_type, nullable=field.nullable)
        fields.append(spark_field)

    return StructType(fields)


def _arrow_type_to_spark_type(arrow_type: pa.DataType):
    """
    Convert Arrow data type to Spark SQL type.

    Args:
        arrow_type: PyArrow data type

    Returns:
        Spark SQL data type
    """
    from pyspark.sql.types import (
        StringType, IntegerType, LongType, DoubleType, FloatType,
        BooleanType, BinaryType, DateType, TimestampType, DecimalType,
        ArrayType, StructType, StructField
    )
    import pyarrow.types as pat

    if pat.is_string(arrow_type) or pat.is_large_string(arrow_type):
        return StringType()
    elif pat.is_int8(arrow_type) or pat.is_int16(arrow_type) or pat.is_int32(arrow_type):
        return IntegerType()
    elif pat.is_int64(arrow_type):
        return LongType()
    elif pat.is_float32(arrow_type):
        return FloatType()
    elif pat.is_float64(arrow_type):
        return DoubleType()
    elif pat.is_boolean(arrow_type):
        return BooleanType()
    elif pat.is_binary(arrow_type) or pat.is_large_binary(arrow_type):
        return BinaryType()
    elif pat.is_date(arrow_type):
        return DateType()
    elif pat.is_timestamp(arrow_type):
        return TimestampType()
    elif pat.is_decimal(arrow_type):
        # Preserve precision and scale
        return DecimalType(precision=arrow_type.precision, scale=arrow_type.scale)
    elif pat.is_list(arrow_type) or pat.is_large_list(arrow_type):
        element_type = _arrow_type_to_spark_type(arrow_type.value_type)
        return ArrayType(element_type)
    elif pat.is_struct(arrow_type):
        fields = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            spark_field_type = _arrow_type_to_spark_type(field.type)
            fields.append(StructField(field.name, spark_field_type, nullable=field.nullable))
        return StructType(fields)
    else:
        # Default to string for unknown types
        logger.warning(f"Unknown Arrow type: {arrow_type}, defaulting to StringType")
        return StringType()
