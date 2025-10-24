"""
Spark adapter for ArrowBatchDataSource.

This module provides integration between Lakesail's Arrow data sources
and Apache Spark. It's the ONLY module that depends on PySpark.

Architecture:
    ArrowBatchDataSource (pure Python, no Spark)
          ↓
    SparkArrowAdapter (this module, has PySpark dependency)
          ↓
    Spark DataFrame

Usage:
    from pysail.read.arrow_datasource import JDBCArrowDataSource
    from pysail.read.spark_adapter import to_spark_dataframe

    # Create data source (no Spark dependency)
    datasource = JDBCArrowDataSource()
    options = {'url': '...', 'dbtable': '...'}

    # Convert to Spark DataFrame (requires Spark)
    df = to_spark_dataframe(spark, datasource, options)
"""

import logging
from typing import Dict, Iterator

import pyarrow as pa
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import (
    StringType, IntegerType, LongType, DoubleType, FloatType,
    BooleanType, BinaryType, DateType, TimestampType, DecimalType,
    ArrayType
)

from .arrow_datasource import ArrowBatchDataSource

logger = logging.getLogger("lakesail.spark_adapter")


def to_spark_dataframe(
    spark: SparkSession,
    datasource: ArrowBatchDataSource,
    options: Dict[str, str]
) -> DataFrame:
    """
    Convert Arrow data source to Spark DataFrame.

    This function:
    1. Infers schema from data source
    2. Plans partitions
    3. Uses mapPartitions to distribute reading
    4. Returns Spark DataFrame

    Args:
        spark: SparkSession
        datasource: ArrowBatchDataSource implementation
        options: Dictionary of data source options

    Returns:
        Spark DataFrame

    Example:
        from pysail.read.arrow_datasource import JDBCArrowDataSource
        from pysail.read.spark_adapter import to_spark_dataframe

        datasource = JDBCArrowDataSource()
        options = {
            'url': 'jdbc:postgresql://localhost/db',
            'dbtable': 'orders',
            'user': 'admin',
            'password': 'secret'
        }

        df = to_spark_dataframe(spark, datasource, options)
        df.show()
    """
    # Infer Arrow schema
    arrow_schema = datasource.infer_schema(options)
    spark_schema = _arrow_schema_to_spark_schema(arrow_schema)

    # Plan partitions
    partitions = datasource.plan_partitions(options)

    logger.info(
        f"Converting {datasource.__class__.__name__} to Spark DataFrame "
        f"with {len(partitions)} partition(s)"
    )

    # Broadcast configuration (NOT objects - just JSON-serializable config)
    broadcast_options = spark.sparkContext.broadcast(options)
    broadcast_partitions = spark.sparkContext.broadcast(partitions)

    # Broadcast datasource class name (we'll recreate on executors)
    datasource_class_name = datasource.__class__.__name__
    datasource_module = datasource.__class__.__module__
    broadcast_datasource_info = spark.sparkContext.broadcast({
        'class_name': datasource_class_name,
        'module': datasource_module
    })

    def partition_reader(partition_iter: Iterator[int]) -> Iterator[Row]:
        """Read partitions on executors."""
        # Import here to recreate on executor
        import importlib

        for partition_idx in partition_iter:
            opts = broadcast_options.value
            part_specs = broadcast_partitions.value
            ds_info = broadcast_datasource_info.value

            # Recreate datasource on executor (avoid pickling issues)
            module = importlib.import_module(ds_info['module'])
            datasource_class = getattr(module, ds_info['class_name'])
            datasource_instance = datasource_class()

            # Get partition spec
            partition_spec = part_specs[partition_idx]

            # Read partition as Arrow batches
            for batch in datasource_instance.read_partition(partition_spec, opts):
                # Convert batch to pandas (efficient)
                df_pandas = batch.to_pandas()

                # Convert pandas rows to Spark Rows
                for row_dict in df_pandas.to_dict('records'):
                    yield Row(**row_dict)

    # Create RDD of partition indices
    rdd = spark.sparkContext.parallelize(
        range(len(partitions)), numSlices=len(partitions)
    )

    # Map each partition to read data
    row_rdd = rdd.mapPartitions(partition_reader)

    # Create DataFrame
    df = spark.createDataFrame(row_rdd, schema=spark_schema)

    logger.info(f"Successfully created Spark DataFrame from {datasource_class_name}")

    return df


def _arrow_schema_to_spark_schema(arrow_schema: pa.Schema) -> StructType:
    """
    Convert Arrow schema to Spark SQL schema.

    Args:
        arrow_schema: PyArrow schema

    Returns:
        Spark SQL StructType schema
    """
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
