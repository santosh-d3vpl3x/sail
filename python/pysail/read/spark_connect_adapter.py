"""
Spark Connect-compatible adapter for ArrowBatchDataSource.

This module provides Spark Connect integration WITHOUT using:
- spark.sparkContext (not available in Spark Connect)
- RDD APIs (not available in Spark Connect)
- Broadcast variables (not available in Spark Connect)

Instead, it uses only DataFrame APIs that work with Spark Connect.

Architecture:
    ArrowBatchDataSource (pure Python, no Spark)
          ↓
    Spark Connect Compatible Adapter (this module)
          ↓
    Spark DataFrame (via Spark Connect)

Usage:
    from pysail.read.arrow_datasource import JDBCArrowDataSource
    from pysail.read.spark_connect_adapter import to_spark_dataframe

    # Works with both regular Spark AND Spark Connect!
    spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

    datasource = JDBCArrowDataSource()
    options = {'url': '...', 'dbtable': '...'}

    df = to_spark_dataframe(spark, datasource, options)
"""

import logging
from typing import Dict

import pyarrow as pa

logger = logging.getLogger("lakesail.spark_connect_adapter")


def to_spark_dataframe(spark, datasource, options: Dict[str, str]):
    """
    Convert Arrow data source to Spark DataFrame (Spark Connect compatible).

    This version works with Spark Connect by:
    1. Reading ALL data on the CLIENT side (not distributed)
    2. Using spark.createDataFrame() to send to server
    3. Letting Spark handle distribution on the server

    This is simpler than the RDD-based approach and works with Spark Connect!

    Args:
        spark: SparkSession (can be Spark Connect remote session)
        datasource: ArrowBatchDataSource implementation
        options: Dictionary of data source options

    Returns:
        Spark DataFrame

    Note:
        This approach reads data on the client and sends to Spark.
        For truly distributed reads (reading on executors), you need
        to deploy your datasource code to the Spark server and use UDFs.

    Examples:
        # Regular Spark
        spark = SparkSession.builder.getOrCreate()
        df = to_spark_dataframe(spark, datasource, options)

        # Spark Connect
        spark = SparkSession.builder.remote("sc://host:15002").getOrCreate()
        df = to_spark_dataframe(spark, datasource, options)  # Works!
    """
    from .arrow_datasource import ArrowBatchDataSource

    logger.info(
        f"Converting {datasource.__class__.__name__} to Spark DataFrame "
        f"(Spark Connect compatible)"
    )

    # Read Arrow table on client
    arrow_table = datasource.to_arrow_table(options)

    logger.info(
        f"Read {arrow_table.num_rows:,} rows, "
        f"{arrow_table.nbytes:,} bytes from {datasource.__class__.__name__}"
    )

    # Convert to pandas (Spark Connect supports this!)
    pandas_df = arrow_table.to_pandas()

    # Send to Spark (works with Spark Connect)
    # Spark will handle distribution automatically
    df = spark.createDataFrame(pandas_df)

    logger.info(
        f"Successfully created Spark DataFrame "
        f"({df.count() if logger.isEnabledFor(logging.DEBUG) else 'unknown'} rows)"
    )

    return df


def to_spark_dataframe_distributed(spark, datasource, options: Dict[str, str]):
    """
    EXPERIMENTAL: Distributed Spark Connect adapter using server-side UDFs.

    This attempts to do distributed reads with Spark Connect by:
    1. Registering datasource as UDF on server
    2. Creating partition metadata DataFrame
    3. Using UDF to read each partition on executors

    WARNING: This requires deploying pysail to Spark server!

    Args:
        spark: SparkSession (Spark Connect)
        datasource: ArrowBatchDataSource implementation
        options: Dictionary of data source options

    Returns:
        Spark DataFrame

    Limitations:
        - Requires pysail installed on Spark server
        - More complex than simple approach
        - May not work with all Spark Connect setups
    """
    logger.warning(
        "Distributed Spark Connect adapter is experimental. "
        "Requires pysail deployed to Spark server."
    )

    # Plan partitions
    partitions = datasource.plan_partitions(options)

    logger.info(f"Planned {len(partitions)} partitions for distributed read")

    # Create DataFrame with partition metadata
    # Each row represents one partition to read
    import pandas as pd

    partition_df = pd.DataFrame([
        {
            'partition_id': i,
            'partition_spec': str(spec),  # JSON string
            'options': str(options),      # JSON string
        }
        for i, spec in enumerate(partitions)
    ])

    partitions_spark_df = spark.createDataFrame(partition_df)

    # Register UDF that reads one partition
    # This runs ON THE SPARK SERVER
    from pyspark.sql.functions import pandas_udf
    import pandas as pd
    import pyarrow as pa

    @pandas_udf("string")  # Returns Arrow table as string (hack)
    def read_partition_udf(partition_specs: pd.Series, opts: pd.Series) -> pd.Series:
        """
        Read partition on Spark executor.

        This UDF runs on the Spark server, so pysail must be installed there!
        """
        import json
        from pysail.read.arrow_datasource import JDBCArrowDataSource

        results = []
        for spec_str, opt_str in zip(partition_specs, opts):
            spec = json.loads(spec_str)
            options_dict = json.loads(opt_str)

            datasource = JDBCArrowDataSource()

            # Read partition
            batches = list(datasource.read_partition(spec, options_dict))

            # Convert to table
            table = pa.Table.from_batches(batches)

            # Serialize as string (hack - not efficient!)
            results.append(table.to_pandas().to_json())

        return pd.Series(results)

    # Apply UDF to read each partition on executors
    result_df = partitions_spark_df.withColumn(
        'data',
        read_partition_udf('partition_spec', 'options')
    )

    # Collect results and parse
    # (This is inefficient - better to use Arrow Flight in production)
    collected = result_df.select('data').collect()

    import json
    all_dfs = []
    for row in collected:
        data_json = row['data']
        pdf = pd.read_json(data_json)
        all_dfs.append(pdf)

    combined_pdf = pd.concat(all_dfs, ignore_index=True)

    # Send back to Spark
    final_df = spark.createDataFrame(combined_pdf)

    logger.info("Distributed read completed via UDFs")

    return final_df


# Alias for backward compatibility
to_spark_dataframe_connect = to_spark_dataframe
