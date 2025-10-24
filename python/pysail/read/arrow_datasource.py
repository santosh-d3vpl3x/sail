"""
ArrowBatchDataSource - Lakesail's framework-agnostic data source abstraction.

This module provides a pure Python abstraction for Arrow-native data sources.
It has NO dependencies on any specific framework (PySpark, Polars, etc.).

The abstraction can be used to:
- Implement various data sources (JDBC, REST APIs, S3, etc.)
- Integrate with multiple frameworks via adapters
- Use Arrow data directly without framework overhead

Architecture:

    Pure Python Layer (this module):
    ┌─────────────────────────────────────┐
    │  ArrowBatchDataSource (abstract)    │
    │  - infer_schema() → Arrow Schema    │
    │  - plan_partitions() → Specs        │
    │  - read_partition() → RecordBatches │
    └─────────────────────────────────────┘
                    ▲
                    │ extends
                    │
    ┌─────────────────────────────────────┐
    │  JDBCArrowDataSource                │
    │  PostgreSQLDataSource               │
    │  RESTApiDataSource                  │
    │  ... (any data source)              │
    └─────────────────────────────────────┘

    Framework Adapters (separate modules):
    ┌─────────────────────────────────────┐
    │  SparkArrowAdapter                  │
    │  PolarsArrowAdapter                 │
    │  DuckDBArrowAdapter                 │
    │  ... (any framework)                │
    └─────────────────────────────────────┘

Example - Pure Arrow usage (no framework):
    from pysail.read.arrow_datasource import JDBCArrowDataSource

    datasource = JDBCArrowDataSource()
    options = {"url": "jdbc:postgresql://...", "dbtable": "orders"}

    # Get Arrow schema
    schema = datasource.infer_schema(options)

    # Get partition specs
    partitions = datasource.plan_partitions(options)

    # Read data as Arrow batches
    for partition_spec in partitions:
        for batch in datasource.read_partition(partition_spec, options):
            # Work with Arrow RecordBatch directly
            print(batch.to_pandas())

Example - With Spark (via adapter):
    from pysail.read.spark_adapter import to_spark_dataframe

    datasource = JDBCArrowDataSource()
    df = to_spark_dataframe(spark, datasource, options)

Example - With Polars (via adapter):
    from pysail.read.polars_adapter import to_polars_dataframe

    datasource = JDBCArrowDataSource()
    df = to_polars_dataframe(datasource, options)
"""

from abc import ABC, abstractmethod
from typing import Dict, List, Iterator, Any
import logging

import pyarrow as pa

logger = logging.getLogger("lakesail.arrow_datasource")


class ArrowBatchDataSource(ABC):
    """
    Abstract base class for Arrow-native data sources.

    This is a pure abstraction with NO framework dependencies.
    Data sources implement this interface, then framework adapters
    can convert the Arrow data to framework-specific formats.

    Implementing a new data source:
        1. Subclass ArrowBatchDataSource
        2. Implement infer_schema() to return Arrow schema
        3. Implement plan_partitions() to generate partition specs
        4. Implement read_partition() to read Arrow batches

    Example:
        class MyDataSource(ArrowBatchDataSource):
            def infer_schema(self, options):
                # Return pyarrow.Schema
                return pa.schema([
                    ('id', pa.int64()),
                    ('name', pa.string())
                ])

            def plan_partitions(self, options):
                # Return partition specs
                return [
                    {'partition_id': 0, 'range': '0-100'},
                    {'partition_id': 1, 'range': '100-200'},
                ]

            def read_partition(self, partition_spec, options):
                # Read and yield Arrow batches
                data = self.fetch_data(partition_spec['range'])
                yield pa.RecordBatch.from_pandas(data)
    """

    @abstractmethod
    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        """
        Infer Arrow schema from data source.

        This is called to determine the schema before reading data.
        Should be fast (e.g., read only first row or use metadata).

        Args:
            options: Dictionary of data source options

        Returns:
            pyarrow.Schema describing the data

        Raises:
            Exception: If schema cannot be inferred

        Example:
            schema = datasource.infer_schema({
                'url': 'jdbc:postgresql://localhost/db',
                'dbtable': 'orders'
            })
            # schema: pa.schema([('id', pa.int64()), ...])
        """
        pass

    @abstractmethod
    def plan_partitions(self, options: Dict[str, str]) -> List[Dict[str, Any]]:
        """
        Plan partitions for distributed/parallel reading.

        Each partition spec is a dictionary containing information
        needed to read that partition. The specs are serializable
        (JSON-compatible types only: str, int, float, bool, list, dict).

        Args:
            options: Dictionary of data source options

        Returns:
            List of partition specifications (dicts)

        Example partition specs:
            # Range-based partitioning
            [
                {'partition_id': 0, 'lower': 0, 'upper': 100},
                {'partition_id': 1, 'lower': 100, 'upper': 200},
            ]

            # Predicate-based partitioning
            [
                {'partition_id': 0, 'filter': "status='active'"},
                {'partition_id': 1, 'filter': "status='pending'"},
            ]

            # File-based partitioning
            [
                {'partition_id': 0, 'file': 's3://bucket/data/part-0.parquet'},
                {'partition_id': 1, 'file': 's3://bucket/data/part-1.parquet'},
            ]

            # Single partition (no parallelism)
            [
                {'partition_id': 0}
            ]
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

        This method will be called (potentially in parallel) for each
        partition spec returned by plan_partitions().

        Args:
            partition_spec: Partition specification from plan_partitions()
            options: Dictionary of data source options

        Yields:
            pyarrow.RecordBatch objects

        Raises:
            Exception: If read fails

        Example:
            for batch in datasource.read_partition(
                {'partition_id': 0, 'lower': 0, 'upper': 100},
                {'url': '...', 'dbtable': '...'}
            ):
                # batch is pa.RecordBatch
                print(f"Read {batch.num_rows} rows")
        """
        pass

    def read_all(self, options: Dict[str, str]) -> Iterator[pa.RecordBatch]:
        """
        Convenience method to read all partitions sequentially.

        This is useful for:
        - Testing
        - Small datasets
        - Single-threaded processing
        - Direct Arrow usage without framework

        Args:
            options: Dictionary of data source options

        Yields:
            pyarrow.RecordBatch objects from all partitions

        Example:
            datasource = MyDataSource()
            for batch in datasource.read_all({'url': '...', 'dbtable': '...'}):
                df_pandas = batch.to_pandas()
                print(df_pandas)
        """
        partitions = self.plan_partitions(options)
        for partition_spec in partitions:
            for batch in self.read_partition(partition_spec, options):
                yield batch

    def to_arrow_table(self, options: Dict[str, str]) -> pa.Table:
        """
        Read all data as a single Arrow Table.

        Convenience method for small datasets or testing.

        Args:
            options: Dictionary of data source options

        Returns:
            pyarrow.Table with all data

        Warning:
            This materializes all data in memory. For large datasets,
            use read_all() or a framework adapter instead.

        Example:
            datasource = MyDataSource()
            table = datasource.to_arrow_table({'url': '...', 'dbtable': '...'})
            print(table.to_pandas())
        """
        batches = list(self.read_all(options))
        if not batches:
            # Empty result - return empty table with schema
            schema = self.infer_schema(options)
            return pa.Table.from_batches([], schema=schema)
        return pa.Table.from_batches(batches)


class JDBCArrowDataSource(ArrowBatchDataSource):
    """
    JDBC data source implementation.

    This implementation is pure Python with no Spark dependencies.
    It uses the existing JDBC backend infrastructure.

    Example usage without any framework:
        datasource = JDBCArrowDataSource()
        options = {
            'url': 'jdbc:postgresql://localhost:5432/mydb',
            'dbtable': 'orders',
            'user': 'admin',
            'password': 'secret'
        }

        # Get schema
        schema = datasource.infer_schema(options)
        print(schema)

        # Read all data as Arrow Table
        table = datasource.to_arrow_table(options)
        print(table.to_pandas())

    Example with partitioning:
        options = {
            'url': 'jdbc:postgresql://localhost:5432/mydb',
            'dbtable': 'orders',
            'partitionColumn': 'order_id',
            'lowerBound': '1',
            'upperBound': '1000',
            'numPartitions': '4'
        }

        # Process partitions in parallel (using any parallel framework)
        partitions = datasource.plan_partitions(options)
        for partition_spec in partitions:
            for batch in datasource.read_partition(partition_spec, options):
                process(batch)
    """

    def infer_schema(self, options: Dict[str, str]) -> pa.Schema:
        """Infer Arrow schema from JDBC source."""
        from .jdbc_options import NormalizedJDBCOptions
        from .jdbc_url_parser import parse_jdbc_url, validate_driver_supported
        from .query_builder import build_schema_inference_query
        from .backends import get_backend
        from .exceptions import SchemaInferenceError, DatabaseError
        from .utils import mask_credentials

        # Normalize options
        normalized_opts = NormalizedJDBCOptions.from_spark_options(options)
        normalized_opts.validate()

        # Get backend
        backend = get_backend(normalized_opts.engine)

        # Parse JDBC URL
        parsed = parse_jdbc_url(normalized_opts.url, normalized_opts.user, normalized_opts.password)
        validate_driver_supported(parsed.driver)

        # Build LIMIT 1 query
        limit_query = build_schema_inference_query(
            dbtable=normalized_opts.dbtable,
            query=normalized_opts.query
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

        # Convert to partition specs (JSON-serializable)
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
