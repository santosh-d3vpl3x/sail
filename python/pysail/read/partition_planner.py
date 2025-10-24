"""Partition planning for distributed JDBC reads."""

import logging
from typing import List, Optional

from .query_builder import build_partition_predicate

logger = logging.getLogger("lakesail.jdbc")


class PartitionPlanner:
    """Generate partition predicates for distributed reading."""

    def __init__(
        self,
        partition_column: Optional[str] = None,
        lower_bound: Optional[int] = None,
        upper_bound: Optional[int] = None,
        num_partitions: int = 1,
        predicates: Optional[List[str]] = None,
    ):
        """
        Initialize partition planner.

        Args:
            partition_column: Column for range partitioning
            lower_bound: Lower bound (inclusive)
            upper_bound: Upper bound (inclusive)
            num_partitions: Number of partitions
            predicates: Explicit predicates (overrides range partitioning)
        """
        self.partition_column = partition_column
        self.lower_bound = lower_bound
        self.upper_bound = upper_bound
        self.num_partitions = num_partitions
        self.predicates = predicates

    def generate_predicates(self) -> List[Optional[str]]:
        """
        Return list of WHERE clause predicates (one per partition).

        Returns:
            List of predicates (None means no WHERE clause for that partition)
        """
        # Option 1: Explicit predicates list
        if self.predicates:
            logger.info(f"Using {len(self.predicates)} explicit predicates")
            return self.predicates

        # Option 2: Range partitioning
        if self.partition_column:
            if self.lower_bound is None or self.upper_bound is None:
                raise ValueError("partition_column requires lower_bound and upper_bound")

            return self._generate_range_predicates()

        # Option 3: Single partition (no WHERE)
        logger.info("No partitioning specified, using single partition")
        return [None]

    def _generate_range_predicates(self) -> List[str]:
        """
        Generate range-based partition predicates.

        Returns:
            List of WHERE clause predicates
        """
        col = self.partition_column
        lower = self.lower_bound
        upper = self.upper_bound
        num_parts = self.num_partitions

        data_range = upper - lower

        # Edge case: collapse to 1 partition if range < numPartitions
        adjusted_parts = min(num_parts, max(1, data_range))

        # Warn if we had to reduce partition count
        if adjusted_parts < num_parts:
            logger.warning(
                f"Adjusted numPartitions from {num_parts} to {adjusted_parts} "
                f"due to small range ({lower}-{upper}, span={data_range}). "
                f"Some executors will have no work."
            )

        if adjusted_parts == 1:
            # Single partition: return one predicate covering entire range
            pred = build_partition_predicate(col, lower, upper + 1)
            logger.info(f"Single partition with predicate: {pred}")
            return [pred]

        stride = data_range // adjusted_parts
        predicates = []

        for i in range(adjusted_parts):
            start = lower + (i * stride)

            # Last partition includes upper bound
            if i == adjusted_parts - 1:
                end = upper + 1  # Inclusive upper bound
            else:
                end = start + stride

            pred = build_partition_predicate(col, start, end)
            predicates.append(pred)

        logger.info(
            f"Generated {len(predicates)} range partitions on column '{col}' "
            f"({lower}-{upper}, stride={stride})"
        )

        return predicates
