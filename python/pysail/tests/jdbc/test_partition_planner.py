"""Unit tests for partition planner."""

import pytest
from pysail.jdbc.partition_planner import PartitionPlanner


class TestPartitionPlanner:
    """Test partition predicate generation."""

    def test_no_partitioning(self):
        """Test single partition (no WHERE clause)."""
        planner = PartitionPlanner()
        predicates = planner.generate_predicates()

        assert predicates == [None]

    def test_explicit_predicates(self):
        """Test explicit predicates list."""
        planner = PartitionPlanner(
            predicates=["status='active'", "status='pending'", "status='completed'"]
        )
        predicates = planner.generate_predicates()

        assert len(predicates) == 3
        assert predicates[0] == "status='active'"
        assert predicates[1] == "status='pending'"
        assert predicates[2] == "status='completed'"

    def test_range_partitioning_basic(self):
        """Test basic range partitioning."""
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=0,
            upper_bound=100,
            num_partitions=4
        )
        predicates = planner.generate_predicates()

        assert len(predicates) == 4
        assert '"id" >= 0 AND "id" < 25' in predicates[0]
        assert '"id" >= 25 AND "id" < 50' in predicates[1]
        assert '"id" >= 50 AND "id" < 75' in predicates[2]
        assert '"id" >= 75 AND "id" < 101' in predicates[3]  # Last partition includes upper

    def test_range_partitioning_single_partition(self):
        """Test range partitioning with numPartitions=1."""
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=1,
            upper_bound=100,
            num_partitions=1
        )
        predicates = planner.generate_predicates()

        assert len(predicates) == 1
        assert '"id" >= 1 AND "id" < 101' in predicates[0]

    def test_range_partitioning_small_range(self):
        """Test range partitioning with range < numPartitions."""
        # Range is 10, but requesting 20 partitions
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=0,
            upper_bound=10,
            num_partitions=20
        )
        predicates = planner.generate_predicates()

        # Should collapse to min(20, 10) = 10 partitions
        assert len(predicates) == 10

    def test_range_partitioning_zero_range(self):
        """Test range partitioning with zero range."""
        # Range is 0 (lower == upper)
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=100,
            upper_bound=100,
            num_partitions=10
        )
        predicates = planner.generate_predicates()

        # Should collapse to 1 partition
        assert len(predicates) == 1

    def test_range_partitioning_uneven_split(self):
        """Test range partitioning with uneven split."""
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=0,
            upper_bound=10,
            num_partitions=3
        )
        predicates = planner.generate_predicates()

        assert len(predicates) == 3
        # stride = 10 // 3 = 3
        assert '"id" >= 0 AND "id" < 3' in predicates[0]
        assert '"id" >= 3 AND "id" < 6' in predicates[1]
        assert '"id" >= 6 AND "id" < 11' in predicates[2]  # Last includes remainder

    def test_range_partitioning_quoted_column(self):
        """Test that column names are properly quoted."""
        planner = PartitionPlanner(
            partition_column="my_column",
            lower_bound=0,
            upper_bound=100,
            num_partitions=2
        )
        predicates = planner.generate_predicates()

        # Column should be quoted
        assert '"my_column"' in predicates[0]
        assert '"my_column"' in predicates[1]

    def test_range_partitioning_with_negative_bounds(self):
        """Test range partitioning with negative bounds."""
        planner = PartitionPlanner(
            partition_column="temperature",
            lower_bound=-100,
            upper_bound=100,
            num_partitions=4
        )
        predicates = planner.generate_predicates()

        assert len(predicates) == 4
        assert '"temperature" >= -100 AND "temperature" < -50' in predicates[0]
        assert '"temperature" >= -50 AND "temperature" < 0' in predicates[1]
        assert '"temperature" >= 0 AND "temperature" < 50' in predicates[2]
        assert '"temperature" >= 50 AND "temperature" < 101' in predicates[3]

    def test_missing_bounds_raises_error(self):
        """Test that missing bounds with partition_column raises error."""
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=None,
            upper_bound=100,
            num_partitions=4
        )

        with pytest.raises(ValueError, match="requires lower_bound and upper_bound"):
            planner.generate_predicates()

    def test_explicit_predicates_override_range(self):
        """Test that explicit predicates override range partitioning."""
        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=0,
            upper_bound=100,
            num_partitions=4,
            predicates=["status='active'", "status='inactive'"]
        )
        predicates = planner.generate_predicates()

        # Explicit predicates should win
        assert len(predicates) == 2
        assert predicates[0] == "status='active'"
        assert predicates[1] == "status='inactive'"
