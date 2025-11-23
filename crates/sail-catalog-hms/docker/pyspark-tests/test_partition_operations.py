"""
Test partition operations with HMS and Kerberos

Tests:
- Creating partitioned tables
- Adding/dropping partitions
- Partition pruning
- Dynamic partitioning
"""

import pytest


def test_create_partitioned_table(test_database, sample_data):
    """Test creating a partitioned table"""
    table_name = "test_partitioned"

    # Create partitioned table
    sample_data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Show partitions
    partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()

    # Should have partitions for each department
    partition_values = [row.partition for row in partitions]
    assert len(partition_values) == 3, f"Expected 3 partitions, got: {partition_values}"

    assert any("Engineering" in p for p in partition_values)
    assert any("Sales" in p for p in partition_values)
    assert any("Marketing" in p for p in partition_values)


def test_partition_pruning(test_database, sample_data):
    """Test that partition pruning works correctly"""
    table_name = "test_pruning"

    # Create partitioned table
    sample_data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Query specific partition
    df = spark.sql(f"SELECT * FROM {table_name} WHERE department = 'Engineering'")

    result = df.collect()
    assert len(result) == 3, "Expected 3 Engineering employees"

    # Verify all results are from Engineering
    for row in result:
        assert row.department == "Engineering"


def test_multi_level_partitioning(test_database):
    """Test multi-level partitioned tables"""
    table_name = "test_multi_partition"

    # Create data with year and month
    data = [
        (1, "Alice", 2024, 1),
        (2, "Bob", 2024, 1),
        (3, "Charlie", 2024, 2),
        (4, "Diana", 2024, 2),
        (5, "Eve", 2023, 12),
    ]

    df = spark.createDataFrame(data, ["id", "name", "year", "month"])

    # Create partitioned by year and month
    df.write.mode("overwrite").partitionBy("year", "month").saveAsTable(table_name)

    # Show partitions
    partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()

    # Should have year/month hierarchy
    assert len(partitions) == 3, f"Expected 3 unique year/month combinations"

    # Verify partition format
    partition_strs = [row.partition for row in partitions]
    assert any("year=2024/month=1" in p for p in partition_strs)
    assert any("year=2024/month=2" in p for p in partition_strs)
    assert any("year=2023/month=12" in p for p in partition_strs)


def test_add_partition_manually(test_database):
    """Test manually adding partitions"""
    table_name = "test_manual_partition"

    # Create initial partitioned table
    initial_data = spark.createDataFrame([
        (1, "Alice", "Engineering"),
    ], ["id", "name", "department"])

    initial_data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Note: Manual partition addition requires a location
    # This is more of an HMS administrative operation
    # For dynamic partition insertion, use regular insert

    # Add data for new partition dynamically
    new_data = spark.createDataFrame([
        (2, "Bob", "Research"),
    ], ["id", "name", "department"])

    new_data.write.mode("append").insertInto(table_name)

    # Verify partitions
    partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
    partition_values = [row.partition for row in partitions]

    assert len(partition_values) == 2, "Expected 2 partitions"
    assert any("Engineering" in p for p in partition_values)
    assert any("Research" in p for p in partition_values)


def test_drop_partition(test_database, sample_data):
    """Test dropping specific partitions"""
    table_name = "test_drop_partition"

    # Create partitioned table
    sample_data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Verify initial partitions
    initial_partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
    assert len(initial_partitions) == 3

    # Drop Sales partition
    spark.sql(f"ALTER TABLE {table_name} DROP IF EXISTS PARTITION (department='Sales')")

    # Verify partition is dropped
    remaining_partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
    partition_values = [row.partition for row in remaining_partitions]

    assert len(partition_values) == 2, "Expected 2 partitions after drop"
    assert not any("Sales" in p for p in partition_values)

    # Verify data is also gone
    df = spark.table(table_name)
    departments = [row.department for row in df.collect()]
    assert "Sales" not in departments


def test_dynamic_partition_insert(test_database):
    """Test dynamic partition insertion"""
    table_name = "test_dynamic_partition"

    # Create initial empty partitioned table
    schema_df = spark.createDataFrame([
        (1, "Alice", "Engineering"),
    ], ["id", "name", "department"])

    schema_df.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Clear the table
    spark.sql(f"TRUNCATE TABLE {table_name}")

    # Insert data with multiple partitions at once
    new_data = spark.createDataFrame([
        (1, "Alice", "Engineering"),
        (2, "Bob", "Sales"),
        (3, "Charlie", "Marketing"),
        (4, "Diana", "Engineering"),
    ], ["id", "name", "department"])

    # Enable dynamic partitioning
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

    new_data.write.mode("append").insertInto(table_name)

    # Verify all partitions were created
    partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
    assert len(partitions) == 3, "Expected 3 partitions from dynamic insert"

    # Verify data
    df = spark.table(table_name)
    assert df.count() == 4


def test_partition_column_ordering(test_database):
    """Test that partition columns appear at the end of schema"""
    table_name = "test_partition_order"

    data = spark.createDataFrame([
        (1, "Alice", 30, "Engineering"),
    ], ["id", "name", "age", "department"])

    # Create table partitioned by department
    data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Read table and check column order
    df = spark.table(table_name)
    columns = df.columns

    # Partition column should be last
    assert columns[-1] == "department", f"Expected 'department' last, got: {columns}"


def test_partition_filter_pushdown(test_database, sample_data):
    """Test that partition filters are pushed down"""
    table_name = "test_filter_pushdown"

    # Create partitioned table
    sample_data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Query with partition filter
    df = spark.sql(f"""
        SELECT name, age
        FROM {table_name}
        WHERE department IN ('Engineering', 'Sales')
    """)

    result = df.collect()

    # Should only return Engineering and Sales employees
    assert len(result) == 4, "Expected 4 rows (3 Engineering + 1 Sales)"

    # Verify no Marketing employees
    names = [row.name for row in result]
    assert "Diana" not in names  # Diana is in Marketing


def test_partitioned_table_stats(test_database, sample_data):
    """Test getting statistics for partitioned tables"""
    table_name = "test_partition_stats"

    # Create partitioned table
    sample_data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Get table statistics
    stats = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()

    # Verify we get partition information
    stats_str = "\n".join([f"{row.col_name}: {row.data_type}" for row in stats])

    # Should mention partitions somewhere
    assert "partition" in stats_str.lower() or "department" in stats_str.lower()
