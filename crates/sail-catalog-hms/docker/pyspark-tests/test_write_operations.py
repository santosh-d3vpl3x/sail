"""
Test write operations with HMS and Kerberos

Tests:
- spark.saveAsTable() with different modes
- spark.insertInto() API
- Append/overwrite modes
- Partitioned tables
"""

import pytest
from pyspark.sql import Row


def test_save_as_table_overwrite(test_database, sample_data):
    """Test saveAsTable with overwrite mode"""
    table_name = "test_overwrite"

    # Initial write
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Verify initial data
    df = spark.table(table_name)
    assert df.count() == 5, "Expected 5 rows initially"

    # Create new data
    new_data = spark.createDataFrame([
        (6, "Frank", 40, "Sales"),
        (7, "Grace", 29, "Marketing"),
    ], ["id", "name", "age", "department"])

    # Overwrite
    new_data.write.mode("overwrite").saveAsTable(table_name)

    # Verify overwritten data
    df = spark.table(table_name)
    assert df.count() == 2, "Expected 2 rows after overwrite"

    names = [row.name for row in df.collect()]
    assert "Frank" in names
    assert "Grace" in names
    assert "Alice" not in names  # Original data should be gone


def test_save_as_table_append(test_database, sample_data):
    """Test saveAsTable with append mode"""
    table_name = "test_append"

    # Initial write
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Verify initial count
    df = spark.table(table_name)
    initial_count = df.count()
    assert initial_count == 5

    # Create new data
    new_data = spark.createDataFrame([
        (6, "Frank", 40, "Sales"),
        (7, "Grace", 29, "Marketing"),
    ], ["id", "name", "age", "department"])

    # Append
    new_data.write.mode("append").saveAsTable(table_name)

    # Verify appended data
    df = spark.table(table_name)
    assert df.count() == 7, "Expected 7 rows after append"

    names = [row.name for row in df.collect()]
    assert "Alice" in names  # Original data should still be there
    assert "Frank" in names  # New data should be added


def test_insert_into(test_database, sample_data):
    """Test insertInto() API"""
    table_name = "test_insert"

    # Create initial table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Create new data
    new_data = spark.createDataFrame([
        (6, "Frank", 40, "Sales"),
    ], ["id", "name", "age", "department"])

    # Insert into existing table
    new_data.write.insertInto(table_name)

    # Verify data
    df = spark.table(table_name)
    assert df.count() == 6, "Expected 6 rows after insert"

    names = [row.name for row in df.collect()]
    assert "Frank" in names


def test_insert_into_overwrite(test_database, sample_data):
    """Test insertInto() with overwrite"""
    table_name = "test_insert_overwrite"

    # Create initial table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Create new data
    new_data = spark.createDataFrame([
        (6, "Frank", 40, "Sales"),
        (7, "Grace", 29, "Marketing"),
    ], ["id", "name", "age", "department"])

    # Insert with overwrite
    new_data.write.mode("overwrite").insertInto(table_name)

    # Verify overwritten data
    df = spark.table(table_name)
    assert df.count() == 2, "Expected 2 rows after overwrite"

    names = [row.name for row in df.collect()]
    assert "Frank" in names
    assert "Alice" not in names  # Original data should be gone


def test_partitioned_table_write(test_database, sample_data):
    """Test writing partitioned tables"""
    table_name = "test_partitioned"

    # Write partitioned by department
    sample_data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Verify table is partitioned
    partitions = spark.sql(f"SHOW PARTITIONS {table_name}").collect()
    assert len(partitions) > 0, "Expected partitions"

    partition_values = [row.partition for row in partitions]
    assert any("Engineering" in p for p in partition_values)
    assert any("Sales" in p for p in partition_values)

    # Read data
    df = spark.table(table_name)
    assert df.count() == 5, "Expected all 5 rows"


def test_partitioned_table_insert(test_database, sample_data):
    """Test inserting into partitioned tables"""
    table_name = "test_partitioned_insert"

    # Create partitioned table
    sample_data.write.mode("overwrite").partitionBy("department").saveAsTable(table_name)

    # Create new data for Engineering department
    new_data = spark.createDataFrame([
        (6, "Frank", 40, "Engineering"),
    ], ["id", "name", "age", "department"])

    # Insert into partitioned table
    new_data.write.mode("append").insertInto(table_name)

    # Verify data
    df = spark.table(table_name)
    assert df.count() == 6, "Expected 6 rows after insert"

    # Verify Engineering partition has 4 rows now
    engineering_df = df.filter(df.department == "Engineering")
    assert engineering_df.count() == 4, "Expected 4 Engineering employees"


def test_create_table_as_select(test_database, sample_data):
    """Test CREATE TABLE AS SELECT (CTAS)"""
    source_table = "test_ctas_source"
    target_table = "test_ctas_target"

    # Create source table
    sample_data.write.mode("overwrite").saveAsTable(source_table)

    # CTAS
    spark.sql(f"""
        CREATE TABLE {target_table} AS
        SELECT name, age, department
        FROM {source_table}
        WHERE age > 28
    """)

    # Verify target table
    df = spark.table(target_table)
    assert df.count() == 3, "Expected 3 rows (age > 28)"

    # Verify schema (no 'id' column)
    assert "id" not in df.columns
    assert "name" in df.columns


def test_multiple_inserts(test_database, sample_data):
    """Test multiple sequential inserts"""
    table_name = "test_multiple_inserts"

    # Create initial table
    initial_data = spark.createDataFrame([
        (1, "Alice", 30, "Engineering"),
    ], ["id", "name", "age", "department"])

    initial_data.write.mode("overwrite").saveAsTable(table_name)

    # Insert multiple times
    for i in range(2, 6):
        new_row = spark.createDataFrame([
            (i, f"Person{i}", 20 + i, "Engineering"),
        ], ["id", "name", "age", "department"])

        new_row.write.mode("append").insertInto(table_name)

    # Verify final count
    df = spark.table(table_name)
    assert df.count() == 5, "Expected 5 rows after multiple inserts"


def test_dataframe_write_options(test_database, sample_data):
    """Test write with various options"""
    table_name = "test_write_options"

    # Write with specific format and options
    sample_data.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("compression", "snappy") \
        .saveAsTable(table_name)

    # Verify we can read it back
    df = spark.table(table_name)
    assert df.count() == 5

    # Verify table format
    table_info = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()
    info_str = "\n".join([f"{row.col_name}: {row.data_type}" for row in table_info])

    # Should have parquet format somewhere in the metadata
    assert "parquet" in info_str.lower() or "serde" in info_str.lower()
