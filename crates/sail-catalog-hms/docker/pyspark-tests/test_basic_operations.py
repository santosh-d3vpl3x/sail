"""
Test basic HMS operations with Kerberos authentication

Tests:
- Database operations (create, list, drop)
- Table operations (create, list, describe, drop)
- spark.table() API
- Basic SQL queries
"""

import pytest
from pyspark.sql.utils import AnalysisException


def test_create_and_list_databases(spark):
    """Test creating and listing databases"""
    # Create test database
    db_name = "test_list_db"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    # List databases
    databases = spark.sql("SHOW DATABASES").collect()
    db_names = [row.databaseName for row in databases]

    assert db_name in db_names, f"Database {db_name} not found in: {db_names}"

    # Cleanup
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")


def test_create_table(test_database, sample_data):
    """Test creating a table"""
    table_name = "employees"

    # Create table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Verify table exists
    tables = spark.sql("SHOW TABLES").collect()
    table_names = [row.tableName for row in tables]

    assert table_name in table_names, f"Table {table_name} not found in: {table_names}"


def test_spark_table_api(test_database, sample_data):
    """Test reading tables using spark.table() API"""
    table_name = "test_table_read"

    # Write data
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Read using spark.table()
    df = spark.table(table_name)

    # Verify data
    assert df.count() == 5, "Expected 5 rows"
    assert len(df.columns) == 4, "Expected 4 columns"
    assert "name" in df.columns, "Expected 'name' column"


def test_describe_table(test_database, sample_data):
    """Test describing table schema"""
    table_name = "test_describe"

    # Create table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Describe table
    schema = spark.sql(f"DESCRIBE {table_name}").collect()

    # Verify schema contains expected columns
    col_names = [row.col_name for row in schema]
    assert "id" in col_names
    assert "name" in col_names
    assert "age" in col_names
    assert "department" in col_names


def test_drop_table(test_database, sample_data):
    """Test dropping a table"""
    table_name = "test_drop"

    # Create table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Verify it exists
    df = spark.table(table_name)
    assert df.count() == 5

    # Drop table
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    # Verify it's gone
    with pytest.raises(AnalysisException):
        spark.table(table_name)


def test_table_properties(test_database, sample_data):
    """Test reading table properties and metadata"""
    table_name = "test_properties"

    # Create table with properties
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Get extended table info
    extended_info = spark.sql(f"DESCRIBE EXTENDED {table_name}").collect()

    # Verify we get some metadata
    assert len(extended_info) > 0, "Expected table metadata"

    # Check for database and table name in metadata
    info_str = "\n".join([f"{row.col_name}: {row.data_type}" for row in extended_info])
    assert test_database in info_str or table_name in info_str


def test_sql_select(test_database, sample_data):
    """Test SQL SELECT queries"""
    table_name = "test_select"

    # Create table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # SQL SELECT
    result = spark.sql(f"SELECT * FROM {table_name} WHERE age > 30").collect()

    assert len(result) == 2, "Expected 2 rows with age > 30"

    # Verify specific values
    names = [row.name for row in result]
    assert "Charlie" in names
    assert "Eve" in names


def test_sql_aggregation(test_database, sample_data):
    """Test SQL aggregation queries"""
    table_name = "test_aggregation"

    # Create table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # SQL aggregation
    result = spark.sql(f"""
        SELECT department, COUNT(*) as count, AVG(age) as avg_age
        FROM {table_name}
        GROUP BY department
        ORDER BY count DESC
    """).collect()

    assert len(result) == 3, "Expected 3 departments"

    # Verify Engineering department
    engineering = [row for row in result if row.department == "Engineering"][0]
    assert engineering["count"] == 3, "Expected 3 employees in Engineering"
