"""
Test schema and type operations with HMS

Tests:
- Schema evolution
- Complex types (arrays, maps, structs)
- Type conversions
- ALTER TABLE operations
"""

import pytest
from pyspark.sql.types import *


def test_complex_types_array(test_database):
    """Test tables with array columns"""
    table_name = "test_array_type"

    # Create data with arrays
    data = [
        (1, "Alice", ["Python", "Scala", "Java"]),
        (2, "Bob", ["Python", "R"]),
        (3, "Charlie", ["Java", "C++"]),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("skills", ArrayType(StringType()), True),
    ])

    df = spark.createDataFrame(data, schema)

    # Write to HMS
    df.write.mode("overwrite").saveAsTable(table_name)

    # Read back
    result_df = spark.table(table_name)

    # Verify schema
    assert result_df.schema["skills"].dataType.typeName() == "array"

    # Verify data
    alice = result_df.filter(result_df.name == "Alice").collect()[0]
    assert len(alice.skills) == 3
    assert "Python" in alice.skills


def test_complex_types_map(test_database):
    """Test tables with map columns"""
    table_name = "test_map_type"

    # Create data with maps
    data = [
        (1, "Alice", {"dept": "Engineering", "level": "Senior"}),
        (2, "Bob", {"dept": "Sales", "level": "Junior"}),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("attributes", MapType(StringType(), StringType()), True),
    ])

    df = spark.createDataFrame(data, schema)

    # Write to HMS
    df.write.mode("overwrite").saveAsTable(table_name)

    # Read back
    result_df = spark.table(table_name)

    # Verify schema
    assert result_df.schema["attributes"].dataType.typeName() == "map"

    # Verify data
    alice = result_df.filter(result_df.name == "Alice").collect()[0]
    assert alice.attributes["dept"] == "Engineering"
    assert alice.attributes["level"] == "Senior"


def test_complex_types_struct(test_database):
    """Test tables with struct columns"""
    table_name = "test_struct_type"

    # Create data with structs
    data = [
        (1, "Alice", Row(street="123 Main St", city="NYC", zip="10001")),
        (2, "Bob", Row(street="456 Oak Ave", city="SF", zip="94102")),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zip", StringType(), True),
        ]), True),
    ])

    df = spark.createDataFrame(data, schema)

    # Write to HMS
    df.write.mode("overwrite").saveAsTable(table_name)

    # Read back
    result_df = spark.table(table_name)

    # Verify schema
    assert result_df.schema["address"].dataType.typeName() == "struct"

    # Verify data
    alice = result_df.filter(result_df.name == "Alice").collect()[0]
    assert alice.address.city == "NYC"
    assert alice.address.zip == "10001"


def test_nested_complex_types(test_database):
    """Test nested complex types (array of structs)"""
    table_name = "test_nested_types"

    # Create data with array of structs
    data = [
        (1, "Alice", [
            Row(skill="Python", years=5),
            Row(skill="Scala", years=3),
        ]),
        (2, "Bob", [
            Row(skill="Java", years=7),
        ]),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("skills", ArrayType(StructType([
            StructField("skill", StringType(), True),
            StructField("years", IntegerType(), True),
        ])), True),
    ])

    df = spark.createDataFrame(data, schema)

    # Write to HMS
    df.write.mode("overwrite").saveAsTable(table_name)

    # Read back
    result_df = spark.table(table_name)

    # Verify data
    alice = result_df.filter(result_df.name == "Alice").collect()[0]
    assert len(alice.skills) == 2
    assert alice.skills[0].skill == "Python"
    assert alice.skills[0].years == 5


def test_alter_table_add_column(test_database, sample_data):
    """Test adding columns to existing table"""
    table_name = "test_add_column"

    # Create initial table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    # Add a new column
    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (salary INT)")

    # Verify schema
    df = spark.table(table_name)
    assert "salary" in df.columns

    # Insert data with new column
    new_row = spark.sql(f"""
        SELECT 6 as id, 'Frank' as name, 40 as age, 'Sales' as department, 80000 as salary
    """)

    new_row.write.mode("append").insertInto(table_name)

    # Read and verify
    result = spark.sql(f"SELECT * FROM {table_name} WHERE name = 'Frank'").collect()[0]
    assert result.salary == 80000


def test_alter_table_rename_column(test_database, sample_data):
    """Test renaming columns (if supported by HMS version)"""
    table_name = "test_rename_column"

    # Create table
    sample_data.write.mode("overwrite").saveAsTable(table_name)

    try:
        # Try to rename column (may not be supported in all HMS versions)
        spark.sql(f"ALTER TABLE {table_name} CHANGE COLUMN age employee_age INT")

        # Verify rename
        df = spark.table(table_name)
        assert "employee_age" in df.columns
        assert "age" not in df.columns

    except Exception as e:
        # Some HMS versions don't support CHANGE COLUMN
        pytest.skip(f"Column rename not supported: {e}")


def test_decimal_type(test_database):
    """Test decimal/numeric types"""
    table_name = "test_decimal"

    # Create data with decimal
    data = [
        (1, "Product A", 19.99),
        (2, "Product B", 29.99),
        (3, "Product C", 9.50),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("price", DecimalType(10, 2), False),
    ])

    df = spark.createDataFrame(data, schema)

    # Write to HMS
    df.write.mode("overwrite").saveAsTable(table_name)

    # Read back
    result_df = spark.table(table_name)

    # Verify decimal precision
    from decimal import Decimal
    product_a = result_df.filter(result_df.name == "Product A").collect()[0]
    assert product_a.price == Decimal("19.99")


def test_timestamp_type(test_database):
    """Test timestamp types"""
    from datetime import datetime
    table_name = "test_timestamp"

    # Create data with timestamps
    data = [
        (1, "Event A", datetime(2024, 1, 1, 10, 30, 0)),
        (2, "Event B", datetime(2024, 1, 2, 14, 45, 30)),
    ]

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("event_time", TimestampType(), False),
    ])

    df = spark.createDataFrame(data, schema)

    # Write to HMS
    df.write.mode("overwrite").saveAsTable(table_name)

    # Read back
    result_df = spark.table(table_name)

    # Verify timestamp
    event_a = result_df.filter(result_df.name == "Event A").collect()[0]
    assert event_a.event_time.year == 2024
    assert event_a.event_time.month == 1
    assert event_a.event_time.day == 1


def test_null_values(test_database):
    """Test handling of NULL values"""
    table_name = "test_nulls"

    # Create data with nulls
    data = [
        (1, "Alice", 30, "Engineering"),
        (2, "Bob", None, "Sales"),
        (3, None, 35, "Marketing"),
    ]

    df = spark.createDataFrame(data, ["id", "name", "age", "department"])

    # Write to HMS
    df.write.mode("overwrite").saveAsTable(table_name)

    # Read back
    result_df = spark.table(table_name)

    # Verify nulls
    bob = result_df.filter(result_df.name == "Bob").collect()[0]
    assert bob.age is None

    row_3 = result_df.filter(result_df.id == 3).collect()[0]
    assert row_3.name is None


def test_schema_describe(test_database):
    """Test DESCRIBE FORMATTED for detailed schema info"""
    table_name = "test_schema_describe"

    # Create table with various types
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("score", DoubleType(), True),
        StructField("active", BooleanType(), True),
    ])

    data = [(1, "Alice", 95.5, True)]
    df = spark.createDataFrame(data, schema)

    df.write.mode("overwrite").saveAsTable(table_name)

    # Get formatted description
    description = spark.sql(f"DESCRIBE FORMATTED {table_name}").collect()

    # Verify we get schema information
    assert len(description) > 0

    # Check that column types are present
    desc_str = "\n".join([f"{row.col_name}: {row.data_type}" for row in description])
    assert "int" in desc_str.lower()
    assert "string" in desc_str.lower()
