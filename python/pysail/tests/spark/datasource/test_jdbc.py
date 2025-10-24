"""Tests for JDBC data source using Spark Connect."""

import os
import sqlite3
import tempfile

import pytest


@pytest.fixture(scope="module")
def sqlite_db():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".db", delete=False) as f:
        db_path = f.name

    # Create database with test data
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create table
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_name TEXT,
            amount REAL,
            status TEXT
        )
    """)

    # Insert test data
    test_data = [
        (1, "Alice", 100.50, "completed"),
        (2, "Bob", 250.75, "pending"),
        (3, "Charlie", 75.25, "completed"),
        (4, "David", 150.00, "completed"),
        (5, "Eve", 300.00, "pending"),
    ]
    cursor.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?)",
        test_data
    )

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    os.unlink(db_path)


def test_jdbc_format_basic_read(spark, sqlite_db):
    """Test basic JDBC read using format('jdbc')."""
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{sqlite_db}") \
        .option("dbtable", "orders") \
        .option("engine", "connectorx") \
        .load()

    result = df.collect()
    assert len(result) == 5
    assert df.columns == ["order_id", "customer_name", "amount", "status"]


def test_jdbc_format_with_filter(spark, sqlite_db):
    """Test JDBC read with SQL filter."""
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{sqlite_db}") \
        .option("dbtable", "(SELECT * FROM orders WHERE status = 'completed') AS filtered") \
        .option("engine", "connectorx") \
        .load()

    result = df.collect()
    assert len(result) == 3
    assert all(row["status"] == "completed" for row in result)


def test_jdbc_python_format(spark, sqlite_db):
    """Test JDBC read using generic Python format (advanced usage)."""
    df = spark.read.format("python") \
        .option("python_module", "pysail.jdbc.datasource") \
        .option("python_class", "JDBCArrowDataSource") \
        .option("url", f"jdbc:sqlite:{sqlite_db}") \
        .option("dbtable", "orders") \
        .option("engine", "connectorx") \
        .load()

    result = df.collect()
    assert len(result) == 5


def test_jdbc_with_spark_operations(spark, sqlite_db):
    """Test that JDBC DataFrames work with Spark operations."""
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{sqlite_db}") \
        .option("dbtable", "orders") \
        .load()

    # Filter
    filtered = df.filter(df.status == "completed")
    assert filtered.count() == 3

    # Group by
    grouped = df.groupBy("status").count()
    result = {row["status"]: row["count"] for row in grouped.collect()}
    assert result == {"completed": 3, "pending": 2}

    # Select
    names = df.select("customer_name").collect()
    assert len(names) == 5


def test_jdbc_schema_inference(spark, sqlite_db):
    """Test that schema is correctly inferred from database."""
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:sqlite:{sqlite_db}") \
        .option("dbtable", "orders") \
        .load()

    schema = df.schema
    field_names = [field.name for field in schema.fields]

    assert "order_id" in field_names
    assert "customer_name" in field_names
    assert "amount" in field_names
    assert "status" in field_names
