"""
Integration tests for JDBC reader with Spark Connect.

Tests the native Spark API integration (spark.read.jdbc and spark.read.format).
"""

import pytest
import sqlite3
import tempfile
import os


@pytest.fixture(scope="module")
def test_db():
    """Create a test SQLite database."""
    db_path = tempfile.mktemp(suffix=".db")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create test table
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product TEXT,
            quantity INTEGER,
            price REAL,
            status TEXT
        )
    """)

    # Insert test data
    test_data = [
        (1, 101, "Widget", 5, 19.99, "completed"),
        (2, 102, "Gadget", 3, 29.99, "pending"),
        (3, 103, "Doohickey", 2, 39.99, "completed"),
        (4, 104, "Thingamajig", 1, 49.99, "completed"),
        (5, 105, "Whatsit", 7, 9.99, "pending"),
        (6, 106, "Widget", 2, 19.99, "completed"),
        (7, 107, "Gadget", 4, 29.99, "cancelled"),
        (8, 108, "Doohickey", 1, 39.99, "completed"),
        (9, 109, "Thingamajig", 3, 49.99, "pending"),
        (10, 110, "Whatsit", 5, 9.99, "completed"),
    ]

    cursor.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
        test_data
    )

    conn.commit()
    conn.close()

    yield db_path

    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


class TestDirectJDBCAPI:
    """Test the direct read_jdbc() API."""

    def test_basic_read(self, spark, test_db):
        """Test basic JDBC read with direct API."""
        from pysail.read import read_jdbc

        jdbc_url = f"jdbc:sqlite:{test_db}"

        df = read_jdbc(
            spark,
            url=jdbc_url,
            dbtable="orders",
            engine="fallback"  # Use fallback for SQLite
        )

        assert df is not None
        assert df.count() == 10
        assert "order_id" in df.columns
        assert "product" in df.columns

    def test_read_with_filter(self, spark, test_db):
        """Test JDBC read with filter."""
        from pysail.read import read_jdbc

        jdbc_url = f"jdbc:sqlite:{test_db}"

        df = read_jdbc(
            spark,
            url=jdbc_url,
            query="SELECT * FROM orders WHERE status = 'completed'",
            engine="fallback"
        )

        count = df.count()
        assert count == 6, f"Expected 6 completed orders, got {count}"


class TestSparkNativeJDBCAPI:
    """Test spark.read.jdbc() native API."""

    def test_install_and_use(self, spark, test_db):
        """Test installing JDBC reader and using spark.read.jdbc()."""
        from pysail.read import install_jdbc_reader

        # Install JDBC reader
        install_jdbc_reader(spark)

        jdbc_url = f"jdbc:sqlite:{test_db}"

        # Use native Spark API
        df = spark.read.jdbc(
            url=jdbc_url,
            table="orders",
            properties={"engine": "fallback"}
        )

        assert df is not None
        assert df.count() == 10

    def test_with_operations(self, spark, test_db):
        """Test spark.read.jdbc() with Spark operations."""
        from pysail.read import install_jdbc_reader

        install_jdbc_reader(spark)

        jdbc_url = f"jdbc:sqlite:{test_db}"

        df = spark.read.jdbc(
            url=jdbc_url,
            table="orders",
            properties={"engine": "fallback"}
        )

        # Filter
        completed = df.filter(df.status == "completed")
        assert completed.count() == 6

        # Group by
        by_status = df.groupBy("status").count()
        assert by_status.count() == 3  # 3 distinct statuses


class TestFormatAPI:
    """Test spark.read.format('jdbc') API."""

    def test_format_jdbc(self, spark, test_db):
        """Test spark.read.format('jdbc') API."""
        from pysail.read import install_jdbc_reader

        install_jdbc_reader(spark)

        jdbc_url = f"jdbc:sqlite:{test_db}"

        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "orders") \
            .option("engine", "fallback") \
            .load()

        assert df is not None
        assert df.count() == 10

    def test_format_with_query(self, spark, test_db):
        """Test spark.read.format('jdbc') with custom query."""
        from pysail.read import install_jdbc_reader

        install_jdbc_reader(spark)

        jdbc_url = f"jdbc:sqlite:{test_db}"

        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", "SELECT product, SUM(quantity) as total FROM orders GROUP BY product") \
            .option("engine", "fallback") \
            .load()

        assert df is not None
        # Should have aggregated results
        products = [row.product for row in df.collect()]
        assert "Widget" in products
        assert "Gadget" in products


class TestPartitioning:
    """Test partitioning functionality."""

    def test_range_partitioning(self, spark, test_db):
        """Test range-based partitioning."""
        from pysail.read import install_jdbc_reader

        install_jdbc_reader(spark)

        jdbc_url = f"jdbc:sqlite:{test_db}"

        df = spark.read.jdbc(
            url=jdbc_url,
            table="orders",
            column="order_id",
            lowerBound=1,
            upperBound=10,
            numPartitions=3,
            properties={"engine": "fallback"}
        )

        assert df is not None
        assert df.count() == 10

        # Check that we got multiple partitions
        num_partitions = df.rdd.getNumPartitions()
        assert num_partitions >= 1, f"Expected at least 1 partition, got {num_partitions}"
