"""
Test script for JDBC reader with live Spark Connect.

This script:
1. Creates a test SQLite database with sample data
2. Starts Spark Connect server
3. Tests spark.read.jdbc() and spark.read.format("jdbc")
4. Validates results

Run with: python test_jdbc_integration.py
"""

import sqlite3
import os
import sys
import tempfile
from pathlib import Path

# Add python module to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

def create_test_database():
    """Create a test SQLite database with sample data."""
    db_path = tempfile.mktemp(suffix=".db")
    print(f"Creating test database: {db_path}")

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

    print(f"Created test database with {len(test_data)} orders")
    return db_path


def test_direct_api(spark, db_path):
    """Test the direct read_jdbc() API."""
    print("\n" + "="*60)
    print("TEST 1: Direct API - read_jdbc()")
    print("="*60)

    from pysail.read import read_jdbc

    try:
        jdbc_url = f"jdbc:sqlite:{db_path}"
        print(f"Reading from: {jdbc_url}")

        df = read_jdbc(
            spark,
            url=jdbc_url,
            dbtable="orders",
            engine="fallback"  # Use fallback for SQLite
        )

        print("\nSchema:")
        df.printSchema()

        print("\nData:")
        df.show()

        print("\nCount:", df.count())

        print("\n✅ TEST 1 PASSED: Direct API works!")
        return True

    except Exception as e:
        print(f"\n❌ TEST 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_spark_jdbc_api(spark, db_path):
    """Test the spark.read.jdbc() API."""
    print("\n" + "="*60)
    print("TEST 2: Spark Native API - spark.read.jdbc()")
    print("="*60)

    from pysail.read import install_jdbc_reader

    try:
        # Install JDBC reader
        print("Installing JDBC reader...")
        install_jdbc_reader(spark)
        print("✓ JDBC reader installed")

        jdbc_url = f"jdbc:sqlite:{db_path}"
        print(f"Reading from: {jdbc_url}")

        df = spark.read.jdbc(
            url=jdbc_url,
            table="orders",
            properties={"engine": "fallback"}  # Use fallback for SQLite
        )

        print("\nSchema:")
        df.printSchema()

        print("\nData:")
        df.show()

        print("\nFiltered (status='completed'):")
        df.filter(df.status == "completed").show()

        print("\n✅ TEST 2 PASSED: spark.read.jdbc() works!")
        return True

    except Exception as e:
        print(f"\n❌ TEST 2 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_format_api(spark, db_path):
    """Test the spark.read.format('jdbc') API."""
    print("\n" + "="*60)
    print("TEST 3: DataSource V2 API - spark.read.format('jdbc')")
    print("="*60)

    try:
        jdbc_url = f"jdbc:sqlite:{db_path}"
        print(f"Reading from: {jdbc_url}")

        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "orders") \
            .option("engine", "fallback") \
            .load()

        print("\nSchema:")
        df.printSchema()

        print("\nData:")
        df.show()

        print("\nGrouped by status:")
        df.groupBy("status").count().show()

        print("\n✅ TEST 3 PASSED: spark.read.format('jdbc') works!")
        return True

    except Exception as e:
        print(f"\n❌ TEST 3 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_with_partitioning(spark, db_path):
    """Test partitioning functionality."""
    print("\n" + "="*60)
    print("TEST 4: Partitioning - spark.read.jdbc() with partitions")
    print("="*60)

    try:
        jdbc_url = f"jdbc:sqlite:{db_path}"
        print(f"Reading from: {jdbc_url} with 3 partitions")

        df = spark.read.jdbc(
            url=jdbc_url,
            table="orders",
            column="order_id",
            lowerBound=1,
            upperBound=10,
            numPartitions=3,
            properties={"engine": "fallback"}
        )

        print("\nSchema:")
        df.printSchema()

        print("\nData:")
        df.show()

        print(f"\nNumber of partitions: {df.rdd.getNumPartitions()}")

        print("\n✅ TEST 4 PASSED: Partitioning works!")
        return True

    except Exception as e:
        print(f"\n❌ TEST 4 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all tests."""
    print("="*60)
    print("JDBC Reader Integration Tests")
    print("="*60)

    # Create test database
    db_path = create_test_database()

    try:
        # Get or create Spark session
        from pyspark.sql import SparkSession

        # Check if we should connect to existing server or start new one
        remote = os.environ.get("SPARK_REMOTE")

        if remote:
            print(f"\nConnecting to Spark Connect: {remote}")
            spark = SparkSession.builder.remote(remote).getOrCreate()
        else:
            print("\nStarting Spark Connect server...")
            from pysail.spark import SparkConnectServer

            server = SparkConnectServer("127.0.0.1", 0)
            server.start(background=True)
            _, port = server.listening_address
            remote = f"sc://localhost:{port}"
            print(f"Spark Connect server started: {remote}")

            spark = SparkSession.builder.remote(remote).getOrCreate()

        print(f"Spark version: {spark.version}")
        print(f"Spark Connect: {remote}")

        # Run tests
        results = []

        results.append(("Direct API", test_direct_api(spark, db_path)))
        results.append(("spark.read.jdbc()", test_spark_jdbc_api(spark, db_path)))
        results.append(("spark.read.format('jdbc')", test_format_api(spark, db_path)))
        results.append(("Partitioning", test_with_partitioning(spark, db_path)))

        # Summary
        print("\n" + "="*60)
        print("TEST SUMMARY")
        print("="*60)

        passed = sum(1 for _, result in results if result)
        total = len(results)

        for name, result in results:
            status = "✅ PASSED" if result else "❌ FAILED"
            print(f"{name:30s} {status}")

        print(f"\nTotal: {passed}/{total} tests passed")

        if passed == total:
            print("\n🎉 All tests passed!")
            return 0
        else:
            print(f"\n⚠️  {total - passed} test(s) failed")
            return 1

    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)
            print(f"\nCleaned up test database: {db_path}")


if __name__ == "__main__":
    sys.exit(main())
