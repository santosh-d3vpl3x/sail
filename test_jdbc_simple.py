#!/usr/bin/env python3
"""
Simple standalone test for JDBC reader.
This script creates a SQLite database and tests the JDBC reader.
"""

import sys
import sqlite3
import tempfile
import os

# Setup paths
sys.path.insert(0, "python")

def create_test_db():
    """Create test SQLite database."""
    db_path = tempfile.mktemp(suffix=".db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

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

    test_data = [
        (1, 101, "Widget", 5, 19.99, "completed"),
        (2, 102, "Gadget", 3, 29.99, "pending"),
        (3, 103, "Doohickey", 2, 39.99, "completed"),
        (4, 104, "Thingamajig", 1, 49.99, "completed"),
        (5, 105, "Whatsit", 7, 9.99, "pending"),
    ]

    cursor.executemany("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)", test_data)
    conn.commit()
    conn.close()

    return db_path


def test_direct_api():
    """Test direct read_jdbc API."""
    print("\n" + "="*60)
    print("TEST 1: Direct API - read_jdbc()")
    print("="*60)

    db_path = create_test_db()

    try:
        # Start Spark Connect server
        from pysail.spark import SparkConnectServer
        from pyspark.sql import SparkSession

        print("Starting Spark Connect server...")
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        remote = f"sc://localhost:{port}"
        print(f"✓ Server started: {remote}")

        print("Creating Spark session...")
        spark = SparkSession.builder.remote(remote).getOrCreate()
        print(f"✓ Spark version: {spark.version}")

        # Test read_jdbc
        from pysail.read import read_jdbc

        jdbc_url = f"jdbc:sqlite:{db_path}"
        print(f"\nReading from: {jdbc_url}")

        df = read_jdbc(
            spark,
            url=jdbc_url,
            dbtable="orders",
            engine="fallback"
        )

        print("\n✓ DataFrame created")
        print(f"✓ Row count: {df.count()}")

        print("\nSchema:")
        df.printSchema()

        print("\nData:")
        df.show()

        print("\n✅ TEST 1 PASSED")
        server.stop()
        return True

    except Exception as e:
        print(f"\n❌ TEST 1 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def test_spark_api():
    """Test spark.read.jdbc() API."""
    print("\n" + "="*60)
    print("TEST 2: Spark Native API - spark.read.jdbc()")
    print("="*60)

    db_path = create_test_db()

    try:
        from pysail.spark import SparkConnectServer
        from pyspark.sql import SparkSession
        from pysail.read import install_jdbc_reader

        print("Starting Spark Connect server...")
        server = SparkConnectServer("127.0.0.1", 0)
        server.start(background=True)
        _, port = server.listening_address
        remote = f"sc://localhost:{port}"
        print(f"✓ Server started: {remote}")

        print("Creating Spark session...")
        spark = SparkSession.builder.remote(remote).getOrCreate()
        print(f"✓ Spark version: {spark.version}")

        print("\nInstalling JDBC reader...")
        install_jdbc_reader(spark)
        print("✓ JDBC reader installed")

        jdbc_url = f"jdbc:sqlite:{db_path}"
        print(f"\nReading from: {jdbc_url}")

        df = spark.read.jdbc(
            url=jdbc_url,
            table="orders",
            properties={"engine": "fallback"}
        )

        print("\n✓ DataFrame created")
        print(f"✓ Row count: {df.count()}")

        print("\nSchema:")
        df.printSchema()

        print("\nData:")
        df.show()

        print("\n✅ TEST 2 PASSED")
        server.stop()
        return True

    except Exception as e:
        print(f"\n❌ TEST 2 FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


def main():
    print("="*60)
    print("JDBC Reader Integration Tests")
    print("="*60)

    results = []
    results.append(("Direct API", test_direct_api()))
    results.append(("Spark Native API", test_spark_api()))

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{name:20s} {status}")

    passed = sum(1 for _, r in results if r)
    print(f"\nTotal: {passed}/{len(results)} passed")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
