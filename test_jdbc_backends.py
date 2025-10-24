#!/usr/bin/env python3
"""
Test JDBC reader backends (ConnectorX, ADBC) with real database.

This tests the Arrow backends directly without requiring Spark.
"""

import sqlite3
import tempfile
import os
import sys
from pathlib import Path

# Add python module to path
sys.path.insert(0, str(Path(__file__).parent / "python"))

import pyarrow as pa
from pysail.read.arrow_datasource import JDBCArrowDataSource


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

    print(f"✓ Created test database with {len(test_data)} orders")
    return db_path


def test_backend(backend_name, db_path):
    """Test a specific backend (connectorx, adbc, fallback)."""
    print(f"\n{'='*60}")
    print(f"Testing {backend_name.upper()} Backend")
    print('='*60)

    datasource = JDBCArrowDataSource()

    # SQLite JDBC URL
    jdbc_url = f"jdbc:sqlite:{db_path}"

    options = {
        "url": jdbc_url,
        "dbtable": "orders",
        "engine": backend_name,
    }

    try:
        # Test 1: Infer schema
        print("\n1. Testing infer_schema()...")
        schema = datasource.infer_schema(options)
        print(f"   Schema: {schema}")
        assert isinstance(schema, pa.Schema), "Must return pa.Schema"
        assert "order_id" in schema.names, "Must have order_id column"
        assert "product" in schema.names, "Must have product column"
        print("   ✓ Schema inference successful")

        # Test 2: Plan partitions
        print("\n2. Testing plan_partitions()...")
        partitions = datasource.plan_partitions(options)
        print(f"   Number of partitions: {len(partitions)}")
        assert isinstance(partitions, list), "Must return list"
        assert len(partitions) > 0, "Must have at least one partition"
        print(f"   ✓ Partition planning successful ({len(partitions)} partitions)")

        # Test 3: Read data
        print("\n3. Testing read_partition()...")
        total_rows = 0
        for i, partition_spec in enumerate(partitions):
            print(f"   Reading partition {i}...")
            batches = list(datasource.read_partition(partition_spec, options))
            partition_rows = sum(batch.num_rows for batch in batches)
            total_rows += partition_rows
            print(f"   Partition {i}: {partition_rows} rows in {len(batches)} batches")

        print(f"\n   Total rows read: {total_rows}")
        assert total_rows == 10, f"Expected 10 rows, got {total_rows}"
        print("   ✓ Data reading successful")

        # Test 4: Verify data content
        print("\n4. Verifying data content...")
        all_batches = []
        for partition_spec in partitions:
            batches = list(datasource.read_partition(partition_spec, options))
            all_batches.extend(batches)

        # Combine all batches into single table
        table = pa.Table.from_batches(all_batches)
        print(f"   Combined table: {table.num_rows} rows x {table.num_columns} columns")

        # Verify some data
        products = table.column("product").to_pylist()
        assert "Widget" in products, "Must have Widget product"
        assert "Gadget" in products, "Must have Gadget product"
        print("   ✓ Data content verified")

        print(f"\n{'='*60}")
        print(f"✓ {backend_name.upper()} Backend: ALL TESTS PASSED")
        print('='*60)

        return True

    except Exception as e:
        print(f"\n{'='*60}")
        print(f"✗ {backend_name.upper()} Backend: FAILED")
        print(f"Error: {e}")
        print('='*60)
        import traceback
        traceback.print_exc()
        return False


def test_partitioned_reads(db_path):
    """Test partitioned reads with range partitioning."""
    print(f"\n{'='*60}")
    print("Testing PARTITIONED READS")
    print('='*60)

    datasource = JDBCArrowDataSource()

    jdbc_url = f"jdbc:sqlite:{db_path}"

    # Test range partitioning
    options = {
        "url": jdbc_url,
        "dbtable": "orders",
        "engine": "connectorx",
        "partitionColumn": "order_id",
        "lowerBound": "1",
        "upperBound": "10",
        "numPartitions": "3",
    }

    try:
        print("\n1. Planning partitions with range partitioning...")
        partitions = datasource.plan_partitions(options)
        print(f"   Number of partitions: {len(partitions)}")
        assert len(partitions) == 3, f"Expected 3 partitions, got {len(partitions)}"

        print("\n2. Reading partitioned data...")
        total_rows = 0
        for i, partition_spec in enumerate(partitions):
            print(f"   Partition {i}: {partition_spec.get('predicate', 'no predicate')}")
            batches = list(datasource.read_partition(partition_spec, options))
            partition_rows = sum(batch.num_rows for batch in batches)
            total_rows += partition_rows
            print(f"   Partition {i}: {partition_rows} rows")

        print(f"\n   Total rows: {total_rows}")
        assert total_rows == 10, f"Expected 10 rows, got {total_rows}"

        print("\n✓ Partitioned reads: ALL TESTS PASSED")
        return True

    except Exception as e:
        print(f"\n✗ Partitioned reads: FAILED")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Run all backend tests."""
    print("="*70)
    print("JDBC Arrow Backend Integration Tests")
    print("="*70)

    # Create test database
    db_path = create_test_database()

    results = {}

    try:
        # Test ConnectorX backend
        results["connectorx"] = test_backend("connectorx", db_path)

        # Test ADBC backend
        results["adbc"] = test_backend("adbc", db_path)

        # Test Fallback backend (SQLite)
        results["fallback"] = test_backend("fallback", db_path)

        # Test partitioned reads
        results["partitioned"] = test_partitioned_reads(db_path)

    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.remove(db_path)
            print(f"\n✓ Cleaned up test database: {db_path}")

    # Summary
    print("\n" + "="*70)
    print("TEST SUMMARY")
    print("="*70)

    total = len(results)
    passed = sum(1 for r in results.values() if r)
    failed = total - passed

    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{test_name:20} {status}")

    print("="*70)
    print(f"Total: {total} | Passed: {passed} | Failed: {failed}")
    print("="*70)

    if failed > 0:
        print("\n✗ Some tests failed!")
        return 1
    else:
        print("\n✓ All tests passed!")
        return 0


if __name__ == "__main__":
    sys.exit(main())
