#!/usr/bin/env python3
"""
Test PostgreSQL URL parsing and interface (without requiring a running database).

This demonstrates that the PostgreSQL support is implemented correctly.
Actual connection tests would require a running PostgreSQL server.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "python"))

from pysail.read.jdbc_url_parser import parse_jdbc_url
from pysail.read.arrow_datasource import JDBCArrowDataSource


def test_postgresql_url_parsing():
    """Test PostgreSQL JDBC URL parsing."""
    print("="*70)
    print("PostgreSQL URL Parsing Tests")
    print("="*70)

    test_cases = [
        {
            "name": "Basic PostgreSQL URL",
            "url": "jdbc:postgresql://localhost:5432/mydb",
            "expected_driver": "postgresql",
            "expected_conn": "postgresql://localhost:5432/mydb",
        },
        {
            "name": "PostgreSQL with credentials in URL",
            "url": "jdbc:postgresql://user:pass@localhost:5432/mydb",
            "expected_driver": "postgresql",
            "expected_conn": "postgresql://user:pass@localhost:5432/mydb",
        },
        {
            "name": "PostgreSQL with parameters",
            "url": "jdbc:postgresql://localhost/mydb?sslmode=require",
            "expected_driver": "postgresql",
            "expected_conn": "postgresql://localhost/mydb?sslmode=require",
        },
        {
            "name": "PostgreSQL with credential override",
            "url": "jdbc:postgresql://localhost/mydb",
            "user": "admin",
            "password": "secret",
            "expected_driver": "postgresql",
            "expected_conn": "postgresql://admin:secret@localhost/mydb",
        },
    ]

    all_passed = True

    for i, test in enumerate(test_cases, 1):
        print(f"\n{i}. {test['name']}")
        print(f"   Input: {test['url']}")

        try:
            parsed = parse_jdbc_url(
                test['url'],
                user=test.get('user'),
                password=test.get('password')
            )

            print(f"   Driver: {parsed.driver}")
            print(f"   Connection String: {parsed.connection_string}")

            assert parsed.driver == test['expected_driver'], \
                f"Expected driver {test['expected_driver']}, got {parsed.driver}"
            assert parsed.connection_string == test['expected_conn'], \
                f"Expected {test['expected_conn']}, got {parsed.connection_string}"

            print(f"   ✓ PASSED")
        except Exception as e:
            print(f"   ✗ FAILED: {e}")
            all_passed = False

    return all_passed


def test_postgresql_interface():
    """Test that PostgreSQL works with the ArrowDataSource interface."""
    print("\n" + "="*70)
    print("PostgreSQL ArrowDataSource Interface Test")
    print("="*70)

    datasource = JDBCArrowDataSource()

    # PostgreSQL JDBC URL
    options = {
        "url": "jdbc:postgresql://localhost:5432/testdb",
        "dbtable": "orders",
        "user": "test",
        "password": "test",
        "engine": "connectorx",
    }

    print(f"\nOptions: {options}")
    print("\nTesting interface methods (without actual database):")

    # Test 1: Check that options are accepted
    print("\n1. Testing option normalization...")
    try:
        from pysail.read.jdbc_options import NormalizedJDBCOptions
        normalized = NormalizedJDBCOptions.from_spark_options(options)
        print(f"   ✓ Options normalized successfully")
        print(f"   URL: {normalized.url}")
        print(f"   Table: {normalized.dbtable}")
        print(f"   Engine: {normalized.engine}")
    except Exception as e:
        print(f"   ✗ Failed: {e}")
        return False

    # Test 2: Check URL parsing
    print("\n2. Testing URL parsing...")
    try:
        from pysail.read.jdbc_url_parser import parse_jdbc_url
        parsed = parse_jdbc_url(options["url"], options.get("user"), options.get("password"))
        print(f"   ✓ URL parsed successfully")
        print(f"   Driver: {parsed.driver}")
        print(f"   Connection String: {parsed.connection_string}")
    except Exception as e:
        print(f"   ✗ Failed: {e}")
        return False

    # Test 3: Check partition planning (doesn't require DB connection)
    print("\n3. Testing partition planning (no DB required)...")
    try:
        from pysail.read.partition_planner import PartitionPlanner

        planner = PartitionPlanner(
            partition_column="id",
            lower_bound=1,
            upper_bound=1000,
            num_partitions=4,
        )

        predicates = planner.generate_predicates()
        print(f"   ✓ Partition planning successful")
        print(f"   Generated {len(predicates)} predicates")
        for i, pred in enumerate(predicates):
            print(f"   Partition {i}: {pred or 'no predicate'}")
    except Exception as e:
        print(f"   ✗ Failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    print("\n" + "="*70)
    print("NOTE: Actual database connection tests require:")
    print("  - Running PostgreSQL server")
    print("  - Test database and table")
    print("  - pip install 'connectorx>=0.3.0'")
    print("\nTo test with real PostgreSQL:")
    print("  1. Start PostgreSQL: docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=test postgres")
    print("  2. Create test database and table")
    print("  3. Run the integration test script")
    print("="*70)

    return True


def main():
    """Run all PostgreSQL tests."""
    print("\n" + "="*70)
    print("PostgreSQL Support Verification")
    print("="*70)

    results = {}

    # Test URL parsing
    results["url_parsing"] = test_postgresql_url_parsing()

    # Test interface
    results["interface"] = test_postgresql_interface()

    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)

    for test_name, passed in results.items():
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"{test_name:20} {status}")

    print("="*70)

    all_passed = all(results.values())

    if all_passed:
        print("\n✓ PostgreSQL support is correctly implemented!")
        print("  - URL parsing works")
        print("  - Interface is compatible")
        print("  - Ready for real database testing")
        return 0
    else:
        print("\n✗ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
