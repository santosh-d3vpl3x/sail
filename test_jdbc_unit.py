#!/usr/bin/env python3
"""
Unit tests for JDBC reader components (no Spark required).
Tests the individual components without needing a running Spark server.
"""

import sys
sys.path.insert(0, "python")


def test_imports():
    """Test that all modules import correctly."""
    print("="*60)
    print("TEST: Module Imports")
    print("="*60)

    try:
        print("Importing pysail.read...")
        from pysail.read import (
            read_jdbc,
            install_jdbc_reader,
            InvalidJDBCUrlError,
            BackendNotAvailableError,
        )
        print("✓ pysail.read imports OK")

        print("\nImporting JDBC options...")
        from pysail.read.jdbc_options import NormalizedJDBCOptions
        print("✓ JDBC options imports OK")

        print("\nImporting JDBC URL parser...")
        from pysail.read.jdbc_url_parser import parse_jdbc_url
        print("✓ JDBC URL parser imports OK")

        print("\nImporting partition planner...")
        from pysail.read.partition_planner import PartitionPlanner
        print("✓ Partition planner imports OK")

        print("\nImporting backends...")
        from pysail.read.backends import get_backend
        print("✓ Backends import OK")

        print("\nImporting ArrowBatchDataSource...")
        from pysail.read.arrow_datasource import ArrowBatchDataSource, JDBCArrowDataSource
        print("✓ ArrowBatchDataSource imports OK")

        print("\nImporting Spark integration...")
        from pysail.read.spark_integration import install_jdbc_reader, jdbc
        print("✓ Spark integration imports OK")

        print("\n✅ All imports successful!")
        return True

    except Exception as e:
        print(f"\n❌ Import failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_url_parsing():
    """Test JDBC URL parsing."""
    print("\n" + "="*60)
    print("TEST: JDBC URL Parsing")
    print("="*60)

    from pysail.read.jdbc_url_parser import parse_jdbc_url

    test_cases = [
        ("jdbc:postgresql://localhost:5432/mydb", "postgresql", "postgresql://localhost:5432/mydb"),
        ("jdbc:mysql://localhost:3306/mydb", "mysql", "mysql://localhost:3306/mydb"),
        ("jdbc:sqlite:/path/to/db.db", "sqlite", "sqlite:/path/to/db.db"),
    ]

    all_passed = True

    for jdbc_url, expected_driver, expected_conn in test_cases:
        try:
            result = parse_jdbc_url(jdbc_url)
            assert result.driver == expected_driver, f"Expected driver {expected_driver}, got {result.driver}"
            assert expected_driver in result.connection_string, f"Expected {expected_driver} in {result.connection_string}"
            print(f"✓ {jdbc_url[:40]:40s} → {result.driver}")
        except Exception as e:
            print(f"❌ {jdbc_url[:40]:40s} → FAILED: {e}")
            all_passed = False

    if all_passed:
        print("\n✅ URL parsing tests passed!")
    else:
        print("\n❌ Some URL parsing tests failed")

    return all_passed


def test_options_normalization():
    """Test options normalization."""
    print("\n" + "="*60)
    print("TEST: Options Normalization")
    print("="*60)

    from pysail.read.jdbc_options import NormalizedJDBCOptions

    # Test case 1: Basic options
    try:
        options = {
            "url": "jdbc:postgresql://localhost/db",
            "dbtable": "orders"
        }
        opts = NormalizedJDBCOptions.from_spark_options(options)
        opts.validate()
        print("✓ Basic options")
    except Exception as e:
        print(f"❌ Basic options failed: {e}")
        return False

    # Test case 2: Case insensitivity
    try:
        options = {
            "URL": "jdbc:postgresql://localhost/db",
            "DbTable": "orders"
        }
        opts = NormalizedJDBCOptions.from_spark_options(options)
        opts.validate()
        print("✓ Case insensitive options")
    except Exception as e:
        print(f"❌ Case insensitive options failed: {e}")
        return False

    # Test case 3: Partitioning
    try:
        options = {
            "url": "jdbc:postgresql://localhost/db",
            "dbtable": "orders",
            "partitionColumn": "id",
            "lowerBound": "1",
            "upperBound": "1000",
            "numPartitions": "10"
        }
        opts = NormalizedJDBCOptions.from_spark_options(options)
        opts.validate()
        print("✓ Partitioning options")
    except Exception as e:
        print(f"❌ Partitioning options failed: {e}")
        return False

    print("\n✅ Options normalization tests passed!")
    return True


def test_partition_planning():
    """Test partition planning."""
    print("\n" + "="*60)
    print("TEST: Partition Planning")
    print("="*60)

    from pysail.read.partition_planner import PartitionPlanner

    # Test case 1: No partitioning
    planner = PartitionPlanner()
    preds = planner.generate_predicates()
    assert preds == [None], f"Expected [None], got {preds}"
    print("✓ No partitioning")

    # Test case 2: Range partitioning
    planner = PartitionPlanner(
        partition_column="id",
        lower_bound=0,
        upper_bound=100,
        num_partitions=4
    )
    preds = planner.generate_predicates()
    assert len(preds) == 4, f"Expected 4 predicates, got {len(preds)}"
    print(f"✓ Range partitioning (4 partitions)")
    for i, pred in enumerate(preds):
        print(f"  Partition {i}: {pred}")

    # Test case 3: Explicit predicates
    planner = PartitionPlanner(
        predicates=["status='A'", "status='B'", "status='C'"]
    )
    preds = planner.generate_predicates()
    assert len(preds) == 3, f"Expected 3 predicates, got {len(preds)}"
    print("✓ Explicit predicates")

    print("\n✅ Partition planning tests passed!")
    return True


def test_credential_masking():
    """Test credential masking."""
    print("\n" + "="*60)
    print("TEST: Credential Masking")
    print("="*60)

    from pysail.read.utils import mask_credentials

    test_cases = [
        ("postgresql://user:pass@localhost/db", "postgresql://***:***@localhost/db"),
        ("mysql://root:secret@host:3306/db", "mysql://***:***@host:3306/db"),
        ("postgresql://localhost/db", "postgresql://localhost/db"),  # No creds
    ]

    all_passed = True

    for input_url, expected in test_cases:
        result = mask_credentials(input_url)
        if result == expected:
            print(f"✓ {input_url[:30]:30s} → masked")
        else:
            print(f"❌ {input_url[:30]:30s} → Expected: {expected}, Got: {result}")
            all_passed = False

    if all_passed:
        print("\n✅ Credential masking tests passed!")
    else:
        print("\n❌ Some credential masking tests failed")

    return all_passed


def test_backend_interface():
    """Test backend interface."""
    print("\n" + "="*60)
    print("TEST: Backend Interface")
    print("="*60)

    from pysail.read.backends import get_backend
    from pysail.read.exceptions import BackendNotAvailableError

    # Test getting backends
    try:
        backend = get_backend("fallback")
        print(f"✓ Fallback backend available: {backend.get_name()}")
    except BackendNotAvailableError as e:
        print(f"⚠️  Fallback backend not available: {e}")

    try:
        backend = get_backend("connectorx")
        print(f"✓ ConnectorX backend available: {backend.get_name()}")
    except BackendNotAvailableError as e:
        print(f"⚠️  ConnectorX backend not available: {e}")

    try:
        backend = get_backend("adbc")
        print(f"✓ ADBC backend available: {backend.get_name()}")
    except BackendNotAvailableError as e:
        print(f"⚠️  ADBC backend not available: {e}")

    print("\n✅ Backend interface tests passed!")
    return True


def test_arrow_datasource_interface():
    """Test ArrowBatchDataSource interface."""
    print("\n" + "="*60)
    print("TEST: ArrowBatchDataSource Interface")
    print("="*60)

    from pysail.read.arrow_datasource import JDBCArrowDataSource

    try:
        datasource = JDBCArrowDataSource()
        print(f"✓ JDBCArrowDataSource instantiated: {type(datasource).__name__}")

        # Check that abstract methods exist
        assert hasattr(datasource, 'infer_schema'), "Missing infer_schema method"
        assert hasattr(datasource, 'plan_partitions'), "Missing plan_partitions method"
        assert hasattr(datasource, 'read_partition'), "Missing read_partition method"
        assert hasattr(datasource, 'to_dataframe'), "Missing to_dataframe method"

        print("✓ All required methods present")

        print("\n✅ ArrowBatchDataSource interface tests passed!")
        return True

    except Exception as e:
        print(f"\n❌ ArrowBatchDataSource interface test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    print("JDBC Reader Unit Tests (No Spark Required)")
    print("="*60)

    results = []
    results.append(("Imports", test_imports()))
    results.append(("URL Parsing", test_url_parsing()))
    results.append(("Options Normalization", test_options_normalization()))
    results.append(("Partition Planning", test_partition_planning()))
    results.append(("Credential Masking", test_credential_masking()))
    results.append(("Backend Interface", test_backend_interface()))
    results.append(("ArrowBatchDataSource", test_arrow_datasource_interface()))

    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)

    passed = sum(1 for _, r in results if r)

    for name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{name:30s} {status}")

    print(f"\nTotal: {passed}/{len(results)} passed")

    return 0 if passed == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
