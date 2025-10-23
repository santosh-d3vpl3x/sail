"""
PyTest configuration and shared fixtures for HMS Kerberos testing
"""

import os
import subprocess
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def kerberos_authenticated():
    """
    Ensure Kerberos authentication is valid before running tests
    """
    keytab = os.environ.get("KRB5_CLIENT_KTNAME", "/etc/security/keytabs/client.keytab")
    principal = "client@EXAMPLE.COM"

    print(f"Authenticating with Kerberos using keytab: {keytab}")

    # Kinit with keytab
    result = subprocess.run(
        ["kinit", "-kt", keytab, principal],
        capture_output=True,
        text=True
    )

    if result.returncode != 0:
        pytest.fail(f"Kerberos authentication failed: {result.stderr}")

    # Verify ticket
    result = subprocess.run(["klist"], capture_output=True, text=True)
    print(f"Kerberos ticket cache:\n{result.stdout}")

    assert result.returncode == 0, "Failed to verify Kerberos ticket"

    yield

    # Cleanup: destroy ticket cache
    subprocess.run(["kdestroy"], capture_output=True)


@pytest.fixture(scope="session")
def spark(kerberos_authenticated):
    """
    Create a Spark session configured for Kerberos HMS access
    """
    hms_uri = os.environ.get("HMS_URI", "thrift://hms.example.com:9083")

    spark = (
        SparkSession.builder
        .appName("HMS Kerberos Test")
        .config("spark.sql.catalogImplementation", "hive")
        .config("hive.metastore.uris", hms_uri)
        .config("hive.metastore.sasl.enabled", "true")
        .config("hive.metastore.kerberos.principal", "hive/hms.example.com@EXAMPLE.COM")
        .config("hive.metastore.client.kerberos.principal", "client@EXAMPLE.COM")
        .config("hive.security.authorization.enabled", "false")
        # S3/MinIO configuration
        .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("AWS_ENDPOINT", "http://minio.example.com:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Increase timeout for slower environments
        .config("spark.sql.hive.metastore.connectTimeout", "60s")
        .config("spark.sql.hive.metastore.readTimeout", "60s")
        .enableHiveSupport()
        .getOrCreate()
    )

    # Set log level
    spark.sparkContext.setLogLevel("INFO")

    print(f"Spark session created, connecting to HMS at: {hms_uri}")

    yield spark

    # Cleanup
    spark.stop()


@pytest.fixture(scope="function")
def test_database(spark):
    """
    Create a test database for each test function
    """
    import uuid
    db_name = f"test_db_{uuid.uuid4().hex[:8]}"

    print(f"Creating test database: {db_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    spark.sql(f"USE {db_name}")

    yield db_name

    # Cleanup: drop database
    print(f"Dropping test database: {db_name}")
    spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")


@pytest.fixture(scope="function")
def sample_data(spark):
    """
    Create sample data for testing
    """
    data = [
        (1, "Alice", 30, "Engineering"),
        (2, "Bob", 25, "Sales"),
        (3, "Charlie", 35, "Engineering"),
        (4, "Diana", 28, "Marketing"),
        (5, "Eve", 32, "Engineering"),
    ]

    columns = ["id", "name", "age", "department"]

    return spark.createDataFrame(data, columns)
