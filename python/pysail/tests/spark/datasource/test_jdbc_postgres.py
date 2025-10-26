"""Integration tests for PostgreSQL JDBC reads using ConnectorX."""

from __future__ import annotations

import os
from collections import Counter
from typing import Any

import psycopg
import pytest

POSTGRES_CONFIG = {
    "host": os.getenv("SAIL_TEST_POSTGRES_HOST"),
    "port": os.getenv("SAIL_TEST_POSTGRES_PORT", "5432"),
    "database": os.getenv("SAIL_TEST_POSTGRES_DB", "postgres"),
    "user": os.getenv("SAIL_TEST_POSTGRES_USER"),
    "password": os.getenv("SAIL_TEST_POSTGRES_PASSWORD"),
}

_required_config = ("host", "user", "password")
pytestmark = pytest.mark.skipif(
    not all(POSTGRES_CONFIG[key] for key in _required_config),
    reason="PostgreSQL test database is not configured",
)


def _status_counts(rows: list[tuple[Any, ...]]) -> Counter[str]:
    """Compute status counts from test data."""
    return Counter(row[-1] for row in rows)


@pytest.fixture(scope="module")
def postgres_orders_table():
    """Provision orders table in PostgreSQL for testing."""
    table_name = "test_orders_connectorx"
    test_data: list[tuple[int, str, float, str]] = [
        (1, "Alice", 100.50, "completed"),
        (2, "Bob", 250.75, "pending"),
        (3, "Charlie", 75.25, "completed"),
        (4, "David", 150.00, "completed"),
        (5, "Eve", 300.00, "pending"),
    ]

    conn = psycopg.connect(
        host=POSTGRES_CONFIG["host"],
        port=POSTGRES_CONFIG["port"],
        user=POSTGRES_CONFIG["user"],
        password=POSTGRES_CONFIG["password"],
        dbname=POSTGRES_CONFIG["database"],
    )

    with conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')  # noqa: S608
        cur.execute(
            f"""
            CREATE TABLE "{table_name}" (
                order_id INTEGER PRIMARY KEY,
                customer_name TEXT NOT NULL,
                amount NUMERIC(10, 2) NOT NULL,
                status TEXT NOT NULL
            )
            """
        )
        cur.executemany(
            f'INSERT INTO "{table_name}" (order_id, customer_name, amount, status) VALUES (%s, %s, %s, %s)',
            test_data,
        )  # noqa: S608

    yield {
        "table": table_name,
        "data": test_data,
    }

    with conn, conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')  # noqa: S608

    conn.close()


def _jdbc_options(table: str) -> dict[str, str]:
    """Build JDBC options for ConnectorX backend."""
    url = (
        f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/"
        f"{POSTGRES_CONFIG['database']}"
    )
    return {
        "url": url,
        "dbtable": table,
        "user": POSTGRES_CONFIG["user"] or "",
        "password": POSTGRES_CONFIG["password"] or "",
        "engine": "connectorx",
    }


def test_postgres_connectorx_basic_read(spark, postgres_orders_table):
    """Verify ConnectorX backend can read from PostgreSQL via Spark JDBC."""
    table = postgres_orders_table["table"]
    expected_data = postgres_orders_table["data"]

    df = spark.read.format("jdbc").options(**_jdbc_options(table)).load()
    collected = df.collect()

    assert df.columns == ["order_id", "customer_name", "amount", "status"]

    result = sorted(
        (row["order_id"], row["customer_name"], float(row["amount"]), row["status"])
        for row in collected
    )
    assert result == sorted(expected_data)

    grouped = df.groupBy("status").count().collect()
    counts = {row["status"]: row["count"] for row in grouped}
    assert counts == _status_counts(expected_data)


def test_postgres_connectorx_filter_pushdown(spark, postgres_orders_table):
    """Ensure where predicates are respected when reading from PostgreSQL."""
    table = postgres_orders_table["table"]

    df = (
        spark.read.format("jdbc")
        .options(**_jdbc_options(table))
        .option("predicates", "status = 'completed'")
        .load()
    )

    collected = df.collect()
    assert all(row["status"] == "completed" for row in collected)
    assert len(collected) == 3  # noqa: PLR2004
