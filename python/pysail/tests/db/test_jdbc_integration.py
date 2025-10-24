from __future__ import annotations

import importlib
import os
import sqlite3
import subprocess
import sys
from typing import Iterable
from urllib.parse import urlparse

import pytest

from pysail.read import jdbc


def _ensure_packages(requirements: Iterable[tuple[str, str]]) -> None:
    missing: list[str] = []
    for module_name, package_name in requirements:
        try:
            spec = importlib.util.find_spec(module_name)
        except ModuleNotFoundError:
            spec = None
        if spec is None:
            missing.append(package_name)
    if not missing:
        return

    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing])
    except subprocess.CalledProcessError as exc:
        pytest.skip(f"Unable to install required packages {missing!r}: {exc}")


_REQUIREMENTS: dict[str, tuple[tuple[str, str], ...]] = {
    "connectorx": (
        ("connectorx", "connectorx"),
        ("pyarrow", "pyarrow"),
    ),
    "adbc": (
        ("adbc_driver_sqlite.dbapi", "adbc-driver-sqlite"),
        ("pyarrow", "pyarrow"),
    ),
    "postgres_psycopg": (
        ("psycopg2", "psycopg2-binary"),
    ),
    "postgres_testcontainers": (
        ("testcontainers.postgres", "testcontainers"),
    ),
    "postgres_connectorx": (
        ("connectorx", "connectorx"),
        ("pyarrow", "pyarrow"),
    ),
    "postgres_adbc": (
        ("adbc_driver_postgresql.dbapi", "adbc-driver-postgresql"),
        ("pyarrow", "pyarrow"),
    ),
}


def _external_postgres_config() -> dict[str, str] | None:
    host = os.getenv("LAKESAIL_TEST_PG_HOST")
    if not host:
        return None

    return {
        "host": host,
        "port": os.getenv("LAKESAIL_TEST_PG_PORT", "5432"),
        "database": os.getenv("LAKESAIL_TEST_PG_DATABASE", "postgres"),
        "user": os.getenv("LAKESAIL_TEST_PG_USER", "postgres"),
        "password": os.getenv("LAKESAIL_TEST_PG_PASSWORD", ""),
    }


@pytest.mark.parametrize(
    ("backend", "module"),
    [
        ("connectorx", "connectorx"),
        ("adbc", "adbc_driver_sqlite.dbapi"),
    ],
)
def test_sqlite_round_trip(tmp_path, backend: str, module: str) -> None:
    _ensure_packages(_REQUIREMENTS[backend])

    pytest.importorskip(module)
    pytest.importorskip("pyarrow")

    db_path = tmp_path / "events.db"

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE events (id INTEGER PRIMARY KEY, kind TEXT, amount INTEGER)")
    cur.executemany(
        "INSERT INTO events (id, kind, amount) VALUES (?, ?, ?)",
        [
            (1, "click", 10),
            (2, "view", 20),
            (3, "click", 15),
            (4, "purchase", 30),
        ],
    )
    conn.commit()
    conn.close()

    reader = jdbc(
        url=f"jdbc:sqlite:///{db_path.as_posix()}",
        table="events",
        column="id",
        lowerBound=1,
        upperBound=4,
        numPartitions=2,
        backend=backend,
    )

    table = reader.to_arrow()
    assert table.num_rows == 4

    rows = sorted(table.to_pylist(), key=lambda row: row["id"])
    assert [row["kind"] for row in rows] == ["click", "view", "click", "purchase"]
    assert [row["amount"] for row in rows] == [10, 20, 15, 30]


@pytest.mark.parametrize(
    ("backend", "module"),
    [
        ("connectorx", "connectorx"),
        ("adbc", "adbc_driver_postgresql.dbapi"),
    ],
)
def test_postgres_round_trip(backend: str, module: str) -> None:
    config = _external_postgres_config()

    _ensure_packages(_REQUIREMENTS["postgres_psycopg"])
    _ensure_packages(_REQUIREMENTS[f"postgres_{backend}"])
    if config is None:
        _ensure_packages(_REQUIREMENTS["postgres_testcontainers"])

    pytest.importorskip(module)
    pytest.importorskip("pyarrow")
    pytest.importorskip("psycopg2")

    import psycopg2

    if config is None:
        pytest.importorskip("testcontainers.postgres")

        try:
            from docker.errors import DockerException  # type: ignore
        except Exception:  # pragma: no cover - docker is optional
            DockerException = Exception  # type: ignore

        from testcontainers.postgres import PostgresContainer

        try:
            with PostgresContainer("postgres:15") as postgres:
                conn_url = postgres.get_connection_url()
                if conn_url.startswith("postgresql+psycopg2://"):
                    conn_url = conn_url.replace("postgresql+psycopg2://", "postgresql://", 1)

                parsed = urlparse(conn_url)
                props = {}
                if parsed.username:
                    props["user"] = parsed.username
                if parsed.password:
                    props["password"] = parsed.password

                with psycopg2.connect(conn_url) as conn:
                    with conn.cursor() as cur:
                        cur.execute("DROP TABLE IF EXISTS public.events")
                        cur.execute(
                            "CREATE TABLE events (id SERIAL PRIMARY KEY, kind TEXT, amount INTEGER)"
                        )
                        cur.executemany(
                            "INSERT INTO events (id, kind, amount) VALUES (%s, %s, %s)",
                            [
                                (1, "click", 10),
                                (2, "view", 20),
                                (3, "click", 15),
                                (4, "purchase", 30),
                            ],
                        )
                        conn.commit()

                jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"

                reader = jdbc(
                    url=jdbc_url,
                    table="public.events",
                    properties=props,
                    column="id",
                    lowerBound=1,
                    upperBound=4,
                    numPartitions=2,
                    backend=backend,
                )

                table = reader.to_arrow()
        except DockerException as exc:  # pragma: no cover - depends on environment
            pytest.skip(f"Docker is required for Postgres integration test: {exc}")
    else:
        props = {}
        if config.get("user"):
            props["user"] = config["user"]
        if config.get("password"):
            props["password"] = config["password"]

        jdbc_url = (
            f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
        )

        with psycopg2.connect(
            host=config["host"],
            port=config["port"],
            dbname=config["database"],
            user=config.get("user") or None,
            password=config.get("password") or None,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS public.events")
                cur.execute(
                    "CREATE TABLE public.events (id SERIAL PRIMARY KEY, kind TEXT, amount INTEGER)"
                )
                cur.executemany(
                    "INSERT INTO public.events (id, kind, amount) VALUES (%s, %s, %s)",
                    [
                        (1, "click", 10),
                        (2, "view", 20),
                        (3, "click", 15),
                        (4, "purchase", 30),
                    ],
                )
                conn.commit()

        reader = jdbc(
            url=jdbc_url,
            table="public.events",
            properties=props,
            column="id",
            lowerBound=1,
            upperBound=4,
            numPartitions=2,
            backend=backend,
        )

        table = reader.to_arrow()

    assert table.num_rows == 4
    rows = sorted(table.to_pylist(), key=lambda row: row["id"])
    assert [row["kind"] for row in rows] == ["click", "view", "click", "purchase"]
    assert [row["amount"] for row in rows] == [10, 20, 15, 30]
