from __future__ import annotations

import importlib.util

import pytest

from pysail.read import JDBCReaderBuilder
from pysail.read import _backends, _config, _plan, _utils


@pytest.fixture(autouse=True)
def clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LAKESAIL_DB_DEFAULT_BACKEND", raising=False)
    monkeypatch.delenv("LAKESAIL_DB_FETCH_CHUNK_ROWS", raising=False)
    monkeypatch.delenv("LAKESAIL_DB_RETRIES", raising=False)
    monkeypatch.delenv("LAKESAIL_DB_RETRY_BACKOFF_SEC", raising=False)
    monkeypatch.delenv("LAKESAIL_DB_MAX_PARALLELISM", raising=False)


def test_normalize_jdbc_url_injects_credentials() -> None:
    normalized = _utils.normalize_jdbc_url(
        "jdbc:postgresql://localhost:5432/db",
        {"user": "alice", "password": "secret", "sslmode": "require"},
    )
    assert normalized.driver == "postgresql"
    assert "alice" in normalized.connection_url
    assert "secret" not in normalized.redacted_url
    assert "sslmode=require" in normalized.connection_url


def test_partition_plan_with_predicates_and_ranges() -> None:
    opts = {
        "url": "jdbc:postgresql://localhost:5432/db",
        "table": "public.events",
        "column": "id",
        "lowerBound": 0,
        "upperBound": 9,
        "numPartitions": 2,
        "predicates": ["kind = 'click'", "kind = 'view'"],
    }
    config = _config.DBConfig(
        default_backend="auto",
        fetch_chunk_rows=10_000,
        retries=1,
        retry_backoff_sec=0.1,
        max_parallelism=8,
        preserve_case=False,
    )
    normalized = _utils.normalize_jdbc_url(opts["url"], {})
    plan = _plan.build_scan_plan(opts=opts, backend="connectorx", normalized=normalized, config=config)
    assert len(plan.partitions) == 4
    sql_samples = {partition.sql for partition in plan.partitions}
    assert any("kind = 'click'" in sql for sql in sql_samples)
    assert all("id" in sql for sql in sql_samples)


def test_pick_backend_prefers_adbc(monkeypatch: pytest.MonkeyPatch) -> None:
    original_find_spec = importlib.util.find_spec

    def fake_find_spec(name: str, package: str | None = None):
        if name == "adbc_driver_postgresql.dbapi":
            return object()
        if name == "connectorx":
            return object()
        return original_find_spec(name, package)  # pragma: no cover - passthrough

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    backend = _backends.pick_backend("auto", "postgresql")
    assert backend == "adbc"


def test_pick_backend_error_message(monkeypatch: pytest.MonkeyPatch) -> None:
    original_find_spec = importlib.util.find_spec

    def fake_find_spec(name: str, package: str | None = None):
        if name in {"connectorx", "adbc_driver_postgresql.dbapi"}:
            return None
        return original_find_spec(name, package)  # pragma: no cover - passthrough

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    with pytest.raises(_backends.BackendUnavailableError):
        _backends.pick_backend("auto", "postgresql")


def test_pick_backend_connectorx_missing_dependency_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_find_spec = importlib.util.find_spec

    def fake_find_spec(name: str, package: str | None = None):
        if name == "connectorx":
            return None
        return original_find_spec(name, package)  # pragma: no cover - passthrough

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    with pytest.raises(_backends.BackendUnavailableError) as exc:
        _backends.pick_backend("connectorx", "sqlite")

    assert "ConnectorX is not available" in str(exc.value)
    assert "backend='adbc'" in str(exc.value)


def test_pick_backend_adbc_missing_dependency_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_find_spec = importlib.util.find_spec

    def fake_find_spec(name: str, package: str | None = None):
        if name == "adbc_driver_sqlite.dbapi":
            return None
        return original_find_spec(name, package)  # pragma: no cover - passthrough

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    with pytest.raises(_backends.BackendUnavailableError) as exc:
        _backends.pick_backend("adbc", "sqlite")

    message = str(exc.value)
    assert "ADBC driver for 'sqlite' is not installed" in message
    assert "backend='connectorx'" in message


def test_builder_option_normalization(monkeypatch: pytest.MonkeyPatch) -> None:
    builder = JDBCReaderBuilder()
    builder.option("dbtable", "public.events")
    config = _config.DBConfig(
        default_backend="connectorx",
        fetch_chunk_rows=1000,
        retries=0,
        retry_backoff_sec=0.0,
        max_parallelism=4,
        preserve_case=False,
    )
    monkeypatch.setattr(_config, "load_config", lambda: config)
    monkeypatch.setattr(_backends, "pick_backend", lambda requested, scheme: "connectorx")
    monkeypatch.setattr(_backends, "execute_plan", lambda plan: "arrow_table")
    reader = builder.options(url="jdbc:postgresql://localhost:5432/db", properties={"user": "u"}).load()
    assert hasattr(reader, "to_arrow")
    assert reader.to_arrow() == "arrow_table"
