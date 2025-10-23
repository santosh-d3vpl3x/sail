from __future__ import annotations

import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")

from pysail.db.jdbc import read_arrow_batches


@pytest.fixture(autouse=True)
def clear_connectorx(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delitem(sys.modules, "connectorx", raising=False)


def test_read_arrow_batches_single_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, str] = {}

    def fake_read_sql(uri: str, sql: str, **_: object) -> pa.Table:
        recorded["uri"] = uri
        recorded["sql"] = sql
        return pa.table({"id": pa.array([1, 2], type=pa.int64())})

    fake_module = types.SimpleNamespace(read_sql=fake_read_sql)
    monkeypatch.setitem(sys.modules, "connectorx", fake_module)

    partitions, schema = read_arrow_batches(
        "jdbc:postgresql://localhost:5432/example",
        table="public.users",
        options={"user": "alice", "password": "secret"},
    )

    assert schema.names == ["id"]
    assert len(partitions) == 1
    assert partitions[0][0].num_rows == 2
    assert recorded["uri"].startswith("postgresql://alice:secret@localhost:5432/example")
    assert "FROM public.users" in recorded["sql"]


def test_read_arrow_batches_partition_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_sql: list[str] = []

    def fake_read_sql(uri: str, sql: str, **_: object) -> pa.Table:
        captured_sql.append(sql)
        return pa.table({"value": pa.array([], type=pa.int64())}, schema=pa.schema([("value", pa.int64())]))

    fake_module = types.SimpleNamespace(read_sql=fake_read_sql)
    monkeypatch.setitem(sys.modules, "connectorx", fake_module)

    partitions, schema = read_arrow_batches(
        "jdbc:postgresql://localhost:5432/example",
        table="metrics",
        partitioning={
            "column": "id",
            "lower_bound": 0,
            "upper_bound": 10,
            "num_partitions": 2,
        },
    )

    assert len(partitions) == 2
    assert schema.field("value").type == pa.int64()
    assert any("WHERE id >= 0" in sql for sql in captured_sql)
    assert any("WHERE id >= 5" in sql for sql in captured_sql)


def test_read_arrow_batches_missing_connector() -> None:
    with pytest.raises(RuntimeError) as excinfo:
        read_arrow_batches(
            "jdbc:sqlite://:memory:",
            table="numbers",
            options={"engine": "connectorx"},
        )
    assert "connectorx" in str(excinfo.value)
