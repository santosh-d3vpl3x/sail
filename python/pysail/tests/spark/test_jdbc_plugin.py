from __future__ import annotations

from typing import Any, Callable, Dict

import pytest


@pytest.mark.usefixtures("spark")
def test_jdbc_register_reader_invokes_plugin(monkeypatch):
    """Ensure custom JDBC readers integrate with the Spark coverage suite."""

    pytest.importorskip("pyarrow")
    import pyarrow as pa

    from pysail.read import jdbc, register_reader
    from pysail.read import _backends

    # Ensure backend auto-selection succeeds without optional dependencies.
    monkeypatch.setattr(
        _backends.ADBCBackend,
        "is_available",
        classmethod(lambda cls, scheme: False),
    )
    monkeypatch.setattr(
        _backends.ConnectorXBackend,
        "is_available",
        classmethod(lambda cls, scheme: scheme == "postgresql"),
    )

    original_registry: Dict[str, Callable[[Any], Any]] = dict(_backends._PLUGIN_REGISTRY)
    captured_plan: Dict[str, Any] = {}

    def plugin(plan):
        captured_plan["plan"] = plan
        return pa.table({"id": [1, 2], "kind": ["click", "view"]})

    register_reader("postgresql", plugin)

    try:
        reader = jdbc(
            url="jdbc:postgresql://example.com:5432/app",
            table="public.events",
            properties={"user": "scott", "password": "tiger"},
            predicates=["kind = 'click'"],
            column="id",
            lowerBound=1,
            upperBound=10,
            numPartitions=2,
            backend="auto",
        )
        table = reader.to_arrow()
        assert table.to_pylist() == [
            {"id": 1, "kind": "click"},
            {"id": 2, "kind": "view"},
        ]

        plan = captured_plan.get("plan")
        assert plan is not None, "custom JDBC reader was not invoked"
        assert plan.driver == "postgresql"
        assert plan.partitions, "plan should generate partitions"
        assert plan.partitions[0].sql.startswith("SELECT"), "partition SQL should be generated"
    finally:
        _backends._PLUGIN_REGISTRY.clear()
        _backends._PLUGIN_REGISTRY.update(original_registry)
