"""Database read helpers for Lakesail."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Literal, TypedDict

from . import _backends, _config, _plan, _utils

__all__ = [
    "JDBCOptions",
    "JDBCReader",
    "JDBCReaderBuilder",
    "jdbc",
    "register_reader",
]

BackendLiteral = Literal["connectorx", "adbc", "auto"]


class JDBCOptions(TypedDict, total=False):
    """Options accepted by :func:`jdbc`."""

    url: str
    table: str
    dbtable: str
    query: str
    properties: dict[str, str]
    column: str
    partitionColumn: str
    lowerBound: int
    upperBound: int
    numPartitions: int
    predicates: list[str]
    where: str
    columns: list[str]
    limit: int
    fetch_chunk_rows: int
    preserve_case: bool
    backend: BackendLiteral
    retries: int
    retry_backoff_sec: float
    max_parallelism: int


@dataclass
class JDBCReader:
    """Represents the result of a JDBC read operation."""

    table: Any

    def to_arrow(self) -> Any:
        """Return the underlying Arrow table."""

        return self.table

    def __getattr__(self, item: str) -> Any:
        return getattr(self.table, item)

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"JDBCReader(table={self.table!r})"


class JDBCReaderBuilder:
    """Convenience builder that mimics ``spark.read.jdbc`` ergonomics."""

    def __init__(self) -> None:
        self._options: dict[str, Any] = {}

    def option(self, key: str, value: Any) -> "JDBCReaderBuilder":
        self._options[_utils.canonicalize_option_key(key)] = value
        return self

    def options(self, **options: Any) -> "JDBCReaderBuilder":
        for key, value in options.items():
            self.option(key, value)
        return self

    def load(self) -> JDBCReader:
        return jdbc(**self._options)


def register_reader(dbms: str, factory: Callable[["_plan.JDBCScanPlan"], Any]) -> None:
    """Register a custom database reader factory.

    Parameters
    ----------
    dbms:
        Database identifier (e.g. ``"postgresql"``).
    factory:
        Callable that receives the :class:`~pysail.read._plan.JDBCScanPlan` and
        returns an Arrow table. Returning ``None`` will fall back to the default
        backend implementation.
    """

    _backends.register_reader(dbms, factory)


def jdbc(**options: Any) -> JDBCReader:
    """Read from a JDBC source using optional ConnectorX or ADBC backends."""

    opts = _utils.coerce_options(options)
    config = _config.load_config()

    properties = dict(opts.get("properties", {}))
    normalized = _utils.normalize_jdbc_url(opts["url"], properties)

    requested_backend = opts.get("backend", config.default_backend)
    backend = _backends.pick_backend(requested_backend, normalized.driver)

    plan = _plan.build_scan_plan(
        opts=opts,
        backend=backend,
        normalized=normalized,
        config=config,
    )

    table = _backends.execute_plan(plan)
    return JDBCReader(table=table)
