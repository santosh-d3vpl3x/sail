"""Backend management for JDBC reads."""
from __future__ import annotations

import importlib
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Optional
from urllib.parse import urlparse

from ._plan import JDBCScanPlan, Partition


class BackendUnavailableError(RuntimeError):
    """Raised when a requested backend is not available."""


@dataclass(frozen=True)
class BackendCapability:
    name: str
    supported_schemes: set[str]

    def is_supported(self, scheme: str) -> bool:
        return scheme in self.supported_schemes


_PLUGIN_REGISTRY: Dict[str, Callable[[JDBCScanPlan], Any]] = {}


def register_reader(dbms: str, factory: Callable[[JDBCScanPlan], Any]) -> None:
    _PLUGIN_REGISTRY[dbms.lower()] = factory


class ConnectorXBackend:
    name = "connectorx"
    CAPABILITY = BackendCapability(
        name=name,
        supported_schemes={
            "postgresql",
            "postgres",
            "mysql",
            "mariadb",
            "sqlite",
            "mssql",
            "redshift",
            "clickhouse",
        },
    )

    @classmethod
    def is_available(cls, scheme: str) -> bool:
        if not cls.CAPABILITY.is_supported(scheme):
            return False
        return importlib.util.find_spec("connectorx") is not None

    @classmethod
    def requirement_hint(cls, scheme: str) -> str:
        if not cls.CAPABILITY.is_supported(scheme):
            return f"ConnectorX does not support JDBC scheme '{scheme}'."
        return (
            "ConnectorX is not available. Install it with `pip install connectorx` "
            "or choose backend='adbc'."
        )

    def execute(self, plan: JDBCScanPlan) -> Any:
        connectorx = importlib.import_module("connectorx")

        def fetch(partition: Partition) -> Any:
            return connectorx.read_sql(
                plan.connection_url,
                partition.sql,
                return_type="arrow",
            )

        return _execute_partitions(plan, fetch)


_ADBC_DRIVER_MODULES = {
    "postgresql": "adbc_driver_postgresql.dbapi",
    "postgres": "adbc_driver_postgresql.dbapi",
    "sqlite": "adbc_driver_sqlite.dbapi",
    "flightsql": "adbc_driver_flightsql.dbapi",
    "snowflake": "adbc_driver_snowflake.dbapi",
    "bigquery": "adbc_driver_bigquery.dbapi",
}


class ADBCBackend:
    name = "adbc"
    CAPABILITY = BackendCapability(name=name, supported_schemes=set(_ADBC_DRIVER_MODULES))

    @classmethod
    def is_available(cls, scheme: str) -> bool:
        module_name = _ADBC_DRIVER_MODULES.get(scheme)
        if module_name is None:
            return False
        return importlib.util.find_spec(module_name) is not None

    @classmethod
    def requirement_hint(cls, scheme: str) -> str:
        module_name = _ADBC_DRIVER_MODULES.get(scheme)
        if module_name is None:
            return f"ADBC driver unavailable for JDBC scheme '{scheme}'."
        package_name = module_name.split(".")[0].replace("_", "-")
        return (
            f"ADBC driver for '{scheme}' is not installed. Install `{package_name}` or set backend='connectorx'."
        )

    def execute(self, plan: JDBCScanPlan) -> Any:
        module_name = _ADBC_DRIVER_MODULES.get(plan.driver)
        if module_name is None:
            raise BackendUnavailableError(
                f"No ADBC driver configured for scheme '{plan.driver}'"
            )
        dbapi = importlib.import_module(module_name)

        def _normalize_uri(uri: str) -> str:
            if plan.driver != "sqlite":
                return uri
            parsed = urlparse(uri)
            if parsed.scheme != "sqlite":
                return uri
            path = parsed.path or ""
            if parsed.netloc:
                path = f"//{parsed.netloc}{path}"
            if parsed.query:
                path = f"{path}?{parsed.query}"
            return f"file:{path}"

        def fetch(partition: Partition) -> Any:
            uri = _normalize_uri(plan.connection_url)
            with dbapi.connect(uri=uri) as conn:  # type: ignore[call-arg]
                with conn.cursor() as cursor:  # type: ignore[call-arg]
                    cursor.execute(partition.sql)  # type: ignore[arg-type]
                    try:
                        import pyarrow as pa
                    except ImportError as exc:  # pragma: no cover - optional dependency
                        raise BackendUnavailableError(
                            "pyarrow is required to consume Arrow results from ADBC"
                        ) from exc
                    table = cursor.fetchallarrow()
                    if table is None:
                        return pa.table({})
                    return table

        return _execute_partitions(plan, fetch)


_BACKENDS: Dict[str, Callable[[], Any]] = {
    ConnectorXBackend.name: ConnectorXBackend,
    ADBCBackend.name: ADBCBackend,
}


def pick_backend(requested: str, scheme: str) -> str:
    requested = requested.lower()
    if requested == "auto":
        if ADBCBackend.is_available(scheme):
            return ADBCBackend.name
        if ConnectorXBackend.is_available(scheme):
            return ConnectorXBackend.name
        raise BackendUnavailableError(_format_unavailable_message(scheme))

    if requested not in _BACKENDS:
        raise BackendUnavailableError(f"Unknown backend '{requested}'")

    backend_cls = _BACKENDS[requested]
    if backend_cls is ConnectorXBackend and not ConnectorXBackend.is_available(scheme):
        raise BackendUnavailableError(ConnectorXBackend.requirement_hint(scheme))
    if backend_cls is ADBCBackend and not ADBCBackend.is_available(scheme):
        raise BackendUnavailableError(ADBCBackend.requirement_hint(scheme))

    return requested


def _format_unavailable_message(scheme: str) -> str:
    connectorx_hint = ConnectorXBackend.requirement_hint(scheme)
    adbc_hint = ADBCBackend.requirement_hint(scheme)
    return (
        f"No JDBC backends available for scheme '{scheme}'. "
        f"{connectorx_hint} {adbc_hint}"
    )


def execute_plan(plan: JDBCScanPlan) -> Any:
    plugin = _PLUGIN_REGISTRY.get(plan.driver)
    if plugin is not None:
        result = plugin(plan)
        if result is not None:
            return result

    backend_cls = _BACKENDS.get(plan.backend)
    if backend_cls is None:
        raise BackendUnavailableError(f"Unknown backend '{plan.backend}'")

    backend = backend_cls()
    return backend.execute(plan)


def _execute_partitions(plan: JDBCScanPlan, fetcher: Callable[[Partition], Any]) -> Any:
    if len(plan.partitions) == 1:
        table = _attempt_fetch(plan, plan.partitions[0], fetcher)
        return table

    with ThreadPoolExecutor(max_workers=plan.parallelism) as executor:
        future_map = {
            executor.submit(_attempt_fetch, plan, partition, fetcher): partition
            for partition in plan.partitions
        }
        results: list[Any] = []
        for future in as_completed(future_map):
            results.append(future.result())

    return _merge_tables(results)


def _attempt_fetch(plan: JDBCScanPlan, partition: Partition, fetcher: Callable[[Partition], Any]) -> Any:
    attempt = 0
    last_exc: Optional[Exception] = None
    while attempt <= plan.retries:
        try:
            return fetcher(partition)
        except Exception as exc:  # pragma: no cover - requires backend failure
            last_exc = exc
            attempt += 1
            if attempt > plan.retries:
                break
            delay = plan.retry_backoff_sec * attempt
            time.sleep(delay)
    redacted = plan.redacted_url
    raise BackendUnavailableError(
        f"Failed to read partition '{partition.description}' from {redacted}: {last_exc}"
    ) from last_exc


def _merge_tables(tables: Iterable[Any]) -> Any:
    tables = list(tables)
    try:
        import pyarrow as pa
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise BackendUnavailableError(
            "pyarrow is required to combine Arrow tables"
        ) from exc

    if not tables:
        return pa.table({})
    non_null = [table for table in tables if table is not None]
    if not non_null:
        return pa.table({})
    if len(non_null) == 1:
        return non_null[0]
    return pa.concat_tables(non_null, promote=True)
