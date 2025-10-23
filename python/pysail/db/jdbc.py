"""Helpers for reading relational databases."""

from __future__ import annotations

import importlib
from decimal import Decimal
from typing import Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple
from urllib.parse import quote, urlparse, urlunparse

import pyarrow as pa

_PARTITION_ALIAS = "spark_jdbc_partition"


def read_arrow_batches(
    url: str,
    *,
    table: Optional[str] = None,
    query: Optional[str] = None,
    paths: Optional[Sequence[str]] = None,
    options: Optional[Mapping[str, str]] = None,
    partitioning: Optional[Mapping[str, object]] = None,
    schema: Optional[pa.Schema] = None,
) -> Tuple[List[List[pa.RecordBatch]], pa.Schema]:
    """Read data from a database into Arrow record batches.

    Parameters
    ----------
    url:
        The JDBC connection string.
    table:
        Optional table name. Mutually exclusive with ``query``.
    query:
        Optional SQL query text.
    paths:
        Additional path arguments supplied via ``DataFrameReader.load``.
    options:
        Reader options from Spark.
    partitioning:
        Partitioning configuration produced by the Rust layer.
    schema:
        Optional target schema to cast the data to.
    """

    normalized = _normalize_options(options)
    connection_uri = _build_connection_uri(url, normalized)
    base_query = _resolve_base_query(table, query, paths, normalized)
    partition_plan = _merge_partitioning(partitioning, normalized)

    engine = normalized.pop("engine", "connectorx")
    queries = _build_partition_queries(base_query, partition_plan)

    errors: List[str] = []
    if engine in ("connectorx", "auto"):
        try:
            return _read_with_connectorx(connection_uri, queries, normalized, schema)
        except ImportError as exc:  # pragma: no cover - depends on environment
            errors.append(f"connectorx import failed: {exc}")
        except Exception as exc:  # pragma: no cover - propagated to caller when engine != auto
            errors.append(str(exc))
            if engine != "auto":
                raise
    if engine in ("adbc", "auto"):
        try:
            return _read_with_adbc(connection_uri, queries, normalized, schema)
        except ImportError as exc:  # pragma: no cover - depends on environment
            errors.append(f"adbc import failed: {exc}")
        except Exception as exc:  # pragma: no cover - propagated to caller when engine != auto
            errors.append(str(exc))
            if engine != "auto":
                raise

    details = f" ({'; '.join(errors)})" if errors else ""
    raise RuntimeError(
        "Unable to read database using connectorx or ADBC" + details,
    )


def _normalize_options(options: Optional[Mapping[str, str]]) -> MutableMapping[str, str]:
    normalized: MutableMapping[str, str] = {}
    if not options:
        return normalized
    for key, value in options.items():
        if value is None:
            continue
        normalized[key.lower()] = value
    return normalized


def _build_connection_uri(url: str, options: MutableMapping[str, str]) -> str:
    if not url:
        raise ValueError("JDBC option 'url' is required")
    uri = url[5:] if url.lower().startswith("jdbc:") else url
    parsed = urlparse(uri)
    user = options.pop("user", None)
    password = options.pop("password", None)
    if (user or password) and not parsed.username:
        netloc = parsed.netloc
        if netloc and "@" not in netloc:
            credential = quote(user or "")
            if password:
                credential = f"{credential}:{quote(password)}"
            netloc = f"{credential}@{netloc}"
            parsed = parsed._replace(netloc=netloc)
    return urlunparse(parsed)


def _resolve_base_query(
    table: Optional[str],
    query: Optional[str],
    paths: Optional[Sequence[str]],
    options: MutableMapping[str, str],
) -> str:
    if query:
        return query
    relation = table or options.pop("dbtable", None) or options.pop("table", None)
    if relation is None and paths:
        relation = paths[0]
    if relation is None:
        raise ValueError("JDBC option 'dbtable' or 'query' is required")
    relation = relation.strip()
    if relation.startswith("("):
        return relation
    return f"SELECT * FROM {relation}"


def _merge_partitioning(
    partitioning: Optional[Mapping[str, object]],
    options: MutableMapping[str, str],
) -> Mapping[str, object]:
    if partitioning:
        return dict(partitioning)

    column = options.pop("partitioncolumn", None)
    lower = options.pop("lowerbound", None)
    upper = options.pop("upperbound", None)
    partitions = options.pop("numpartitions", None)
    if column and lower is not None and upper is not None and partitions is not None:
        return {
            "column": column,
            "lower_bound": lower,
            "upper_bound": upper,
            "num_partitions": int(partitions),
        }
    return {}


def _build_partition_queries(
    base_query: str,
    partitioning: Mapping[str, object],
) -> List[str]:
    if not partitioning:
        return [base_query]

    predicates = partitioning.get("predicates")
    if isinstance(predicates, Iterable):
        queries = [
            f"SELECT * FROM ({base_query}) AS {_PARTITION_ALIAS} WHERE {str(predicate).strip()}"
            for predicate in predicates
            if str(predicate).strip()
        ]
        return queries or [base_query]

    column = partitioning.get("column")
    lower = partitioning.get("lower_bound")
    upper = partitioning.get("upper_bound")
    partitions = partitioning.get("num_partitions")
    if not column or lower is None or upper is None or not partitions:
        return [base_query]

    try:
        total_partitions = int(partitions)
    except ValueError as exc:  # pragma: no cover - validation happens in Rust
        raise ValueError("invalid partition count") from exc
    if total_partitions <= 0:
        return [base_query]

    lower_dec = Decimal(str(lower))
    upper_dec = Decimal(str(upper))
    step = (upper_dec - lower_dec) / Decimal(total_partitions)
    queries: List[str] = []
    for index in range(total_partitions):
        start = lower_dec + step * Decimal(index)
        if index == total_partitions - 1:
            predicate = (
                f"{column} >= {_format_decimal(start)} AND {column} <= {_format_decimal(upper_dec)}"
            )
        else:
            end = lower_dec + step * Decimal(index + 1)
            predicate = (
                f"{column} >= {_format_decimal(start)} AND {column} < {_format_decimal(end)}"
            )
        queries.append(
            f"SELECT * FROM ({base_query}) AS {_PARTITION_ALIAS} WHERE {predicate}"
        )
    return queries or [base_query]


def _format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    text = format(normalized, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def _read_with_connectorx(
    uri: str,
    queries: Sequence[str],
    options: Mapping[str, str],
    schema: Optional[pa.Schema],
) -> Tuple[List[List[pa.RecordBatch]], pa.Schema]:
    import connectorx  # type: ignore

    batches: List[List[pa.RecordBatch]] = []
    final_schema = schema
    protocol = options.get("protocol", "binary")

    for sql in queries:
        table = connectorx.read_sql(  # type: ignore[call-arg]
            uri,
            sql,
            return_type="arrow2",
            protocol=protocol,
        )
        if schema is not None:
            table = table.cast(schema, safe=False)
        elif final_schema is None:
            final_schema = table.schema
        batches.append(table.to_batches())

    if final_schema is None:
        raise RuntimeError("Unable to determine schema from JDBC result")
    return batches, final_schema


def _read_with_adbc(
    uri: str,
    queries: Sequence[str],
    options: Mapping[str, str],
    schema: Optional[pa.Schema],
) -> Tuple[List[List[pa.RecordBatch]], pa.Schema]:
    driver = options.get("adbc_driver")
    if not driver:
        raise RuntimeError("Option 'adbc_driver' is required when engine='adbc'")

    module = importlib.import_module(driver)
    connect_kwargs = {
        key[5:]: value
        for key, value in options.items()
        if key.startswith("adbc.") and key != "adbc.driver"
    }
    connection = module.connect(uri, **connect_kwargs)

    try:
        batches: List[List[pa.RecordBatch]] = []
        final_schema = schema
        for sql in queries:
            cursor = connection.cursor()
            try:
                cursor.execute(sql)
                table = cursor.fetch_arrow_table()
            finally:
                cursor.close()
            if schema is not None:
                table = table.cast(schema, safe=False)
            elif final_schema is None:
                final_schema = table.schema
            batches.append(table.to_batches())
        if final_schema is None:
            raise RuntimeError("Unable to determine schema from JDBC result")
        return batches, final_schema
    finally:
        connection.close()
