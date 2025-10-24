"""Planner for JDBC read execution."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from ._config import DBConfig
from ._utils import (
    NormalizedJDBCUrl,
    build_select_statement,
    combine_clauses,
    compute_partition_ranges,
)


@dataclass(slots=True)
class Partition:
    sql: str
    description: str


@dataclass(slots=True)
class JDBCScanPlan:
    backend: str
    driver: str
    connection_url: str
    redacted_url: str
    connection_kwargs: Dict[str, str]
    partitions: List[Partition]
    fetch_chunk_rows: int
    retries: int
    retry_backoff_sec: float
    parallelism: int
    preserve_case: bool


class JDBCOptionsError(ValueError):
    """Raised when JDBC options are invalid."""


def _validate_partitioning(opts: Dict[str, Any]) -> None:
    has_column = "column" in opts
    has_bounds = {key: key in opts for key in ("lowerBound", "upperBound", "numPartitions")}
    if has_column and not all(has_bounds.values()):
        missing = [key for key, present in has_bounds.items() if not present]
        raise JDBCOptionsError(
            "partitioning requires column, lowerBound, upperBound, and numPartitions" f"; missing {missing}"
        )
    if any(has_bounds.values()) and not has_column:
        raise JDBCOptionsError(
            "partitioning bounds require 'column' to be specified"
        )


def _build_partitions(opts: Dict[str, Any]) -> List[Partition]:
    base_select, where_clause, predicates = build_select_statement(opts)
    base_where_clauses = combine_clauses(where_clause)

    partitions: List[Partition] = []

    predicate_clauses = predicates if predicates else [None]

    range_clauses: List[str | None]
    if "column" in opts:
        ranges = compute_partition_ranges(opts["lowerBound"], opts["upperBound"], opts["numPartitions"])
        range_clauses = [
            f"({opts['column']} >= {rng.start} AND {opts['column']} < {rng.end})"
            for rng in ranges
        ]
    else:
        range_clauses = [None]

    limit = opts.get("limit")

    for predicate in predicate_clauses:
        predicate_clause = predicate.strip() if isinstance(predicate, str) else None
        for range_clause in range_clauses:
            clauses = combine_clauses(*base_where_clauses, predicate_clause, range_clause)
            sql = base_select
            if clauses:
                sql = f"{sql} WHERE {' AND '.join(clauses)}"
            if limit is not None:
                sql = f"{sql} LIMIT {limit}"
            description_parts = []
            if predicate_clause:
                description_parts.append(predicate_clause)
            if range_clause:
                description_parts.append(range_clause)
            description = " AND ".join(description_parts) if description_parts else "all rows"
            partitions.append(Partition(sql=sql, description=description))

    return partitions


def build_scan_plan(
    *,
    opts: Dict[str, Any],
    backend: str,
    normalized: NormalizedJDBCUrl,
    config: DBConfig,
) -> JDBCScanPlan:
    _validate_partitioning(opts)

    partitions = _build_partitions(opts)
    if not partitions:
        raise JDBCOptionsError("no partitions generated for the provided options")

    fetch_chunk_rows = opts.get("fetch_chunk_rows", config.fetch_chunk_rows)
    retries = opts.get("retries", config.retries)
    retry_backoff = opts.get("retry_backoff_sec", config.retry_backoff_sec)
    preserve_case = opts.get("preserve_case", config.preserve_case)

    parallelism_hint = opts.get("numPartitions") or len(partitions)
    max_parallelism = opts.get("max_parallelism", config.max_parallelism)
    parallelism = max(1, min(max_parallelism, parallelism_hint, len(partitions)))

    return JDBCScanPlan(
        backend=backend,
        driver=normalized.driver,
        connection_url=normalized.connection_url,
        redacted_url=normalized.redacted_url,
        connection_kwargs=normalized.connection_kwargs,
        partitions=partitions,
        fetch_chunk_rows=fetch_chunk_rows,
        retries=retries,
        retry_backoff_sec=retry_backoff,
        parallelism=parallelism,
        preserve_case=preserve_case,
    )
