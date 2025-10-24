"""Utility functions for JDBC reads."""
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Dict, Iterable
from urllib.parse import ParseResult, parse_qsl, quote, urlencode, urlparse, urlunparse


@dataclass(slots=True)
class NormalizedJDBCUrl:
    driver: str
    connection_url: str
    redacted_url: str
    connection_kwargs: Dict[str, str]


_OPTION_KEY_ALIASES = {
    "dbtable": "table",
    "partitioncolumn": "column",
    "numpartitions": "numPartitions",
    "num_partitions": "numPartitions",
    "lowerbound": "lowerBound",
    "upperbound": "upperBound",
    "fetchsize": "fetch_chunk_rows",
}


def canonicalize_option_key(key: str) -> str:
    normalized = key.strip()
    normalized = normalized.replace(" ", "")
    normalized = normalized.replace("-", "")
    normalized_lower = normalized.lower()
    return _OPTION_KEY_ALIASES.get(normalized_lower, normalized)


def coerce_options(options: Dict[str, Any]) -> Dict[str, Any]:
    opts: Dict[str, Any] = {}
    for key, value in options.items():
        canonical = canonicalize_option_key(key)
        opts[canonical] = value

    if "dbtable" in opts and "table" not in opts:
        opts["table"] = opts.pop("dbtable")

    if "partitionColumn" in opts and "column" not in opts:
        opts["column"] = opts.pop("partitionColumn")

    if "url" not in opts:
        raise ValueError("'url' is a required option for JDBC reads")

    if "table" not in opts and "query" not in opts:
        raise ValueError("either 'table' or 'query' must be provided")

    if "table" in opts and "query" in opts:
        raise ValueError("only one of 'table' or 'query' can be provided")

    if "properties" in opts and not isinstance(opts["properties"], dict):
        raise TypeError("'properties' must be a mapping of connection properties")

    def _coerce_int(name: str) -> None:
        if name in opts and not isinstance(opts[name], int):
            try:
                opts[name] = int(opts[name])
            except Exception as exc:  # pragma: no cover - defensive
                raise TypeError(f"option '{name}' must be an integer") from exc

    for name in ("lowerBound", "upperBound", "numPartitions", "limit", "fetch_chunk_rows"):
        _coerce_int(name)

    if "predicates" in opts and isinstance(opts["predicates"], str):
        opts["predicates"] = [opts["predicates"]]

    if "columns" in opts and isinstance(opts["columns"], str):
        opts["columns"] = [col.strip() for col in opts["columns"].split(",") if col.strip()]

    return opts


def normalize_jdbc_url(url: str, properties: Dict[str, str]) -> NormalizedJDBCUrl:
    if not url.startswith("jdbc:"):
        raise ValueError("JDBC URLs must start with 'jdbc:'")

    trimmed = url[5:]
    parsed = urlparse(trimmed)
    driver = parsed.scheme or trimmed.split(":", 1)[0]

    query_params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    connection_kwargs = {k: str(v) for k, v in properties.items()}

    user = connection_kwargs.get("user") or connection_kwargs.get("username")
    password = connection_kwargs.get("password") or connection_kwargs.get("passwd")

    for key in ("user", "username", "password", "passwd"):
        connection_kwargs.pop(key, None)

    query_params.update(connection_kwargs)

    netloc = parsed.netloc
    if user or password:
        host_part = parsed.hostname or ""
        if parsed.port:
            host_part = f"{host_part}:{parsed.port}"
        userinfo = quote(user or "")
        if password is not None:
            userinfo = f"{userinfo}:{quote(password)}"
        netloc = f"{userinfo}@{host_part}" if host_part else userinfo
    elif parsed.username or parsed.password:
        auth = quote(parsed.username or "")
        if parsed.password:
            auth = f"{auth}:{quote(parsed.password)}"
        host_part = parsed.hostname or ""
        if parsed.port:
            host_part = f"{host_part}:{parsed.port}"
        netloc = f"{auth}@{host_part}" if host_part else auth

    rebuilt = _rebuild_url(parsed, netloc, query_params)
    redacted = redact_credentials(rebuilt)

    return NormalizedJDBCUrl(
        driver=driver.lower(),
        connection_url=rebuilt,
        redacted_url=redacted,
        connection_kwargs={k: str(v) for k, v in properties.items()},
    )


def _rebuild_url(parsed: ParseResult, netloc: str, query_params: Dict[str, str]) -> str:
    new_query = urlencode(query_params, doseq=True)
    rebuilt = parsed._replace(netloc=netloc, query=new_query)
    url = urlunparse(rebuilt)

    if not netloc and parsed.scheme and rebuilt.path.startswith("/"):
        # ``urlunparse`` collapses URLs without a network location into
        # ``scheme:/path``. Many database drivers (ConnectorX, ADBC) expect the
        # triple-slash form for absolute file paths (e.g. sqlite:///file.db), so
        # we reintroduce it here when applicable.
        prefix = f"{parsed.scheme}:///"
        path = rebuilt.path.lstrip("/")
        url = prefix + path
        if new_query:
            url = f"{url}?{new_query}"

    return url


_CREDENTIAL_PATTERN = re.compile(r"(?P<prefix>://[^/@]*:)(?P<secret>[^@]+)(?=@)")


def redact_credentials(url: str) -> str:
    return _CREDENTIAL_PATTERN.sub(lambda match: f"{match.group('prefix')}***", url)


@dataclass(slots=True)
class PartitionRange:
    start: int
    end: int


def compute_partition_ranges(lower: int, upper: int, partitions: int) -> list[PartitionRange]:
    if partitions <= 0:
        raise ValueError("numPartitions must be positive")
    if upper < lower:
        raise ValueError("upperBound must be >= lowerBound")

    span = upper - lower + 1
    step = max(1, math.ceil(span / partitions))

    ranges: list[PartitionRange] = []
    current = lower
    while current <= upper:
        end = min(current + step, upper + 1)
        ranges.append(PartitionRange(start=current, end=end))
        current = end
    return ranges


def combine_clauses(*clauses: Iterable[str | None]) -> list[str]:
    parts: list[str] = []
    for clause in clauses:
        if clause:
            text = clause.strip()
            if text:
                parts.append(text)
    return parts


def build_select_statement(opts: Dict[str, Any]) -> tuple[str, str | None, list[str]]:
    columns = opts.get("columns") or []
    projection = "*" if not columns else ", ".join(columns)

    if "table" in opts:
        base = f"SELECT {projection} FROM {opts['table']}"
    else:
        query = opts["query"].strip()
        base = f"SELECT {projection} FROM ( {query} ) AS lakesail_subquery"

    where_clause = opts.get("where")
    predicates = opts.get("predicates") or []
    if not isinstance(predicates, list):
        predicates = [predicates]
    return base, where_clause, predicates
