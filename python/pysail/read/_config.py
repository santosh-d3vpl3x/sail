"""Configuration helpers for database reads."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

DEFAULT_FETCH_CHUNK_ROWS = 100_000
DEFAULT_RETRIES = 3
DEFAULT_RETRY_BACKOFF = 1.5
DEFAULT_PARALLELISM_CAP = 32
DEFAULT_BACKEND: Literal["auto", "connectorx", "adbc"] = "auto"


@dataclass(slots=True)
class DBConfig:
    default_backend: Literal["auto", "connectorx", "adbc"]
    fetch_chunk_rows: int
    retries: int
    retry_backoff_sec: float
    max_parallelism: int
    preserve_case: bool


def _load_yaml_config() -> dict[str, Any]:
    """Attempt to load a YAML configuration file if available."""

    config_env = os.environ.get("LAKESAIL_CONFIG")
    candidate_paths: list[Path] = []
    if config_env:
        candidate_paths.append(Path(config_env).expanduser())
    candidate_paths.append(Path.home() / ".config" / "lakesail" / "config.yaml")

    for path in candidate_paths:
        if not path.is_file():
            continue
        try:
            import yaml  # type: ignore
        except ImportError:  # pragma: no cover - optional dependency
            return {}
        with path.open("r", encoding="utf-8") as handle:
            data = yaml.safe_load(handle)  # type: ignore[misc]
        if isinstance(data, dict):
            return data
    return {}


def _extract_db_section(raw: dict[str, Any]) -> dict[str, Any]:
    candidates = [
        raw.get("lakesail", {}).get("db", {}),
        raw.get("lakesail.db", {}),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return {}


def _env_override(key: str) -> str | None:
    return os.environ.get(f"LAKESAIL_DB_{key.upper()}")


def load_config() -> DBConfig:
    """Load configuration from env variables or YAML."""

    yaml_config = _extract_db_section(_load_yaml_config())

    def resolve_int(key: str, default: int) -> int:
        env_value = _env_override(key)
        if env_value is not None:
            try:
                return int(env_value)
            except ValueError:  # pragma: no cover - invalid env
                pass
        value = yaml_config.get(key, default)
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except Exception:  # pragma: no cover - invalid YAML value
            return default

    def resolve_float(key: str, default: float) -> float:
        env_value = _env_override(key)
        if env_value is not None:
            try:
                return float(env_value)
            except ValueError:  # pragma: no cover - invalid env
                pass
        value = yaml_config.get(key, default)
        if isinstance(value, (int, float)):
            return float(value)
        try:
            return float(value)
        except Exception:  # pragma: no cover - invalid YAML value
            return default

    def resolve_backend(default: Literal["auto", "connectorx", "adbc"]) -> Literal["auto", "connectorx", "adbc"]:
        env_value = _env_override("default_backend")
        if env_value:
            env_value = env_value.lower()
            if env_value in {"auto", "connectorx", "adbc"}:
                return env_value  # type: ignore[return-value]
        value = yaml_config.get("default_backend", default)
        if isinstance(value, str) and value.lower() in {"auto", "connectorx", "adbc"}:
            return value.lower()  # type: ignore[return-value]
        return default

    def resolve_bool(key: str, default: bool) -> bool:
        env_value = _env_override(key)
        if env_value is not None:
            if env_value.lower() in {"1", "true", "yes"}:
                return True
            if env_value.lower() in {"0", "false", "no"}:
                return False
        value = yaml_config.get(key, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lower = value.lower()
            if lower in {"1", "true", "yes"}:
                return True
            if lower in {"0", "false", "no"}:
                return False
        return default

    fetch_chunk_rows = max(1, resolve_int("fetch_chunk_rows", DEFAULT_FETCH_CHUNK_ROWS))
    retries = max(0, resolve_int("retries", DEFAULT_RETRIES))
    retry_backoff = max(0.0, resolve_float("retry_backoff_sec", DEFAULT_RETRY_BACKOFF))
    preserve_case = resolve_bool("preserve_case", False)

    cpu_count = os.cpu_count() or 1
    max_parallelism = resolve_int("max_parallelism", min(cpu_count, DEFAULT_PARALLELISM_CAP))
    max_parallelism = max(1, min(max_parallelism, DEFAULT_PARALLELISM_CAP))

    default_backend = resolve_backend(DEFAULT_BACKEND)

    return DBConfig(
        default_backend=default_backend,
        fetch_chunk_rows=fetch_chunk_rows,
        retries=retries,
        retry_backoff_sec=retry_backoff,
        max_parallelism=max_parallelism,
        preserve_case=preserve_case,
    )
