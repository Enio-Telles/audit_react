from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from utilitarios.delta_lake import read_delta_table, scan_delta_table


def resolve_materialized_path(path: Path) -> Path:
    if path.exists():
        return path
    if path.suffix.lower() == ".parquet":
        delta_candidate = path.with_suffix("")
        if delta_candidate.exists():
            return delta_candidate
    return path


def is_delta_materialized(path: Path) -> bool:
    resolved = resolve_materialized_path(path)
    return resolved.exists() and resolved.is_dir()


def read_materialized_frame(path: Path) -> pl.DataFrame:
    resolved = resolve_materialized_path(path)
    if not resolved.exists():
        raise FileNotFoundError(str(resolved))
    if resolved.is_dir():
        return read_delta_table(resolved)
    return pl.read_parquet(resolved)


def scan_materialized_frame(path: Path) -> pl.LazyFrame:
    resolved = resolve_materialized_path(path)
    if not resolved.exists():
        raise FileNotFoundError(str(resolved))
    if resolved.is_dir():
        return scan_delta_table(resolved)
    return pl.scan_parquet(resolved)


def probe_materialized(path: Path) -> dict[str, Any]:
    resolved = resolve_materialized_path(path)
    if not resolved.exists():
        return {
            "exists": False,
            "rows": 0,
            "path": str(resolved),
            "status": "ausente",
            "format": "delta" if resolved.suffix == "" else "parquet",
        }

    try:
        rows = int(
            scan_materialized_frame(resolved)
            .select(pl.len().alias("rows"))
            .collect()
            .item()
        )
        return {
            "exists": True,
            "rows": rows,
            "path": str(resolved),
            "status": "materializado",
            "format": "delta" if resolved.is_dir() else "parquet",
        }
    except Exception as exc:
        return {
            "exists": True,
            "rows": 0,
            "path": str(resolved),
            "status": "erro",
            "error": str(exc),
            "format": "delta" if resolved.is_dir() else "parquet",
        }
