from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import polars as pl

from utilitarios.schema_registry import SchemaRegistry

try:
    from deltalake.writer import write_deltalake
except Exception:  # pragma: no cover - opcional
    write_deltalake = None


def is_delta_enabled() -> bool:
    return os.getenv("DATA_LAKE_FORMAT", "parquet").lower() == "delta"


def resolve_delta_target(path: Path) -> Path:
    return path.with_suffix("") if path.suffix.lower() == ".parquet" else path


def write_delta_table(
    df: pl.DataFrame | pl.LazyFrame,
    target_path: Path,
    *,
    mode: str = "overwrite",
    partition_by: Iterable[str] | None = None,
    table_name: str | None = None,
) -> Path:
    if write_deltalake is None:
        raise RuntimeError("deltalake nao instalado. Adicione a dependencia para usar Delta Lake.")

    if isinstance(df, pl.LazyFrame):
        df = df.collect()

    target_path = resolve_delta_target(target_path)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    write_deltalake(
        str(target_path),
        df.to_arrow(),
        mode=mode,
        partition_by=list(partition_by or []),
    )

    registry = SchemaRegistry()
    registry.record_schema(
        table_name or target_path.name,
        df.schema,
        source_path=str(target_path),
        metadata={"format": "delta", "mode": mode},
    )
    return target_path


def scan_delta_table(target_path: Path) -> pl.LazyFrame:
    target_path = resolve_delta_target(target_path)
    if hasattr(pl, "scan_delta"):
        return pl.scan_delta(str(target_path))
    raise RuntimeError("Versao atual do Polars nao suporta scan_delta().")


def read_delta_table(target_path: Path) -> pl.DataFrame:
    target_path = resolve_delta_target(target_path)
    if hasattr(pl, "read_delta"):
        return pl.read_delta(str(target_path))
    return scan_delta_table(target_path).collect()
