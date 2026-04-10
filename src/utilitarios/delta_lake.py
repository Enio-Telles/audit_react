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


def _normalize_table_name(table_name: str) -> str:
    return table_name.strip().lower().removesuffix(".parquet")


def get_delta_enabled_tables() -> set[str]:
    raw = os.getenv("DELTA_ENABLED_TABLES", "")
    return {
        _normalize_table_name(item)
        for item in raw.split(",")
        if item.strip()
    }


def is_delta_enabled() -> bool:
    return os.getenv("DATA_LAKE_FORMAT", "parquet").lower() == "delta"


def is_delta_selected_for_table(table_name: str) -> bool:
    if not is_delta_enabled():
        return False
    enabled_tables = get_delta_enabled_tables()
    if not enabled_tables:
        return True
    return _normalize_table_name(table_name) in enabled_tables


def resolve_delta_target(path: Path) -> Path:
    return path.with_suffix("") if path.suffix.lower() == ".parquet" else path


def resolve_storage_format(target_path: Path, explicit_format: str | None = None) -> str:
    if explicit_format and explicit_format.lower() in {"parquet", "delta"}:
        return explicit_format.lower()
    table_name = target_path.stem if target_path.suffix else target_path.name
    return "delta" if is_delta_selected_for_table(table_name) else "parquet"


def get_delta_runtime_config() -> dict[str, object]:
    return {
        "global_format": os.getenv("DATA_LAKE_FORMAT", "parquet").lower(),
        "delta_enabled": is_delta_enabled(),
        "delta_write_mode": os.getenv("DELTA_WRITE_MODE", "overwrite"),
        "delta_enabled_tables": sorted(get_delta_enabled_tables()),
    }


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
