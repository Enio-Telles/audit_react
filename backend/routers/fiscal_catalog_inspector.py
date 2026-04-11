from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl

from utilitarios.dataset_registry import (
    catalogo_resumido,
    diagnosticar_disponibilidade,
    encontrar_dataset,
    listar_aliases_dataset,
)
from utilitarios.schema_registry import SchemaRegistry

from .fiscal_storage import read_materialized_frame, resolve_materialized_path
from .fiscal_summary import probe_parquet


def sanitize_cnpj(cnpj: str | None) -> str | None:
    if cnpj is None:
        return None
    cleaned = re.sub(r"\D", "", cnpj)
    return cleaned or None


def _safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, list):
        return [_safe_value(item) for item in v]
    return v


def catalog_status() -> dict[str, Any]:
    return catalogo_resumido()


def availability_for_cnpj(cnpj: str) -> dict[str, Any]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    if not cnpj_sanitized:
        return {"cnpj": None, "items": []}
    return {
        "cnpj": cnpj_sanitized,
        "items": diagnosticar_disponibilidade(cnpj_sanitized),
    }


def inspect_dataset(cnpj: str, dataset_id: str, limit: int = 20) -> dict[str, Any]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    if not cnpj_sanitized:
        return {
            "cnpj": None,
            "dataset_id": dataset_id,
            "aliases": listar_aliases_dataset(dataset_id),
            "probe": {"status": "ausente"},
            "preview": [],
        }

    localizado = encontrar_dataset(cnpj_sanitized, dataset_id)
    aliases = listar_aliases_dataset(dataset_id)
    if localizado is None:
        return {
            "cnpj": cnpj_sanitized,
            "dataset_id": dataset_id,
            "aliases": aliases,
            "probe": {"status": "ausente"},
            "preview": [],
            "metadata": None,
        }

    resolved = resolve_materialized_path(localizado.caminho)
    probe = probe_parquet(resolved)
    preview: list[dict[str, Any]] = []
    columns: list[str] = []
    try:
        df = read_materialized_frame(resolved)
        df_preview = df.head(max(1, min(limit, 100)))
        columns = list(df.columns)
        preview = [
            {column: _safe_value(row[column]) for column in df_preview.columns}
            for row in df_preview.to_dicts()
        ]
    except Exception:
        preview = []
        columns = []

    return {
        "cnpj": cnpj_sanitized,
        "dataset_id": localizado.dataset_id,
        "aliases": aliases,
        "caminho": str(localizado.caminho),
        "reutilizado": localizado.reutilizado,
        "metadata": localizado.metadata,
        "schema_registry": _inspect_schema_registry(resolved, localizado.dataset_id),
        "probe": probe,
        "columns": columns,
        "preview": preview,
    }


def _inspect_schema_registry(resolved: Path, dataset_id: str) -> dict[str, Any] | None:
    registry = SchemaRegistry()
    table_name = resolved.stem if resolved.suffix else resolved.name
    snapshot = registry.find_latest_by_source_path(str(resolved)) or registry.latest_snapshot(table_name)
    try:
        if resolved.suffix.lower() == ".parquet":
            current_schema = dict(pl.read_parquet_schema(resolved))
        else:
            current_schema = dict(read_materialized_frame(resolved).schema)
    except Exception:
        current_schema = None

    if snapshot is None and current_schema is None:
        return None

    diff = registry.diff_latest(table_name, current_schema) if current_schema is not None else None
    latest_fields = snapshot.fields if snapshot is not None else None
    current_fields = {str(key): str(value) for key, value in (current_schema or {}).items()} if current_schema else None

    return {
        "table_name": table_name,
        "dataset_id": dataset_id,
        "latest_version": snapshot.version if snapshot else None,
        "schema_hash": snapshot.schema_hash if snapshot else None,
        "recorded_at": snapshot.recorded_at if snapshot else None,
        "source_path": snapshot.source_path if snapshot else str(resolved),
        "field_count": len(latest_fields or current_fields or {}),
        "latest_fields": latest_fields,
        "current_fields": current_fields,
        "diff_current_vs_latest": diff,
    }
