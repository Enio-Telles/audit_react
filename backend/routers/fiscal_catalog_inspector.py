from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

from utilitarios.dataset_registry import (
    catalogo_resumido,
    diagnosticar_disponibilidade,
    encontrar_dataset,
    listar_aliases_dataset,
)

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
        "probe": probe,
        "columns": columns,
        "preview": preview,
    }
