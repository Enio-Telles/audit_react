from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter

from interface_grafica.config import CNPJ_ROOT
from .fiscal_dataset_locator import locate_dataset
from .fiscal_storage import resolve_materialized_path

router = APIRouter()

def _sanitize(cnpj: str | None) -> str | None:
    if cnpj is None:
        return None
    cleaned = re.sub(r"\D", "", cnpj)
    return cleaned or None

def _base_cnpj(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj

def _empty_page(page: int, page_size: int) -> dict[str, Any]:
    return {
        "total_rows": 0,
        "page": page,
        "page_size": page_size,
        "total_pages": 1,
        "columns": [],
        "rows": [],
    }

def _page_from_parquet(
    path: Path,
    page: int = 1,
    page_size: int = 100,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
    dataset_id: str | None = None,
    camada: str | None = "analitica",
) -> dict[str, Any]:
    resolved = resolve_materialized_path(path)
    if not resolved.exists():
        return {
            **_empty_page(page, page_size),
            "_provenance": {
                "dataset_id": dataset_id or "desconhecido",
                "status": "faltante",
                "path": str(path)
            }
        }

    try:
        df = pl.read_parquet(resolved)
        
        # Filtros básicos
        if filter_text:
            search = filter_text.lower()
            masks = [pl.col(c).cast(pl.Utf8).str.to_lowercase().str.contains(search) for c in df.columns]
            df = df.filter(pl.any_horizontal(masks))
            
        if filter_column and filter_value and filter_column in df.columns:
            df = df.filter(pl.col(filter_column).cast(pl.Utf8).str.contains(filter_value))

        if sort_by and sort_by in df.columns:
            df = df.sort(sort_by, descending=sort_desc)

        total = df.height
        start = (page - 1) * page_size
        df_page = df.slice(start, page_size)

        def _safe(v):
            if v is None: return None
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): return None
            return v

        rows = [
            {col: _safe(row[col]) for col in df_page.columns}
            for row in df_page.to_dicts()
        ]

        return {
            "total_rows": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, math.ceil(total / page_size)),
            "columns": df_page.columns,
            "rows": rows,
            "_provenance": {
                "dataset_id": dataset_id or "desconhecido",
                "camada": camada,
                "source_path": str(resolved),
            },
        }
    except Exception as e:
        return {
            **_empty_page(page, page_size),
            "error": str(e),
            "_provenance": {"dataset_id": dataset_id, "status": "erro_leitura"}
        }

# Locators
def _find_mensal(cnpj: str) -> Path:
    return locate_dataset(
        cnpj, "ressarcimento_mensal",
        _base_cnpj(cnpj) / "analises" / "ressarcimento_st" / f"ressarcimento_st_mensal_{cnpj}.parquet",
    )

def _find_itens(cnpj: str) -> Path:
    return locate_dataset(
        cnpj, "ressarcimento_itens",
        _base_cnpj(cnpj) / "analises" / "ressarcimento_st" / f"ressarcimento_st_item_{cnpj}.parquet",
    )

def _find_conciliacao(cnpj: str) -> Path:
    return locate_dataset(
        cnpj, "ressarcimento_conciliacao",
        _base_cnpj(cnpj) / "analises" / "ressarcimento_st" / f"ressarcimento_st_conciliacao_{cnpj}.parquet",
    )

@router.get("/mensal")
def ressarcimento_mensal(
    cnpj: str, page: int = 1, page_size: int = 100,
    sort_by: str | None = None, sort_desc: bool = False,
    filter_text: str | None = None
):
    c = _sanitize(cnpj)
    if not c: return _empty_page(page, page_size)
    return _page_from_parquet(_find_mensal(c), page, page_size, sort_by, sort_desc, filter_text, dataset_id="ressarcimento_mensal")

@router.get("/itens")
def ressarcimento_itens(
    cnpj: str, page: int = 1, page_size: int = 100,
    sort_by: str | None = None, sort_desc: bool = False,
    filter_text: str | None = None
):
    c = _sanitize(cnpj)
    if not c: return _empty_page(page, page_size)
    return _page_from_parquet(_find_itens(c), page, page_size, sort_by, sort_desc, filter_text, dataset_id="ressarcimento_itens")

@router.get("/conciliacao")
def ressarcimento_conciliacao(
    cnpj: str, page: int = 1, page_size: int = 100,
    sort_by: str | None = None, sort_desc: bool = False,
    filter_text: str | None = None
):
    c = _sanitize(cnpj)
    if not c: return _empty_page(page, page_size)
    return _page_from_parquet(_find_conciliacao(c), page, page_size, sort_by, sort_desc, filter_text, dataset_id="ressarcimento_conciliacao")
