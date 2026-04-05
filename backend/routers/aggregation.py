from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from interface_grafica.config import CNPJ_ROOT

router = APIRouter()


def _sanitize(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def _pasta_produtos(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "analises" / "produtos"


def _safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, list):
        return [_safe_value(x) for x in v]
    return v


def _df_to_response(df: pl.DataFrame, page: int = 1, page_size: int = 300) -> dict:
    total = df.height
    df_page = df.slice((page - 1) * page_size, page_size)
    rows = [
        {col: _safe_value(row[col]) for col in df_page.columns}
        for row in df_page.to_dicts()
    ]
    return {
        "total_rows": total,
        "page": page,
        "total_pages": max(1, math.ceil(total / page_size)),
        "columns": df_page.columns,
        "rows": rows,
    }


@router.get("/{cnpj}/tabela_agrupada")
def get_tabela_agrupada(cnpj: str, page: int = 1, page_size: int = 300):
    cnpj = _sanitize(cnpj)
    pasta = _pasta_produtos(cnpj)
    candidates = [
        pasta / f"produtos_agrupados_{cnpj}.parquet",
        pasta / f"produtos_final_{cnpj}.parquet",
    ]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        raise HTTPException(404, "Tabela agrupada não encontrada")
    df = pl.read_parquet(path)
    return _df_to_response(df, page, page_size)


class AggregateRequest(BaseModel):
    cnpj: str
    id_agrupado_destino: str
    ids_origem: list[str]


@router.post("/merge")
def merge_agrupados(req: AggregateRequest):
    cnpj = _sanitize(req.cnpj)
    try:
        from interface_grafica.services.aggregation_service import ServicoAgregacao
        svc = ServicoAgregacao()
        svc.merge_id_agrupados(
            cnpj=cnpj,
            id_agrupado_destino=req.id_agrupado_destino,
            ids_origem=req.ids_origem,
        )
        return {"ok": True}
    except Exception as exc:
        raise HTTPException(500, str(exc))
