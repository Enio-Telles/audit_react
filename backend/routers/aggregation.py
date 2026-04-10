from __future__ import annotations

import json
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
        "page_size": page_size,
        "total_pages": max(1, math.ceil(total / page_size)),
        "columns": df_page.columns,
        "rows": rows,
    }


def _ordenar_df(
    df: pl.DataFrame,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> pl.DataFrame:
    if not sort_by or sort_by not in df.columns:
        return df
    try:
        return df.sort(sort_by, descending=sort_desc, nulls_last=True)
    except Exception:
        return df


def _carregar_filtros_colunas(column_filters: str | None = None) -> dict[str, str]:
    if not column_filters:
        return {}
    try:
        payload = json.loads(column_filters)
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict):
        return {}
    return {
        str(chave): str(valor)
        for chave, valor in payload.items()
        if valor is not None and str(valor).strip()
    }


def _aplicar_filtros_texto(
    df: pl.DataFrame,
    search_desc: str | None = None,
    search_ncm: str | None = None,
    search_cest: str | None = None,
    column_filters: str | None = None,
) -> pl.DataFrame:
    filtros_fixos = {
        "descr_padrao": search_desc,
        "ncm_padrao": search_ncm,
        "cest_padrao": search_cest,
    }
    for coluna, valor in filtros_fixos.items():
        if not valor or coluna not in df.columns:
            continue
        termo = valor.strip().lower()
        df = df.filter(
            pl.col(coluna).cast(pl.Utf8).str.to_lowercase().str.contains(termo, literal=True)
        )

    for coluna, valor in _carregar_filtros_colunas(column_filters).items():
        if coluna not in df.columns:
            continue
        termo = valor.strip().lower()
        df = df.filter(
            pl.col(coluna).cast(pl.Utf8).str.to_lowercase().str.contains(termo, literal=True)
        )

    return df


def _enriquecer_lista_descr_compl(df: pl.DataFrame, cnpj: str) -> pl.DataFrame:
    """Junta lista_descr_compl (do C170) agrupada por id_agrupado."""
    arq_c170 = CNPJ_ROOT / cnpj / "arquivos_parquet" / f"c170_xml_{cnpj}.parquet"
    if not arq_c170.exists() or "id_agrupado" not in df.columns:
        return df
    try:
        df_c170 = pl.scan_parquet(arq_c170).select(["id_agrupado", "Descr_compl"]).collect()
        df_agg = (
            df_c170
            .filter(
                pl.col("Descr_compl").is_not_null()
                & (pl.col("Descr_compl").str.strip_chars() != "")
            )
            .group_by("id_agrupado")
            .agg(pl.col("Descr_compl").unique().sort().alias("lista_descr_compl"))
        )
        df = df.join(df_agg, on="id_agrupado", how="left")
        df = df.with_columns(
            pl.col("lista_descr_compl").fill_null([]).cast(pl.List(pl.String))
        )
    except Exception:
        pass
    return df


@router.get("/{cnpj}/tabela_agrupada")
def get_tabela_agrupada(
    cnpj: str,
    page: int = 1,
    page_size: int = 300,
    sort_by: str | None = None,
    sort_desc: bool = False,
    search_desc: str | None = None,
    search_ncm: str | None = None,
    search_cest: str | None = None,
    column_filters: str | None = None,
):
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
    df = _enriquecer_lista_descr_compl(df, cnpj)
    df = _aplicar_filtros_texto(df, search_desc, search_ncm, search_cest, column_filters)
    df = _ordenar_df(df, sort_by, sort_desc)
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
        # O primeiro elemento da lista é o id canônico (destino); os demais são as origens.
        ids_ordenados = [req.id_agrupado_destino] + [
            i for i in req.ids_origem if i != req.id_agrupado_destino
        ]
        resultado = svc.agregar_linhas(cnpj=cnpj, ids_agrupados_selecionados=ids_ordenados)
        return {"ok": True, "resultado": resultado}
    except ValueError as exc:
        raise HTTPException(400, str(exc))
    except Exception as exc:
        raise HTTPException(500, str(exc))
