from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter

from utilitarios.project_paths import CNPJ_ROOT

router = APIRouter()


def _sanitize(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def _pasta_ressarcimento(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "analises" / "ressarcimento_st"


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


def _df_to_response(df: pl.DataFrame, page: int = 1, page_size: int = 500) -> dict:
    total = df.height
    start = (page - 1) * page_size
    df_page = df.slice(start, page_size)
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
    search: str | None = None,
    column_filters: str | None = None,
) -> pl.DataFrame:
    if search and search.strip():
        termo = search.strip().lower()
        exprs = [
            pl.col(coluna).cast(pl.Utf8).str.to_lowercase().str.contains(termo, literal=True)
            for coluna in df.columns
        ]
        if exprs:
            df = df.filter(pl.any_horizontal(exprs))

    for coluna, valor in _carregar_filtros_colunas(column_filters).items():
        if coluna not in df.columns:
            continue
        termo = valor.strip().lower()
        df = df.filter(
            pl.col(coluna).cast(pl.Utf8).str.to_lowercase().str.contains(termo, literal=True)
        )

    return df


def _resposta_vazia(page: int = 1, page_size: int = 500) -> dict:
    return {
        "total_rows": 0,
        "page": page,
        "page_size": page_size,
        "total_pages": 1,
        "columns": [],
        "rows": [],
    }


def _ler_tabela_ressarcimento_ou_vazia(
    path: Path,
    page: int = 1,
    page_size: int = 500,
    sort_by: str | None = None,
    sort_desc: bool = False,
    search: str | None = None,
    column_filters: str | None = None,
) -> dict:
    if not path.exists():
        return _resposta_vazia(page, page_size)
    df = pl.read_parquet(path)
    df = _aplicar_filtros_texto(df, search, column_filters)
    df = _ordenar_df(df, sort_by, sort_desc)
    return _df_to_response(df, page, page_size)


@router.get("/{cnpj}/itens")
def get_itens(
    cnpj: str,
    page: int = 1,
    page_size: int = 500,
    sort_by: str | None = None,
    sort_desc: bool = False,
    search: str | None = None,
    column_filters: str | None = None,
):
    cnpj = _sanitize(cnpj)
    path = _pasta_ressarcimento(cnpj) / f"ressarcimento_st_item_{cnpj}.parquet"
    return _ler_tabela_ressarcimento_ou_vazia(
        path,
        page,
        page_size,
        sort_by,
        sort_desc,
        search,
        column_filters,
    )


@router.get("/{cnpj}/mensal")
def get_mensal(
    cnpj: str,
    page: int = 1,
    page_size: int = 500,
    sort_by: str | None = None,
    sort_desc: bool = False,
    search: str | None = None,
    column_filters: str | None = None,
):
    cnpj = _sanitize(cnpj)
    path = _pasta_ressarcimento(cnpj) / f"ressarcimento_st_mensal_{cnpj}.parquet"
    return _ler_tabela_ressarcimento_ou_vazia(
        path,
        page,
        page_size,
        sort_by,
        sort_desc,
        search,
        column_filters,
    )


@router.get("/{cnpj}/conciliacao")
def get_conciliacao(
    cnpj: str,
    page: int = 1,
    page_size: int = 500,
    sort_by: str | None = None,
    sort_desc: bool = False,
    search: str | None = None,
    column_filters: str | None = None,
):
    cnpj = _sanitize(cnpj)
    path = _pasta_ressarcimento(cnpj) / f"ressarcimento_st_conciliacao_{cnpj}.parquet"
    return _ler_tabela_ressarcimento_ou_vazia(
        path,
        page,
        page_size,
        sort_by,
        sort_desc,
        search,
        column_filters,
    )


@router.get("/{cnpj}/validacoes")
def get_validacoes(
    cnpj: str,
    page: int = 1,
    page_size: int = 500,
    sort_by: str | None = None,
    sort_desc: bool = False,
    search: str | None = None,
    column_filters: str | None = None,
):
    cnpj = _sanitize(cnpj)
    path = _pasta_ressarcimento(cnpj) / f"ressarcimento_st_validacoes_{cnpj}.parquet"
    return _ler_tabela_ressarcimento_ou_vazia(
        path,
        page,
        page_size,
        sort_by,
        sort_desc,
        search,
        column_filters,
    )


@router.get("/{cnpj}/resumo")
def get_resumo(cnpj: str):
    cnpj = _sanitize(cnpj)
    pasta_produtos = _pasta_produtos(cnpj)
    pasta_ressarcimento = _pasta_ressarcimento(cnpj)

    prerequisitos = {
        "efd_atomizacao": (CNPJ_ROOT / cnpj / "analises" / "atomizadas" / f"c176_tipado_{cnpj}.parquet").exists(),
        "c176_xml": (pasta_produtos / f"c176_xml_{cnpj}.parquet").exists(),
        "fatores_conversao": (pasta_produtos / f"fatores_conversao_{cnpj}.parquet").exists(),
    }
    faltantes = [nome for nome, ok in prerequisitos.items() if not ok]

    path_item = pasta_ressarcimento / f"ressarcimento_st_item_{cnpj}.parquet"
    if not path_item.exists():
        return {
            "ready": False,
            "prerequisitos": prerequisitos,
            "faltantes": faltantes,
            "qtd_itens": 0,
            "pendencias_conversao": 0,
            "parciais_pos_2022": 0,
            "itens_com_st_calc": 0,
            "itens_com_fronteira": 0,
            "cobertura_pre_2023": 0.0,
            "cobertura_pos_2023": 0.0,
        }

    df = pl.read_parquet(path_item)
    total = df.height
    # ⚡ Bolt Optimization: Compute multiple counts in a single pass over the dataframe to prevent performance bottlenecks.
    aggs = df.select([
        (pl.col("status_calculo") == "pendente_conversao").sum().alias("pendencias"),
        (pl.col("status_calculo") == "parcial_pos_2022").sum().alias("parciais"),
        pl.col("possui_st_calc_ate_2022").sum().alias("com_calc"),
        pl.col("possui_fronteira").sum().alias("com_fronteira"),

        (pl.col("ano_ref") <= 2022).sum().alias("pre_2023_height"),
        ((pl.col("ano_ref") <= 2022) & pl.col("possui_st_calc_ate_2022")).sum().alias("pre_2023_com_calc"),

        (pl.col("ano_ref") > 2022).sum().alias("pos_2023_height"),
        ((pl.col("ano_ref") > 2022) & pl.col("possui_fronteira")).sum().alias("pos_2023_com_fronteira"),
    ]).row(0)

    pendencias = aggs[0]
    parciais = aggs[1]
    com_calc = aggs[2]
    com_fronteira = aggs[3]
    pre_2023_height = aggs[4]
    pre_2023_com_calc = aggs[5]
    pos_2023_height = aggs[6]
    pos_2023_com_fronteira = aggs[7]

    cobertura_pre = round((pre_2023_com_calc * 100.0) / pre_2023_height, 2) if pre_2023_height > 0 else 0.0
    cobertura_pos = round((pos_2023_com_fronteira * 100.0) / pos_2023_height, 2) if pos_2023_height > 0 else 0.0

    return {
        "ready": total > 0 and not faltantes,
        "prerequisitos": prerequisitos,
        "faltantes": faltantes,
        "qtd_itens": total,
        "pendencias_conversao": pendencias,
        "parciais_pos_2022": parciais,
        "itens_com_st_calc": com_calc,
        "itens_com_fronteira": com_fronteira,
        "cobertura_pre_2023": cobertura_pre,
        "cobertura_pos_2023": cobertura_pos,
    }
