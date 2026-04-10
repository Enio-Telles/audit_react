from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter

from interface_grafica.config import CNPJ_ROOT

from .fiscal_summary import (
    build_dataset_listing,
    build_domain_summary,
    probe_parquet,
    stage_label,
)

router = APIRouter()


def _sanitize(cnpj: str | None) -> str | None:
    if cnpj is None:
        return None
    cleaned = re.sub(r"\D", "", cnpj)
    return cleaned or None


def _pasta_produtos(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "analises" / "produtos"


def _path_bloco_h(cnpj: str) -> Path:
    base_cnpj = CNPJ_ROOT / cnpj
    candidatos = [
        _pasta_produtos(cnpj) / f"bloco_h_{cnpj}.parquet",
        base_cnpj / "arquivos_parquet" / f"bloco_h_{cnpj}.parquet",
        base_cnpj / "arquivos_parquet" / "fiscal" / "efd" / f"bloco_h_{cnpj}.parquet",
    ]
    for path in candidatos:
        if path.exists():
            return path
    return candidatos[0]


def _analysis_paths(cnpj: str) -> dict[str, Path]:
    pasta_produtos = _pasta_produtos(cnpj)
    return {
        "mov_estoque": pasta_produtos / f"mov_estoque_{cnpj}.parquet",
        "estoque_mensal": pasta_produtos / f"aba_mensal_{cnpj}.parquet",
        "estoque_anual": pasta_produtos / f"aba_anual_{cnpj}.parquet",
        "bloco_h": _path_bloco_h(cnpj),
        "fatores_conversao": pasta_produtos / f"fatores_conversao_{cnpj}.parquet",
        "produtos_agrupados": pasta_produtos / f"produtos_agrupados_{cnpj}.parquet",
        "produtos_final": pasta_produtos / f"produtos_final_{cnpj}.parquet",
    }


def _analysis_probes(cnpj: str | None) -> dict[str, dict[str, Any]]:
    if not cnpj:
        return {}

    return {key: probe_parquet(path) for key, path in _analysis_paths(cnpj).items()}


def _describe_count(probe: dict[str, Any], singular: str, plural: str) -> str:
    if probe.get("status") == "materializado":
        rows = int(probe.get("rows", 0))
        unidade = singular if rows == 1 else plural
        return f"{rows} {unidade}"
    if probe.get("status") == "erro":
        return "erro de leitura"
    return "não materializado"


def _safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, list):
        return [_safe_value(item) for item in v]
    return v


def _empty_page(page: int, page_size: int) -> dict[str, Any]:
    return {
        "total_rows": 0,
        "page": page,
        "page_size": page_size,
        "total_pages": 1,
        "columns": [],
        "all_columns": [],
        "rows": [],
    }


def _page_from_parquet(
    path: Path,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> dict[str, Any]:
    if not path.exists():
        return _empty_page(page, page_size)

    df = pl.read_parquet(path)
    if sort_by and sort_by in df.columns:
        try:
            df = df.sort(sort_by, descending=sort_desc, nulls_last=True)
        except Exception:
            pass

    total = df.height
    start = max(0, (page - 1) * page_size)
    df_page = df.slice(start, page_size)
    rows = [
        {column: _safe_value(row[column]) for column in df_page.columns}
        for row in df_page.to_dicts()
    ]
    return {
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, math.ceil(total / page_size)),
        "columns": df_page.columns,
        "all_columns": df.columns,
        "rows": rows,
    }


def _payload(cnpj: str | None) -> dict[str, object]:
    probes = _analysis_probes(cnpj)
    mov = probes.get("mov_estoque", {"status": "ausente", "rows": 0})
    mensal = probes.get("estoque_mensal", {"status": "ausente", "rows": 0})
    anual = probes.get("estoque_anual", {"status": "ausente", "rows": 0})
    bloco_h = probes.get("bloco_h", {"status": "ausente", "rows": 0})
    fatores = probes.get("fatores_conversao", {"status": "ausente", "rows": 0})
    agrupados = probes.get("produtos_agrupados", {"status": "ausente", "rows": 0})
    produtos_final = probes.get("produtos_final", {"status": "ausente", "rows": 0})

    cards = [
        {
            "id": "cruzamentos",
            "title": "Cruzamentos",
            "value": _describe_count(mov, "linha legada", "linhas legadas"),
            "description": "Ponte inicial para Estoque. Considera o parquet de movimentação já usado pela aba legada.",
        },
        {
            "id": "verificacoes",
            "title": "Verificações",
            "value": _describe_count(fatores, "fator", "fatores"),
            "description": "Ponte inicial para Conversão. Lê o parquet de fatores de conversão já mantido pela camada atual.",
        },
        {
            "id": "classificacao",
            "title": "Classificação dos produtos",
            "value": _describe_count(produtos_final, "produto-base", "produtos-base"),
            "description": "Base inicial para classificação. Aproveita produtos_final enquanto o catálogo mestre não nasce como dataset próprio.",
        },
    ]
    datasets = [
        {
            "id": "cross_estoque_legado_mov",
            "label": "Movimentação de estoque",
            "stage": stage_label(mov),
            "description": "Parquet legado usado como primeira ponte para cruzamentos analíticos.",
        },
        {
            "id": "cross_estoque_legado_mensal",
            "label": "Tabela mensal",
            "stage": stage_label(mensal),
            "description": "Série mensal legada que será absorvida pela camada de cruzamentos.",
        },
        {
            "id": "cross_estoque_legado_anual",
            "label": "Tabela anual",
            "stage": stage_label(anual),
            "description": "Série anual legada que será absorvida pela camada de cruzamentos.",
        },
        {
            "id": "cross_bloco_h_legado",
            "label": "Bloco H",
            "stage": stage_label(bloco_h),
            "description": "Inventário legado usado como apoio para visão de estoque e auditoria.",
        },
        {
            "id": "verificacoes_conversao_legado",
            "label": "Fatores de conversão",
            "stage": stage_label(fatores),
            "description": "Base legada da aba Conversão, agora tratada como verificação estrutural.",
        },
        {
            "id": "verificacoes_agregacao_legado",
            "label": "Produtos agrupados",
            "stage": stage_label(agrupados),
            "description": "Base legada da aba Agregação, agora tratada como verificação da identidade do produto.",
        },
        {
            "id": "produtos_base_legado",
            "label": "Produtos final",
            "stage": stage_label(produtos_final),
            "description": "Base legada de produtos que servirá de ponte para a futura classificação dos produtos.",
        },
    ]
    next_steps = [
        "substituir a ponte legada por datasets canônicos de cruzamentos",
        "absorver agregação e conversão em verificações materializadas",
        "abrir o primeiro catálogo mestre de produtos como contrato próprio",
    ]
    legacy_shortcuts = [
        {
            "id": "estoque",
            "label": "Estoque (legado)",
            "description": f"Movimentação: {_describe_count(mov, 'linha', 'linhas')}; mensal: {_describe_count(mensal, 'linha', 'linhas')}; anual: {_describe_count(anual, 'linha', 'linhas')}",
        },
        {
            "id": "agregacao",
            "label": "Agregação (legado)",
            "description": f"Tabela agrupada: {_describe_count(agrupados, 'linha', 'linhas')}",
        },
        {
            "id": "conversao",
            "label": "Conversão (legado)",
            "description": f"Fatores de conversão: {_describe_count(fatores, 'linha', 'linhas')}",
        },
    ]
    summary = build_domain_summary(
        domain="analise",
        title="Análise Fiscal",
        subtitle="Cruzamentos, verificações e classificação dos produtos.",
        cnpj=cnpj,
        cards=cards,
        datasets=datasets,
        next_steps=next_steps,
        legacy_shortcuts=legacy_shortcuts,
    )
    if cnpj:
        summary["status"] = "ponte_legada_ativa"
    return summary


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "domain": "analise"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(_sanitize(cnpj))


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    cnpj_sanitized = _sanitize(cnpj)
    payload = _payload(cnpj_sanitized)
    return build_dataset_listing("analise", cnpj_sanitized, payload["datasets"])


@router.get("/estoque-mov")
def estoque_mov_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> dict[str, Any]:
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return _empty_page(page, page_size)
    return _page_from_parquet(
        _analysis_paths(cnpj_sanitized)["mov_estoque"],
        page,
        page_size,
        sort_by,
        sort_desc,
    )


@router.get("/estoque-mensal")
def estoque_mensal_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> dict[str, Any]:
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return _empty_page(page, page_size)
    return _page_from_parquet(
        _analysis_paths(cnpj_sanitized)["estoque_mensal"],
        page,
        page_size,
        sort_by,
        sort_desc,
    )


@router.get("/estoque-anual")
def estoque_anual_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> dict[str, Any]:
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return _empty_page(page, page_size)
    return _page_from_parquet(
        _analysis_paths(cnpj_sanitized)["estoque_anual"],
        page,
        page_size,
        sort_by,
        sort_desc,
    )


@router.get("/agregacao")
def agregacao_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> dict[str, Any]:
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return _empty_page(page, page_size)
    return _page_from_parquet(
        _analysis_paths(cnpj_sanitized)["produtos_agrupados"],
        page,
        page_size,
        sort_by,
        sort_desc,
    )


@router.get("/conversao")
def conversao_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> dict[str, Any]:
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return _empty_page(page, page_size)
    return _page_from_parquet(
        _analysis_paths(cnpj_sanitized)["fatores_conversao"],
        page,
        page_size,
        sort_by,
        sort_desc,
    )


@router.get("/produtos-base")
def produtos_base_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
) -> dict[str, Any]:
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return _empty_page(page, page_size)
    return _page_from_parquet(
        _analysis_paths(cnpj_sanitized)["produtos_final"],
        page,
        page_size,
        sort_by,
        sort_desc,
    )
