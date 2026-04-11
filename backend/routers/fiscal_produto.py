from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from .fiscal_analysis_support import (
    analysis_paths,
    analysis_probes,
    describe_count,
    empty_page,
    page_from_parquet,
    sanitize_cnpj,
)
from .fiscal_summary import build_dataset_listing, build_domain_summary, stage_label

router = APIRouter()


def _payload(cnpj: str | None) -> dict[str, object]:
    probes = analysis_probes(cnpj)
    agrupados = probes.get("produtos_agrupados", {"status": "ausente", "rows": 0})
    produtos_final = probes.get("produtos_final", {"status": "ausente", "rows": 0})

    cards = [
        {
            "id": "grupos_produto",
            "title": "Grupos de produto",
            "value": describe_count(agrupados, "grupo", "grupos"),
            "description": "Base operacional atual de agrupamento que será evoluída para o workbench canônico de Produto Master.",
        },
        {
            "id": "produtos_base",
            "title": "Produtos base",
            "value": describe_count(produtos_final, "produto", "produtos"),
            "description": "Catálogo atual de produtos usado como ponte para a futura camada de identidade, conflitos e classificação.",
        },
    ]
    datasets = [
        {
            "id": "produto_master_grupos_legado",
            "label": "Produtos agrupados",
            "stage": stage_label(agrupados),
            "description": "Base legada de agrupamento reaproveitada como ponto de partida do módulo Produto Master.",
        },
        {
            "id": "produto_master_base_legado",
            "label": "Produtos final",
            "stage": stage_label(produtos_final),
            "description": "Base atual de produtos que sustenta a migração para um catálogo mestre com evidências e conflitos.",
        },
    ]
    next_steps = [
        "separar grupos, conflitos e evidências de identidade do produto",
        "evoluir o merge manual para um workbench de produto master",
        "materializar datasets próprios para candidatos, conflitos e classificação",
    ]
    legacy_shortcuts = [
        {
            "id": "agregacao",
            "label": "Agregação (legado)",
            "description": f"Tabela agrupada atual: {describe_count(agrupados, 'linha', 'linhas')}",
        }
    ]
    summary = build_domain_summary(
        domain="produto",
        title="Produto Master",
        subtitle="Identidade, agrupamento, conflitos e classificação dos produtos do contribuinte.",
        cnpj=cnpj,
        cards=cards,
        datasets=datasets,
        next_steps=next_steps,
        legacy_shortcuts=legacy_shortcuts,
    )
    if cnpj:
        summary["status"] = "canonicacao_iniciada"
    return summary


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "domain": "produto"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(sanitize_cnpj(cnpj))


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    payload = _payload(cnpj_sanitized)
    return build_dataset_listing("produto", cnpj_sanitized, payload["datasets"])


@router.get("/agrupacoes")
def agrupacoes_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
) -> dict[str, Any]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    if not cnpj_sanitized:
        return empty_page(page, page_size)
    return page_from_parquet(
        analysis_paths(cnpj_sanitized)["produtos_agrupados"],
        page,
        page_size,
        sort_by,
        sort_desc,
        filter_text,
        filter_column,
        filter_value,
    )


@router.get("/produtos-base")
def produtos_base_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
) -> dict[str, Any]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    if not cnpj_sanitized:
        return empty_page(page, page_size)
    return page_from_parquet(
        analysis_paths(cnpj_sanitized)["produtos_final"],
        page,
        page_size,
        sort_by,
        sort_desc,
        filter_text,
        filter_column,
        filter_value,
    )
