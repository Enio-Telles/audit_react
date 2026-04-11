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
    produtos_base = probes.get("produtos_final", {"status": "ausente", "rows": 0})

    cards = [
        {
            "id": "grupos_produto",
            "title": "Grupos de produto",
            "value": describe_count(agrupados, "grupo", "grupos"),
            "description": "Base operacional atual de agregacao para identidade de produto, conflitos e sugestoes de merge.",
        },
        {
            "id": "catalogo_base",
            "title": "Catalogo base",
            "value": describe_count(produtos_base, "produto", "produtos"),
            "description": "Catálogo consolidado de apoio para comparar descricao, GTIN, NCM, CEST e unidade praticada.",
        },
    ]
    datasets = [
        {
            "id": "agregacao_grupos_legado",
            "label": "Grupos de agregacao",
            "stage": stage_label(agrupados),
            "description": "Tabela agrupada reaproveitada como primeira versao do workbench canonico de agregacao.",
        },
        {
            "id": "agregacao_produtos_base_legado",
            "label": "Produtos base",
            "stage": stage_label(produtos_base),
            "description": "Base de produtos usada como evidencia estrutural para agregacao, conflitos e revisao manual.",
        },
    ]
    next_steps = [
        "separar candidatos, conflitos e merges confirmados em datasets proprios",
        "adicionar score de confianca e justificativas por evidencia de produto",
        "materializar uma trilha clara entre agregacao automatica e edicao manual",
    ]
    legacy_shortcuts = [
        {
            "id": "agregacao",
            "label": "Agregacao (legado)",
            "description": f"Tabela agrupada atual: {describe_count(agrupados, 'linha', 'linhas')}",
        }
    ]
    summary = build_domain_summary(
        domain="agregacao",
        title="Agregacao",
        subtitle="Identidade de produto, conflitos, evidencias e merges operacionais.",
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
    return {"status": "ok", "domain": "agregacao"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(sanitize_cnpj(cnpj))


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    payload = _payload(cnpj_sanitized)
    return build_dataset_listing("agregacao", cnpj_sanitized, payload["datasets"])


@router.get("/grupos")
def grupos_rows(
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
        dataset_id="agregacao_grupos_legado",
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
        dataset_id="agregacao_produtos_base_legado",
    )
