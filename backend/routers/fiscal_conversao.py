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
    fatores = probes.get("fatores_conversao", {"status": "ausente", "rows": 0})

    cards = [
        {
            "id": "fatores_atuais",
            "title": "Fatores de conversão",
            "value": describe_count(fatores, "fator", "fatores"),
            "description": "Base operacional atual que será desdobrada em fator declarado, inferido, auxiliar e manual.",
        },
    ]
    datasets = [
        {
            "id": "conversao_fatores_legado",
            "label": "Fatores de conversão",
            "stage": stage_label(fatores),
            "description": "Dataset legado reaproveitado como ponte para o módulo canônico de conversão.",
        },
    ]
    next_steps = [
        "separar fator EFD, inferido, auxiliar e manual",
        "adicionar histórico de edição e impacto do fator final",
        "materializar datasets próprios para candidatos e evidências de unidade",
    ]
    legacy_shortcuts = [
        {
            "id": "conversao",
            "label": "Conversão (legado)",
            "description": f"Base atual de fatores: {describe_count(fatores, 'linha', 'linhas')}",
        }
    ]
    summary = build_domain_summary(
        domain="conversao",
        title="Conversão",
        subtitle="Fatores de conversão, origem do fator final e evidências de unidade praticada.",
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
    return {"status": "ok", "domain": "conversao"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(sanitize_cnpj(cnpj))


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    payload = _payload(cnpj_sanitized)
    return build_dataset_listing("conversao", cnpj_sanitized, payload["datasets"])


@router.get("/fatores")
def fatores_rows(
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
        analysis_paths(cnpj_sanitized)["fatores_conversao"],
        page,
        page_size,
        sort_by,
        sort_desc,
        filter_text,
        filter_column,
        filter_value,
    )
