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
    mov = probes.get("mov_estoque", {"status": "ausente", "rows": 0})
    mensal = probes.get("estoque_mensal", {"status": "ausente", "rows": 0})
    anual = probes.get("estoque_anual", {"status": "ausente", "rows": 0})
    bloco_h = probes.get("bloco_h", {"status": "ausente", "rows": 0})

    cards = [
        {
            "id": "eventos_estoque",
            "title": "Eventos",
            "value": describe_count(mov, "evento", "eventos"),
            "description": "Base operacional atual de movimentação que será evoluída para a visão canônica de eventos fiscais.",
        },
        {
            "id": "saldo_mensal",
            "title": "Saldo mensal",
            "value": describe_count(mensal, "linha mensal", "linhas mensais"),
            "description": "Série mensal atual usada como ponte para a futura camada canônica de saldo e divergência.",
        },
        {
            "id": "inventario",
            "title": "Inventário",
            "value": describe_count(bloco_h, "linha de inventário", "linhas de inventário"),
            "description": "Snapshot fiscal reaproveitado como base para a separação entre evento, snapshot e saldo.",
        },
    ]
    datasets = [
        {
            "id": "estoque_eventos_legado",
            "label": "Movimentação de estoque",
            "stage": stage_label(mov),
            "description": "Dataset legado usado como ponto de partida para a camada canônica de eventos.",
        },
        {
            "id": "estoque_saldo_mensal_legado",
            "label": "Tabela mensal",
            "stage": stage_label(mensal),
            "description": "Série mensal legada reaproveitada na transição para a camada canônica de saldo.",
        },
        {
            "id": "estoque_saldo_anual_legado",
            "label": "Tabela anual",
            "stage": stage_label(anual),
            "description": "Série anual legada reaproveitada na transição para a camada canônica de saldo.",
        },
        {
            "id": "estoque_inventario_bloco_h",
            "label": "Bloco H",
            "stage": stage_label(bloco_h),
            "description": "Inventário EFD usado como snapshot fiscal para confronto com eventos e saldos.",
        },
    ]
    next_steps = [
        "separar eventos, snapshots, estoque declarado e saldos",
        "abrir workbench de divergências entre movimentação, inventário e estoque declarado",
        "materializar datasets próprios de evento, snapshot, saldo e divergência",
    ]
    legacy_shortcuts = [
        {
            "id": "estoque",
            "label": "Estoque (legado)",
            "description": (
                f"Movimentação: {describe_count(mov, 'linha', 'linhas')}; "
                f"mensal: {describe_count(mensal, 'linha', 'linhas')}; "
                f"anual: {describe_count(anual, 'linha', 'linhas')}"
            ),
        }
    ]
    summary = build_domain_summary(
        domain="estoque",
        title="Estoque",
        subtitle="Eventos, inventário, saldos e divergências do estoque fiscal.",
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
    return {"status": "ok", "domain": "estoque"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(sanitize_cnpj(cnpj))


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    payload = _payload(cnpj_sanitized)
    return build_dataset_listing("estoque", cnpj_sanitized, payload["datasets"])


@router.get("/mov")
def mov_rows(
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
        analysis_paths(cnpj_sanitized)["mov_estoque"],
        page,
        page_size,
        sort_by,
        sort_desc,
        filter_text,
        filter_column,
        filter_value,
    )


@router.get("/mensal")
def mensal_rows(
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
        analysis_paths(cnpj_sanitized)["estoque_mensal"],
        page,
        page_size,
        sort_by,
        sort_desc,
        filter_text,
        filter_column,
        filter_value,
    )


@router.get("/anual")
def anual_rows(
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
        analysis_paths(cnpj_sanitized)["estoque_anual"],
        page,
        page_size,
        sort_by,
        sort_desc,
        filter_text,
        filter_column,
        filter_value,
    )


@router.get("/bloco-h")
def bloco_h_rows(
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
        analysis_paths(cnpj_sanitized)["bloco_h"],
        page,
        page_size,
        sort_by,
        sort_desc,
        filter_text,
        filter_column,
        filter_value,
    )
