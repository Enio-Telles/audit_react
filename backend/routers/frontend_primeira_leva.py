from __future__ import annotations

from typing import Any

from fastapi import APIRouter

from .fiscal_analysis_support import sanitize_cnpj
from .fiscal_documentos import nfe_rows
from .fiscal_estoque import anual_rows, mensal_rows, mov_rows
from .frontend_table_contract import build_supported_filters, build_table_payload

router = APIRouter()


_DATASET_TITLES = {
    "mov_estoque": "Movimentação de estoque",
    "tabela_mensal": "Tabela mensal",
    "tabela_anual": "Tabela anual",
    "nfe_entrada": "NFe Entrada",
}


def _sort_direction(sort_desc: bool) -> str:
    return "desc" if sort_desc else "asc"


def _visible_columns_from_query(value: str | None) -> list[str] | None:
    if value is None:
        return None
    columns = [item.strip() for item in value.split(",") if item.strip()]
    return columns or None


def _common_filters(
    *,
    filter_text: str | None,
    filter_column: str | None,
    filter_value: str | None,
) -> dict[str, Any]:
    applied: dict[str, Any] = {}
    if filter_text:
        applied["filter_text"] = filter_text
    if filter_column and filter_value:
        applied[filter_column] = filter_value
    return applied


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "domain": "frontend_primeira_leva"}


@router.get("/analise-fiscal/mov-estoque")
def frontend_mov_estoque(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
    visible_columns: str | None = None,
) -> dict[str, Any]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    legacy_page = mov_rows(cnpj_sanitized or "", page, page_size, sort_by, sort_desc, filter_text, filter_column, filter_value)
    filters_applied = _common_filters(
        filter_text=filter_text,
        filter_column=filter_column,
        filter_value=filter_value,
    )
    return build_table_payload(
        dataset_id="mov_estoque",
        bloco_fiscal="analise_fiscal",
        cnpj=cnpj_sanitized or "",
        title=_DATASET_TITLES["mov_estoque"],
        legacy_page=legacy_page,
        filters_applied=filters_applied,
        filters_supported=build_supported_filters(
            [
                "filter_text",
                "id_agrupado",
                "descricao",
                "ncm",
                "tipo_item",
                "data_inicial",
                "data_final",
                "valor_min",
                "valor_max",
            ]
        ),
        sort_by=sort_by,
        sort_direction=_sort_direction(sort_desc),
        visible_columns=_visible_columns_from_query(visible_columns),
    )


@router.get("/analise-fiscal/tabela-mensal")
def frontend_tabela_mensal(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
    visible_columns: str | None = None,
) -> dict[str, Any]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    legacy_page = mensal_rows(cnpj_sanitized or "", page, page_size, sort_by, sort_desc, filter_text, filter_column, filter_value)
    filters_applied = _common_filters(
        filter_text=filter_text,
        filter_column=filter_column,
        filter_value=filter_value,
    )
    return build_table_payload(
        dataset_id="tabela_mensal",
        bloco_fiscal="analise_fiscal",
        cnpj=cnpj_sanitized or "",
        title=_DATASET_TITLES["tabela_mensal"],
        legacy_page=legacy_page,
        filters_applied=filters_applied,
        filters_supported=build_supported_filters(
            [
                "filter_text",
                "id_agrupado",
                "descricao",
                "ano",
                "mes",
                "valor_min",
                "valor_max",
            ]
        ),
        sort_by=sort_by,
        sort_direction=_sort_direction(sort_desc),
        visible_columns=_visible_columns_from_query(visible_columns),
    )


@router.get("/analise-fiscal/tabela-anual")
def frontend_tabela_anual(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
    visible_columns: str | None = None,
) -> dict[str, Any]:
    cnpj_sanitized = sanitize_cnpj(cnpj)
    legacy_page = anual_rows(cnpj_sanitized or "", page, page_size, sort_by, sort_desc, filter_text, filter_column, filter_value)
    filters_applied = _common_filters(
        filter_text=filter_text,
        filter_column=filter_column,
        filter_value=filter_value,
    )
    return build_table_payload(
        dataset_id="tabela_anual",
        bloco_fiscal="analise_fiscal",
        cnpj=cnpj_sanitized or "",
        title=_DATASET_TITLES["tabela_anual"],
        legacy_page=legacy_page,
        filters_applied=filters_applied,
        filters_supported=build_supported_filters(
            [
                "filter_text",
                "id_agrupado",
                "descricao",
                "ano",
                "valor_min",
                "valor_max",
                "selection_anchor",
            ]
        ),
        sort_by=sort_by,
        sort_direction=_sort_direction(sort_desc),
        visible_columns=_visible_columns_from_query(visible_columns),
    )


@router.get("/documentos-fiscais/nfe-entrada")
def frontend_nfe_entrada(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
    visible_columns: str | None = None,
) -> dict[str, Any]:
    legacy_page = nfe_rows(cnpj, page, page_size, sort_by, sort_desc, filter_text, filter_column, filter_value)
    cnpj_sanitized = sanitize_cnpj(cnpj)
    filters_applied = _common_filters(
        filter_text=filter_text,
        filter_column=filter_column,
        filter_value=filter_value,
    )
    return build_table_payload(
        dataset_id="nfe_entrada",
        bloco_fiscal="documentos_fiscais",
        cnpj=cnpj_sanitized or "",
        title=_DATASET_TITLES["nfe_entrada"],
        legacy_page=legacy_page,
        filters_applied=filters_applied,
        filters_supported=build_supported_filters(
            [
                "filter_text",
                "id_agrupado",
                "descricao",
                "ncm",
                "co_sefin",
                "data_inicial",
                "data_final",
            ]
        ),
        sort_by=sort_by,
        sort_direction=_sort_direction(sort_desc),
        visible_columns=_visible_columns_from_query(visible_columns),
    )
