from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable


ColumnType = str


_TYPE_PRIORITY: tuple[tuple[type, ColumnType], ...] = (
    (bool, "boolean"),
    (int, "integer"),
    (float, "number"),
)


def _column_type_from_rows(rows: list[dict[str, Any]], key: str) -> ColumnType:
    for row in rows:
        value = row.get(key)
        if value is None:
            continue
        for python_type, column_type in _TYPE_PRIORITY:
            if isinstance(value, python_type):
                return column_type
        return "string"
    return "string"


def _normalize_visible_columns(
    requested_columns: Iterable[str] | None,
    available_columns: Iterable[str],
) -> list[str]:
    available = list(dict.fromkeys(available_columns))
    if not requested_columns:
        return available
    requested = [column for column in requested_columns if column in available]
    return requested or available


def build_columns_metadata(
    all_columns: Iterable[str],
    rows: list[dict[str, Any]],
    visible_columns: Iterable[str] | None = None,
) -> list[dict[str, Any]]:
    visible = set(_normalize_visible_columns(visible_columns, all_columns))
    metadata: list[dict[str, Any]] = []
    for key in all_columns:
        metadata.append(
            {
                "key": key,
                "label": key.replace("_", " ").title(),
                "type": _column_type_from_rows(rows, key),
                "visible": key in visible,
                "sortable": True,
                "filterable": True,
            }
        )
    return metadata


def build_supported_filters(keys: Iterable[str]) -> dict[str, dict[str, Any]]:
    supported: dict[str, dict[str, Any]] = {}
    for key in keys:
        if key.endswith("data") or key.endswith("_data") or key in {"data_inicial", "data_final"}:
            supported[key] = {"type": "date"}
        elif key.endswith("_min") or key.endswith("_max"):
            supported[key] = {"type": "number"}
        else:
            supported[key] = {"type": "text"}
    return supported


def build_view_state(
    *,
    dataset_id: str,
    bloco_fiscal: str,
    cnpj: str,
    title: str,
    filters: dict[str, Any],
    sort_by: str | None,
    sort_direction: str,
    visible_columns: list[str],
    page: int,
    page_size: int,
    perfil: str = "default",
) -> dict[str, Any]:
    return {
        "datasetId": dataset_id,
        "blocoFiscal": bloco_fiscal,
        "cnpj": cnpj,
        "title": title,
        "filters": filters,
        "sorting": {
            "sort_by": sort_by,
            "sort_direction": sort_direction,
        },
        "visible_columns": visible_columns,
        "page": page,
        "page_size": page_size,
        "perfil": perfil,
    }


def build_table_payload(
    *,
    dataset_id: str,
    bloco_fiscal: str,
    cnpj: str,
    title: str,
    legacy_page: dict[str, Any],
    filters_applied: dict[str, Any],
    filters_supported: dict[str, dict[str, Any]],
    sort_by: str | None,
    sort_direction: str,
    visible_columns: Iterable[str] | None = None,
    source: str = "parquet",
) -> dict[str, Any]:
    rows = legacy_page.get("rows", []) or []
    all_columns = legacy_page.get("all_columns") or legacy_page.get("columns") or []
    normalized_visible_columns = _normalize_visible_columns(visible_columns, all_columns)
    columns_metadata = build_columns_metadata(all_columns, rows, normalized_visible_columns)

    view_state = build_view_state(
        dataset_id=dataset_id,
        bloco_fiscal=bloco_fiscal,
        cnpj=cnpj,
        title=title,
        filters=filters_applied,
        sort_by=sort_by,
        sort_direction=sort_direction,
        visible_columns=normalized_visible_columns,
        page=int(legacy_page.get("page", 1) or 1),
        page_size=int(legacy_page.get("page_size", 50) or 50),
    )

    return {
        "datasetId": dataset_id,
        "blocoFiscal": bloco_fiscal,
        "cnpj": cnpj,
        "title": title,
        "columns": columns_metadata,
        "rows": rows,
        "filters": {
            "applied": filters_applied,
            "supported": filters_supported,
        },
        "sorting": {
            "sort_by": sort_by,
            "sort_direction": sort_direction,
        },
        "pagination": {
            "page": int(legacy_page.get("page", 1) or 1),
            "page_size": int(legacy_page.get("page_size", 50) or 50),
            "total_rows": int(legacy_page.get("total_rows", 0) or 0),
            "total_pages": int(legacy_page.get("total_pages", 1) or 1),
        },
        "export": {
            "enabled": True,
            "formatos": ["xlsx", "csv", "parquet"],
        },
        "detach": {
            "enabled": True,
            "view_state": view_state,
        },
        "meta": {
            "source": source,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "provenance": legacy_page.get("_provenance"),
        },
    }
