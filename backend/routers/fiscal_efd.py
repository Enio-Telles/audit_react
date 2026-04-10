from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter

from interface_grafica.config import CNPJ_ROOT

from .fiscal_dataset_locator import locate_dataset
from .fiscal_storage import read_materialized_frame, resolve_materialized_path
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


def _base_cnpj(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj


def _first_existing(candidates: list[Path]) -> Path:
    for candidate in candidates:
        resolved = resolve_materialized_path(candidate)
        if resolved.exists():
            return resolved
    return resolve_materialized_path(candidates[0])


def _find_c170(cnpj: str) -> Path:
    return locate_dataset(
        cnpj,
        "c170_xml",
        _base_cnpj(cnpj) / "arquivos_parquet" / f"c170_xml_{cnpj}.parquet",
        _base_cnpj(cnpj) / "arquivos_parquet" / "fiscal" / "efd" / f"c170_xml_{cnpj}.parquet",
        _base_cnpj(cnpj) / "analises" / "produtos" / f"c170_xml_{cnpj}.parquet",
    )


def _find_c176(cnpj: str) -> Path:
    return locate_dataset(
        cnpj,
        "c176_xml",
        _base_cnpj(cnpj) / "arquivos_parquet" / f"c176_xml_{cnpj}.parquet",
        _base_cnpj(cnpj) / "arquivos_parquet" / "fiscal" / "efd" / f"c176_xml_{cnpj}.parquet",
        _base_cnpj(cnpj) / "analises" / "produtos" / f"c176_xml_{cnpj}.parquet",
    )


def _find_bloco_h(cnpj: str) -> Path:
    return locate_dataset(
        cnpj,
        "bloco_h",
        _base_cnpj(cnpj) / "analises" / "produtos" / f"bloco_h_{cnpj}.parquet",
        _base_cnpj(cnpj) / "arquivos_parquet" / f"bloco_h_{cnpj}.parquet",
        _base_cnpj(cnpj) / "arquivos_parquet" / "fiscal" / "efd" / f"bloco_h_{cnpj}.parquet",
    )


def _find_k200(cnpj: str) -> Path:
    return _first_existing(
        [
            _base_cnpj(cnpj) / "arquivos_parquet" / f"k200_{cnpj}.parquet",
            _base_cnpj(cnpj) / "arquivos_parquet" / "fiscal" / "efd" / f"k200_{cnpj}.parquet",
            _base_cnpj(cnpj) / "analises" / "produtos" / f"k200_{cnpj}.parquet",
        ]
    )


def _find_c197(cnpj: str) -> Path:
    return _first_existing(
        [
            _base_cnpj(cnpj) / "arquivos_parquet" / f"c197_agrupado_{cnpj}.parquet",
            _base_cnpj(cnpj) / "arquivos_parquet" / f"c197_{cnpj}.parquet",
            _base_cnpj(cnpj) / "arquivos_parquet" / "fiscal" / "efd" / f"c197_agrupado_{cnpj}.parquet",
            _base_cnpj(cnpj) / "arquivos_parquet" / "fiscal" / "efd" / f"c197_{cnpj}.parquet",
        ]
    )


def _efd_probes(cnpj: str | None) -> dict[str, dict[str, Any]]:
    if not cnpj:
        return {}

    return {
        "c170_xml": probe_parquet(_find_c170(cnpj)),
        "c176_xml": probe_parquet(_find_c176(cnpj)),
        "bloco_h": probe_parquet(_find_bloco_h(cnpj)),
        "k200": probe_parquet(_find_k200(cnpj)),
        "c197": probe_parquet(_find_c197(cnpj)),
    }


def _describe_count(probe: dict[str, Any], singular: str, plural: str) -> str:
    if probe.get("status") == "materializado":
        rows = int(probe.get("rows", 0))
        unidade = singular if rows == 1 else plural
        return f"{rows} {unidade}"
    if probe.get("status") == "erro":
        return "erro de leitura"
    return "não materializado"


def _materialized_count(probes: dict[str, dict[str, Any]]) -> int:
    return sum(1 for probe in probes.values() if probe.get("status") == "materializado")


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


def _apply_filter(df: pl.DataFrame, filter_text: str | None = None) -> pl.DataFrame:
    if not filter_text or df.is_empty():
        return df
    term = filter_text.strip().lower()
    if not term:
        return df
    try:
        exprs = [
            pl.col(column)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.to_lowercase()
            .str.contains(term, literal=True)
            for column in df.columns
        ]
        return df.filter(pl.any_horizontal(exprs))
    except Exception:
        return df


def _apply_column_filter(
    df: pl.DataFrame,
    filter_column: str | None = None,
    filter_value: str | None = None,
) -> pl.DataFrame:
    if not filter_column or not filter_value or filter_column not in df.columns or df.is_empty():
        return df
    term = filter_value.strip().lower()
    if not term:
        return df
    try:
        return df.filter(
            pl.col(filter_column)
            .cast(pl.Utf8, strict=False)
            .fill_null("")
            .str.to_lowercase()
            .str.contains(term, literal=True)
        )
    except Exception:
        return df


def _page_from_parquet(
    path: Path,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
) -> dict[str, Any]:
    resolved = resolve_materialized_path(path)
    if not resolved.exists():
        return _empty_page(page, page_size)

    df = read_materialized_frame(resolved)
    df = _apply_filter(df, filter_text)
    df = _apply_column_filter(df, filter_column, filter_value)
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
    probes = _efd_probes(cnpj)
    c170 = probes.get("c170_xml", {"status": "ausente", "rows": 0})
    c176 = probes.get("c176_xml", {"status": "ausente", "rows": 0})
    bloco_h = probes.get("bloco_h", {"status": "ausente", "rows": 0})
    k200 = probes.get("k200", {"status": "ausente", "rows": 0})
    c197 = probes.get("c197", {"status": "ausente", "rows": 0})

    cards = [
        {
            "id": "cobertura_real",
            "title": "Artefatos EFD localizados",
            "value": f"{_materialized_count(probes)} materializado(s)",
            "description": "Primeira ponte da EFD baseada nos datasets já produzidos pelo pipeline e pelas camadas legadas.",
        },
        {
            "id": "itens_efd",
            "title": "Itens e ajustes",
            "value": _describe_count(c170, "linha C170", "linhas C170"),
            "description": "C170 representa itens escriturados e é a base operacional mais forte da ponte inicial da EFD.",
        },
        {
            "id": "inventario",
            "title": "Inventário e estoque fiscal",
            "value": _describe_count(bloco_h, "linha de inventário", "linhas de inventário"),
            "description": "Bloco H já aparece na camada atual e ajuda a explicar o elo entre EFD e estoque fiscal.",
        },
    ]
    datasets = [
        {
            "id": "efd_c170_xml_legado",
            "label": "C170 XML",
            "stage": stage_label(c170),
            "description": "Base legada de itens escriturados que alimenta estoque e cruzamentos posteriores.",
        },
        {
            "id": "efd_c176_xml_legado",
            "label": "C176 XML",
            "stage": stage_label(c176),
            "description": "Base legada de ressarcimento e vínculos complementares derivados da EFD.",
        },
        {
            "id": "efd_bloco_h_legado",
            "label": "Bloco H",
            "stage": stage_label(bloco_h),
            "description": "Inventário real declarado, já reaproveitado na camada de estoque e auditoria.",
        },
        {
            "id": "efd_c197_legado",
            "label": "C197",
            "stage": stage_label(c197),
            "description": "Ajustes e observações fiscais quando esse artefato estiver materializado no CNPJ.",
        },
        {
            "id": "efd_k200_legado",
            "label": "K200",
            "stage": stage_label(k200),
            "description": "Posição de estoque escriturada quando esse artefato já existir no acervo do contribuinte.",
        },
    ]
    next_steps = [
        "localizar e materializar a camada de arquivos válidos e retificadoras",
        "abrir visão por bloco e por registro a partir dos datasets já presentes",
        "expandir a ponte real para C100, C197 e K200 quando esses artefatos estiverem disponíveis",
    ]
    summary = build_domain_summary(
        domain="efd",
        title="EFD",
        subtitle="Resumo, blocos, registros, árvore e dicionário da escrituração.",
        cnpj=cnpj,
        cards=cards,
        datasets=datasets,
        next_steps=next_steps,
    )
    if cnpj:
        summary["status"] = "ponte_legada_ativa"
    return summary


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "domain": "efd"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(_sanitize(cnpj))


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    cnpj_sanitized = _sanitize(cnpj)
    payload = _payload(cnpj_sanitized)
    return build_dataset_listing("efd", cnpj_sanitized, payload["datasets"])


@router.get("/c170")
def c170_rows(
    cnpj: str,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
) -> dict[str, Any]:
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return _empty_page(page, page_size)
    return _page_from_parquet(
        _find_c170(cnpj_sanitized),
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_desc=sort_desc,
        filter_text=filter_text,
        filter_column=filter_column,
        filter_value=filter_value,
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
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return _empty_page(page, page_size)
    return _page_from_parquet(
        _find_bloco_h(cnpj_sanitized),
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_desc=sort_desc,
        filter_text=filter_text,
        filter_column=filter_column,
        filter_value=filter_value,
    )
