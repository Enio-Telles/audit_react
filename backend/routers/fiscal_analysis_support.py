from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import polars as pl

from utilitarios.project_paths import CNPJ_ROOT

from .fiscal_dataset_locator import locate_dataset
from .fiscal_storage import read_materialized_frame, resolve_materialized_path
from .fiscal_summary import probe_parquet


def sanitize_cnpj(cnpj: str | None) -> str | None:
    if cnpj is None:
        return None
    cleaned = re.sub(r"\D", "", cnpj)
    return cleaned or None


def pasta_produtos(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "analises" / "produtos"


def path_bloco_h(cnpj: str) -> Path:
    base_cnpj = CNPJ_ROOT / cnpj
    return locate_dataset(
        cnpj,
        "bloco_h",
        pasta_produtos(cnpj) / f"bloco_h_{cnpj}.parquet",
        base_cnpj / "arquivos_parquet" / f"bloco_h_{cnpj}.parquet",
        base_cnpj / "arquivos_parquet" / "fiscal" / "efd" / f"bloco_h_{cnpj}.parquet",
    )


def analysis_paths(cnpj: str) -> dict[str, Path]:
    pasta = pasta_produtos(cnpj)
    return {
        "mov_estoque": locate_dataset(cnpj, "mov_estoque", pasta / f"mov_estoque_{cnpj}.parquet"),
        "estoque_mensal": locate_dataset(cnpj, "aba_mensal", pasta / f"aba_mensal_{cnpj}.parquet"),
        "estoque_anual": locate_dataset(cnpj, "aba_anual", pasta / f"aba_anual_{cnpj}.parquet"),
        "bloco_h": path_bloco_h(cnpj),
        "fatores_conversao": locate_dataset(cnpj, "fatores_conversao", pasta / f"fatores_conversao_{cnpj}.parquet"),
        "produtos_agrupados": locate_dataset(cnpj, "produtos_agrupados", pasta / f"produtos_agrupados_{cnpj}.parquet"),
        "produtos_final": locate_dataset(cnpj, "produtos_final", pasta / f"produtos_final_{cnpj}.parquet"),
    }


def analysis_probes(cnpj: str | None) -> dict[str, dict[str, Any]]:
    if not cnpj:
        return {}
    return {key: probe_parquet(path) for key, path in analysis_paths(cnpj).items()}


def describe_count(probe: dict[str, Any], singular: str, plural: str) -> str:
    if probe.get("status") == "materializado":
        rows = int(probe.get("rows", 0))
        unidade = singular if rows == 1 else plural
        return f"{rows} {unidade}"
    if probe.get("status") == "erro":
        return "erro de leitura"
    return "não materializado"


def safe_value(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, list):
        return [safe_value(item) for item in v]
    return v


def empty_page(page: int, page_size: int) -> dict[str, Any]:
    return {
        "total_rows": 0,
        "page": page,
        "page_size": page_size,
        "total_pages": 1,
        "columns": [],
        "all_columns": [],
        "rows": [],
    }


def apply_filter(df: pl.DataFrame, filter_text: str | None = None) -> pl.DataFrame:
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


def apply_column_filter(
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


def page_from_parquet(
    path: Path,
    page: int = 1,
    page_size: int = 50,
    sort_by: str | None = None,
    sort_desc: bool = False,
    filter_text: str | None = None,
    filter_column: str | None = None,
    filter_value: str | None = None,
    dataset_id: str | None = None,
    camada: str = "legado",
) -> dict[str, Any]:
    resolved = resolve_materialized_path(path)
    if not resolved.exists():
        result = empty_page(page, page_size)
        result["_provenance"] = {
            "dataset_id": dataset_id,
            "camada": camada,
            "source_path": str(path),
            "resolved": False,
        }
        return result

    df = read_materialized_frame(resolved)
    df = apply_filter(df, filter_text)
    df = apply_column_filter(df, filter_column, filter_value)
    if sort_by and sort_by in df.columns:
        try:
            df = df.sort(sort_by, descending=sort_desc, nulls_last=True)
        except Exception:
            pass

    total = df.height
    start = max(0, (page - 1) * page_size)
    df_page = df.slice(start, page_size)
    rows = [
        {column: safe_value(row[column]) for column in df_page.columns}
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
        "_provenance": {
            "dataset_id": dataset_id,
            "camada": camada,
            "source_path": str(resolved),
            "resolved": True,
        },
    }

