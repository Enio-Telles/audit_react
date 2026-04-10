from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import polars as pl
from fastapi import APIRouter

from utilitarios.project_paths import APP_STATE_ROOT

from .fiscal_dataset_locator import locate_dataset
from .fiscal_storage import read_materialized_frame, resolve_materialized_path
from .fiscal_summary import (
    build_dataset_listing,
    build_domain_summary,
    probe_parquet,
    stage_label,
)

router = APIRouter()

DSF_ACERVO_PATH = APP_STATE_ROOT / "fisconforme_dsfs.json"


def _sanitize(cnpj: str | None) -> str | None:
    if cnpj is None:
        return None
    cleaned = re.sub(r"\D", "", cnpj)
    return cleaned or None


def _dados_cadastrais_path(cnpj: str) -> Path:
    return locate_dataset(cnpj, "dados_cadastrais")


def _malhas_path(cnpj: str) -> Path:
    return locate_dataset(cnpj, "malhas")


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


def _read_first_record(path: Path) -> dict[str, Any]:
    resolved = resolve_materialized_path(path)
    if not resolved.exists():
        return {}
    try:
        df = read_materialized_frame(resolved)
        if df.is_empty():
            return {}
        row = df.row(0, named=True)
        return {key: _safe_value(value) for key, value in row.items()}
    except Exception:
        return {}


def _related_dsfs(cnpj: str | None) -> list[dict[str, Any]]:
    if cnpj is None or not DSF_ACERVO_PATH.exists():
        return []
    try:
        raw = json.loads(DSF_ACERVO_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(raw, list):
        return []

    resultados: list[dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        cnpjs = item.get("cnpjs", []) or []
        if not any(re.sub(r"\D", "", str(valor or "")) == cnpj for valor in cnpjs):
            continue
        resultados.append(
            {
                "id": str(item.get("id", "") or ""),
                "dsf": str(item.get("dsf", "") or ""),
                "referencia": str(item.get("referencia", "") or ""),
                "auditor": str(item.get("auditor", "") or ""),
                "cargo_titulo": str(item.get("cargo_titulo", "") or ""),
                "orgao_origem": str(item.get("orgao_origem", "") or ""),
                "updated_at": str(item.get("updated_at", "") or ""),
                "created_at": str(item.get("created_at", "") or ""),
                "pdf_file_name": str(item.get("pdf_file_name", "") or ""),
                "cnpjs_count": len(list(cnpjs)),
            }
        )
    resultados.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
    return resultados


def _fiscalizacao_probes(cnpj: str | None) -> dict[str, dict[str, Any]]:
    if not cnpj:
        return {}
    return {
        "dados_cadastrais": probe_parquet(_dados_cadastrais_path(cnpj)),
        "malhas": probe_parquet(_malhas_path(cnpj)),
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


def _payload(cnpj: str | None) -> dict[str, object]:
    probes = _fiscalizacao_probes(cnpj)
    cadastral = probes.get("dados_cadastrais", {"status": "ausente", "rows": 0})
    malhas = probes.get("malhas", {"status": "ausente", "rows": 0})
    dsf_count = len(_related_dsfs(cnpj))

    cards = [
        {
            "id": "cobertura_real",
            "title": "Artefatos fiscalizatórios localizados",
            "value": f"{_materialized_count(probes)} materializado(s)",
            "description": "A ponte atual de fiscalização usa o cache real do Fisconforme por CNPJ e o acervo de DSFs do sistema.",
        },
        {
            "id": "malhas",
            "title": "Malhas e pendências",
            "value": _describe_count(malhas, "malha", "malhas"),
            "description": "Reflete o dataset de malhas salvo no cache do Fisconforme para o contribuinte selecionado.",
        },
        {
            "id": "resolucoes",
            "title": "DSFs e resoluções relacionadas",
            "value": f"{dsf_count} item(ns) do acervo",
            "description": "Conta itens do acervo de DSFs vinculados ao CNPJ, servindo como sinal inicial de histórico fiscalizatório e resolução.",
        },
    ]
    datasets = [
        {
            "id": "fiscalizacao_fisconforme_cadastro",
            "label": "Cadastro Fisconforme",
            "stage": stage_label(cadastral),
            "description": "Cache cadastral do Fisconforme reaproveitado como base de leitura do domínio fiscalizatório.",
        },
        {
            "id": "fiscalizacao_fisconforme_malhas",
            "label": "Malhas Fisconforme",
            "stage": stage_label(malhas),
            "description": "Cache de malhas e pendências do Fisconforme por CNPJ.",
        },
        {
            "id": "fiscalizacao_resolucoes_dsf",
            "label": "Acervo de DSFs",
            "stage": f"{dsf_count} relacionado(s)",
            "description": "Acervo persistido de DSFs/notificações, usado como primeira ponte para histórico e resolução.",
        },
        {
            "id": "fiscalizacao_fronteira",
            "label": "Fronteira",
            "stage": "ausente",
            "description": "Ainda não há artefato de fronteira integrado nesta ponte inicial. Permanecerá como próximo eixo de expansão.",
        },
    ]
    next_steps = [
        "ligar as chaves e malhas do Fisconforme a uma tabela navegável do novo domínio",
        "integrar artefatos de fronteira quando o projeto já os materializar por CNPJ",
        "abrir histórico de resoluções a partir do acervo de DSFs e seus desdobramentos",
    ]
    summary = build_domain_summary(
        domain="fiscalizacao",
        title="Fiscalização",
        subtitle="Fronteira, Fisconforme, malhas, chaves e resoluções.",
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
    return {"status": "ok", "domain": "fiscalizacao"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(_sanitize(cnpj))


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    cnpj_sanitized = _sanitize(cnpj)
    payload = _payload(cnpj_sanitized)
    return build_dataset_listing("fiscalizacao", cnpj_sanitized, payload["datasets"])


@router.get("/cadastro")
def cadastro(cnpj: str) -> dict[str, Any]:
    cnpj_sanitized = _sanitize(cnpj)
    if not cnpj_sanitized:
        return {}
    return _read_first_record(_dados_cadastrais_path(cnpj_sanitized))


@router.get("/malhas")
def malhas_rows(
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
        _malhas_path(cnpj_sanitized),
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_desc=sort_desc,
        filter_text=filter_text,
        filter_column=filter_column,
        filter_value=filter_value,
    )


@router.get("/dsfs")
def dsfs(cnpj: str) -> list[dict[str, Any]]:
    return _related_dsfs(_sanitize(cnpj))
