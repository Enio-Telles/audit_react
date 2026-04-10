from __future__ import annotations

from pathlib import Path
from typing import Any

from .fiscal_storage import probe_materialized

FiscalPayload = dict[str, Any]


def build_domain_summary(
    *,
    domain: str,
    title: str,
    subtitle: str,
    cnpj: str | None,
    cards: list[dict[str, str]],
    datasets: list[dict[str, str]],
    next_steps: list[str],
    legacy_shortcuts: list[dict[str, str]] | None = None,
) -> FiscalPayload:
    return {
        "domain": domain,
        "title": title,
        "subtitle": subtitle,
        "cnpj": cnpj,
        "status": "aguardando_cnpj" if not cnpj else "pronto_para_materializacao",
        "pipeline": "sql -> parquet|delta -> api -> ui",
        "cards": cards,
        "datasets": datasets,
        "next_steps": next_steps,
        "legacy_shortcuts": legacy_shortcuts or [],
    }


def build_dataset_listing(domain: str, cnpj: str | None, datasets: list[dict[str, str]]) -> FiscalPayload:
    return {
        "domain": domain,
        "cnpj": cnpj,
        "count": len(datasets),
        "items": datasets,
    }


def probe_parquet(path: Path) -> dict[str, Any]:
    return probe_materialized(path)


def stage_label(probe: dict[str, Any]) -> str:
    if probe["status"] == "materializado":
        fmt = probe.get("format", "parquet")
        return f"materializado {fmt} ({probe['rows']} linhas)"
    if probe["status"] == "erro":
        return "erro ao ler"
    return "ausente"
