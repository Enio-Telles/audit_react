from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter

from interface_grafica.config import CNPJ_ROOT
from utilitarios.project_paths import APP_STATE_ROOT

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


def _fisconforme_dir(cnpj: str) -> Path:
    return CNPJ_ROOT / cnpj / "fisconforme"


def _dados_cadastrais_path(cnpj: str) -> Path:
    return _fisconforme_dir(cnpj) / "dados_cadastrais.parquet"


def _malhas_path(cnpj: str) -> Path:
    return _fisconforme_dir(cnpj) / "malhas.parquet"


def _count_related_dsfs(cnpj: str | None) -> int:
    if cnpj is None or not DSF_ACERVO_PATH.exists():
        return 0
    try:
        raw = json.loads(DSF_ACERVO_PATH.read_text(encoding="utf-8"))
    except Exception:
        return 0
    if not isinstance(raw, list):
        return 0

    total = 0
    for item in raw:
        if not isinstance(item, dict):
            continue
        cnpjs = item.get("cnpjs", []) or []
        if any(re.sub(r"\D", "", str(valor or "")) == cnpj for valor in cnpjs):
            total += 1
    return total


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
    dsf_count = _count_related_dsfs(cnpj)

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
            "description": "Reflete o parquet de malhas salvo no cache do Fisconforme para o contribuinte selecionado.",
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
