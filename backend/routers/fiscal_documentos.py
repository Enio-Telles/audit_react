from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Iterable

from fastapi import APIRouter

from interface_grafica.config import CNPJ_ROOT

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


def _roots(cnpj: str) -> list[Path]:
    base = _base_cnpj(cnpj)
    return [
        base / "arquivos_parquet",
        base / "arquivos_parquet" / "fiscal",
        base / "arquivos_parquet" / "fiscal" / "documentos",
        base / "analises",
        base / "analises" / "produtos",
        base,
    ]


def _first_existing(candidates: list[Path]) -> Path | None:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _search_patterns(roots: Iterable[Path], patterns: list[str]) -> Path | None:
    matches: list[Path] = []
    for root in roots:
        if not root.exists():
            continue
        for pattern in patterns:
            matches.extend(root.rglob(pattern))
    if not matches:
        return None
    unique_sorted = sorted({match for match in matches if match.is_file()}, key=lambda path: (len(path.parts), len(path.name), str(path)))
    return unique_sorted[0] if unique_sorted else None


def _find_document_path(cnpj: str, candidates: list[str], fallback_patterns: list[str]) -> Path:
    roots = _roots(cnpj)
    exact_candidates = [root / candidate for root in roots for candidate in candidates]
    exact = _first_existing(exact_candidates)
    if exact is not None:
        return exact
    fallback = _search_patterns(roots, fallback_patterns)
    if fallback is not None:
        return fallback
    return exact_candidates[0]


def _find_nfe(cnpj: str) -> Path:
    return _find_document_path(
        cnpj,
        [
            f"nfe_{cnpj}.parquet",
            f"nfe_xml_{cnpj}.parquet",
            f"NFe_{cnpj}.parquet",
            f"NFe_xml_{cnpj}.parquet",
            f"docs_nfe_itens_{cnpj}.parquet",
        ],
        [
            f"*nfe*{cnpj}*.parquet",
            f"*NFe*{cnpj}*.parquet",
        ],
    )


def _find_nfce(cnpj: str) -> Path:
    return _find_document_path(
        cnpj,
        [
            f"nfce_{cnpj}.parquet",
            f"nfce_xml_{cnpj}.parquet",
            f"NFCe_{cnpj}.parquet",
            f"NFCe_xml_{cnpj}.parquet",
            f"docs_nfce_itens_{cnpj}.parquet",
        ],
        [
            f"*nfce*{cnpj}*.parquet",
            f"*NFCe*{cnpj}*.parquet",
        ],
    )


def _find_cte(cnpj: str) -> Path:
    return _find_document_path(
        cnpj,
        [
            f"cte_{cnpj}.parquet",
            f"cte_xml_{cnpj}.parquet",
            f"CTe_{cnpj}.parquet",
            f"CTe_xml_{cnpj}.parquet",
            f"docs_cte_{cnpj}.parquet",
        ],
        [
            f"*cte*{cnpj}*.parquet",
            f"*CTe*{cnpj}*.parquet",
        ],
    )


def _find_info_complementar(cnpj: str) -> Path:
    return _find_document_path(
        cnpj,
        [
            f"nfe_info_compl_{cnpj}.parquet",
            f"NFe_info_compl_{cnpj}.parquet",
            f"docs_nfe_info_complementar_{cnpj}.parquet",
        ],
        [
            f"*info_compl*{cnpj}*.parquet",
            f"*complement*{cnpj}*.parquet",
        ],
    )


def _find_contatos(cnpj: str) -> Path:
    return _find_document_path(
        cnpj,
        [
            f"email_nfe_{cnpj}.parquet",
            f"Email_NFe_{cnpj}.parquet",
            f"docs_nfe_contatos_{cnpj}.parquet",
        ],
        [
            f"*email*{cnpj}*.parquet",
            f"*contato*{cnpj}*.parquet",
            f"*telefone*{cnpj}*.parquet",
        ],
    )


def _document_probes(cnpj: str | None) -> dict[str, dict[str, Any]]:
    if not cnpj:
        return {}
    return {
        "nfe": probe_parquet(_find_nfe(cnpj)),
        "nfce": probe_parquet(_find_nfce(cnpj)),
        "cte": probe_parquet(_find_cte(cnpj)),
        "info_complementar": probe_parquet(_find_info_complementar(cnpj)),
        "contatos": probe_parquet(_find_contatos(cnpj)),
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
    probes = _document_probes(cnpj)
    nfe = probes.get("nfe", {"status": "ausente", "rows": 0})
    nfce = probes.get("nfce", {"status": "ausente", "rows": 0})
    cte = probes.get("cte", {"status": "ausente", "rows": 0})
    info = probes.get("info_complementar", {"status": "ausente", "rows": 0})
    contatos = probes.get("contatos", {"status": "ausente", "rows": 0})

    cards = [
        {
            "id": "cobertura_real",
            "title": "Artefatos documentais localizados",
            "value": f"{_materialized_count(probes)} materializado(s)",
            "description": "Primeira ponte dos documentos fiscais baseada nos parquets já produzidos pelo projeto.",
        },
        {
            "id": "nfe",
            "title": "NF-e",
            "value": _describe_count(nfe, "linha de NF-e", "linhas de NF-e"),
            "description": "Representa o espelho primário de notas fiscais eletrônicas que a camada nova deverá consolidar.",
        },
        {
            "id": "cte_nfce",
            "title": "NFC-e e CT-e",
            "value": f"NFC-e: {_describe_count(nfce, 'linha', 'linhas')} | CT-e: {_describe_count(cte, 'linha', 'linhas')}",
            "description": "Mostra o estado atual dos espelhos documentais complementares já materializados para o contribuinte.",
        },
    ]
    datasets = [
        {
            "id": "docs_nfe_legado",
            "label": "NF-e",
            "stage": stage_label(nfe),
            "description": "Espelho ou extração legada de NF-e já existente no acervo do contribuinte.",
        },
        {
            "id": "docs_nfce_legado",
            "label": "NFC-e",
            "stage": stage_label(nfce),
            "description": "Espelho ou extração legada de NFC-e reutilizado como ponte do novo domínio.",
        },
        {
            "id": "docs_cte_legado",
            "label": "CT-e",
            "stage": stage_label(cte),
            "description": "Espelho ou extração legada de CT-e com potencial de composição por papel do contribuinte.",
        },
        {
            "id": "docs_info_complementar_legado",
            "label": "Informações complementares",
            "stage": stage_label(info),
            "description": "Camada textual associada aos documentos, útil para auditoria contextual e leitura humana.",
        },
        {
            "id": "docs_contatos_legado",
            "label": "Contatos extraídos",
            "stage": stage_label(contatos),
            "description": "Emails, telefones ou demais sinais auxiliares extraídos dos documentos fiscais.",
        },
    ]
    next_steps = [
        "normalizar os espelhos documentais por papel do contribuinte",
        "abrir detalhe de documento e item a partir dos artefatos já presentes",
        "substituir a ponte legada por datasets canônicos de NF-e, NFC-e, CT-e e enriquecimentos",
    ]
    summary = build_domain_summary(
        domain="documentos_fiscais",
        title="Documentos Fiscais",
        subtitle="NF-e, NFC-e, CT-e, informações complementares e contatos.",
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
    return {"status": "ok", "domain": "documentos_fiscais"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(_sanitize(cnpj))


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    cnpj_sanitized = _sanitize(cnpj)
    payload = _payload(cnpj_sanitized)
    return build_dataset_listing("documentos_fiscais", cnpj_sanitized, payload["datasets"])
