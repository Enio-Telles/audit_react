from __future__ import annotations

from fastapi import APIRouter

from .fiscal_summary import build_dataset_listing, build_domain_summary

router = APIRouter()


def _payload(cnpj: str | None) -> dict[str, object]:
    cards = [
        {
            "id": "fronteira",
            "title": "Fronteira",
            "value": "Eventos e sinais externos",
            "description": "A área fiscalizatória deve consolidar ocorrências operacionais relevantes.",
        },
        {
            "id": "fisconforme",
            "title": "Fisconforme",
            "value": "Malhas, chaves e pendências",
            "description": "O domínio fiscal deve absorver sinais já produzidos pelo modo de análise em lote.",
        },
        {
            "id": "desfecho",
            "title": "Rastreabilidade",
            "value": "Pendência até resolução",
            "description": "Cada ocorrência deve evoluir de sinal a resolução com histórico visível.",
        },
    ]
    datasets = [
        {
            "id": "fiscalizacao_fronteira",
            "label": "Fronteira",
            "stage": "planejado",
            "description": "Consolida dados de fiscalização relacionados a fronteira e eventos correlatos.",
        },
        {
            "id": "fiscalizacao_fisconforme_chaves",
            "label": "Chaves e malhas",
            "stage": "planejado",
            "description": "Representa chaves, malhas, status e resolução do Fisconforme.",
        },
        {
            "id": "fiscalizacao_resolucoes",
            "label": "Resoluções",
            "stage": "planejado",
            "description": "Mantém trilha de desfechos, ações e contexto fiscalizatório.",
        },
    ]
    next_steps = [
        "unificar sinais de fronteira e Fisconforme em datasets padronizados",
        "abrir filtros por status, período, malha e chave fiscal",
        "exibir histórico de resolução com rastreabilidade de origem",
    ]
    return build_domain_summary(
        domain="fiscalizacao",
        title="Fiscalização",
        subtitle="Fronteira, Fisconforme, malhas, chaves e resoluções.",
        cnpj=cnpj,
        cards=cards,
        datasets=datasets,
        next_steps=next_steps,
    )


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "domain": "fiscalizacao"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(cnpj)


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    payload = _payload(cnpj)
    return build_dataset_listing("fiscalizacao", cnpj, payload["datasets"])
