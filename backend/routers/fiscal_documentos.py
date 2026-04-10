from __future__ import annotations

from fastapi import APIRouter

from .fiscal_summary import build_dataset_listing, build_domain_summary

router = APIRouter()


def _payload(cnpj: str | None) -> dict[str, object]:
    cards = [
        {
            "id": "espelhos",
            "title": "Espelhos prioritários",
            "value": "NF-e, NFC-e e CT-e",
            "description": "Primeira camada de leitura amigável dos documentos fiscais.",
        },
        {
            "id": "papel",
            "title": "Papel do contribuinte",
            "value": "Entrada, saída e tomador",
            "description": "A leitura do documento deve explicitar o papel do contribuinte no fato.",
        },
        {
            "id": "enriquecimento",
            "title": "Enriquecimentos",
            "value": "Info complementar e contatos",
            "description": "Informações textuais e sinais auxiliares devem ficar em datasets próprios.",
        },
    ]
    datasets = [
        {
            "id": "docs_nfe_itens",
            "label": "NF-e por item",
            "stage": "planejado",
            "description": "Espelho detalhado de NF-e com granularidade por item.",
        },
        {
            "id": "docs_cte",
            "label": "CT-e",
            "stage": "planejado",
            "description": "Visão consolidada de CT-e com destaque para tomador e demais papéis.",
        },
        {
            "id": "docs_nfe_info_complementar",
            "label": "Informações complementares",
            "stage": "planejado",
            "description": "Separa textos complementares do corpo principal do documento.",
        },
    ]
    next_steps = [
        "consolidar espelhos de NF-e, NFC-e e CT-e em datasets canônicos",
        "normalizar filtros por papel do contribuinte e período",
        "abrir detalhe de documento sem depender de SQL bruto em tela",
    ]
    return build_domain_summary(
        domain="documentos_fiscais",
        title="Documentos Fiscais",
        subtitle="NF-e, NFC-e, CT-e, informações complementares e contatos.",
        cnpj=cnpj,
        cards=cards,
        datasets=datasets,
        next_steps=next_steps,
    )


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "domain": "documentos_fiscais"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(cnpj)


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    payload = _payload(cnpj)
    return build_dataset_listing("documentos_fiscais", cnpj, payload["datasets"])
