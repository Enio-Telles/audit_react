from __future__ import annotations

from fastapi import APIRouter

from .fiscal_summary import build_dataset_listing, build_domain_summary

router = APIRouter()


def _payload(cnpj: str | None) -> dict[str, object]:
    cards = [
        {
            "id": "cruzamentos",
            "title": "Cruzamentos",
            "value": "EFD x documentos x demais fontes",
            "description": "Núcleo analítico para divergências, omissões e correlação entre bases.",
        },
        {
            "id": "verificacoes",
            "title": "Verificações",
            "value": "Agregação e conversão",
            "description": "Camada de consistência estrutural que sustenta estoque e classificação.",
        },
        {
            "id": "classificacao",
            "title": "Classificação dos produtos",
            "value": "Catálogo mestre e pendências",
            "description": "A próxima expansão nasce dessa base, não de telas isoladas.",
        },
    ]
    datasets = [
        {
            "id": "cross_efd_docs",
            "label": "Cruzamentos EFD x documentos",
            "stage": "planejado",
            "description": "Base para omissões, divergências e diferenças de escrituração.",
        },
        {
            "id": "verificacoes_agregacao",
            "label": "Verificações de agregação",
            "stage": "legado_em_migracao",
            "description": "Recebe a lógica hoje espalhada na aba de agregação.",
        },
        {
            "id": "verificacoes_conversao",
            "label": "Verificações de conversão",
            "stage": "legado_em_migracao",
            "description": "Recebe a lógica hoje espalhada na aba de conversão.",
        },
        {
            "id": "produtos_catalogo_mestre",
            "label": "Catálogo mestre de produtos",
            "stage": "planejado",
            "description": "Base da classificação dos produtos e da governança analítica.",
        },
    ]
    next_steps = [
        "migrar Estoque para a camada de cruzamentos",
        "migrar Agregação e Conversão para a camada de verificações",
        "abrir o primeiro dataset do catálogo mestre de produtos",
    ]
    legacy_shortcuts = [
        {
            "id": "estoque",
            "label": "Estoque (legado)",
            "description": "Permanecerá acessível até a migração completa para cruzamentos.",
        },
        {
            "id": "agregacao",
            "label": "Agregação (legado)",
            "description": "Permanece disponível enquanto a camada de verificações não absorve toda a lógica.",
        },
        {
            "id": "conversao",
            "label": "Conversão (legado)",
            "description": "Permanece disponível enquanto as verificações estruturais não forem reimplantadas.",
        },
    ]
    return build_domain_summary(
        domain="analise",
        title="Análise Fiscal",
        subtitle="Cruzamentos, verificações e classificação dos produtos.",
        cnpj=cnpj,
        cards=cards,
        datasets=datasets,
        next_steps=next_steps,
        legacy_shortcuts=legacy_shortcuts,
    )


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "domain": "analise"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(cnpj)


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    payload = _payload(cnpj)
    return build_dataset_listing("analise", cnpj, payload["datasets"])
