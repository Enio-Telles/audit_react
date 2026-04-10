from __future__ import annotations

from fastapi import APIRouter

from .fiscal_summary import build_dataset_listing, build_domain_summary

router = APIRouter()


def _payload(cnpj: str | None) -> dict[str, object]:
    cards = [
        {
            "id": "blocos",
            "title": "Blocos cobertos",
            "value": "0, C, D, E, H, K, 1, 9",
            "description": "A camada EFD deve espelhar a organização oficial por blocos.",
        },
        {
            "id": "registros",
            "title": "Registros prioritários",
            "value": "C100, C170, C176, C197, H010, K200",
            "description": "Os primeiros datasets devem nascer desses registros e seus vínculos.",
        },
        {
            "id": "contrato",
            "title": "Contrato de leitura",
            "value": "Parquet canônico",
            "description": "A UI não deve depender de SQL de tela para explicar a escrituração.",
        },
    ]
    datasets = [
        {
            "id": "efd_arquivos_validos",
            "label": "Arquivos válidos",
            "stage": "planejado",
            "description": "Seleciona arquivo original ou retificador válido por competência.",
        },
        {
            "id": "efd_resumo_bloco",
            "label": "Resumo por bloco",
            "stage": "planejado",
            "description": "Resume volume e presença de registros por bloco fiscal.",
        },
        {
            "id": "efd_c100_c170",
            "label": "Documentos e itens da EFD",
            "stage": "planejado",
            "description": "Base para espelhos de documentos escriturados e cruzamentos posteriores.",
        },
    ]
    next_steps = [
        "mapear SQLs base para seleção de EFD válida e extemporâneos",
        "materializar datasets por bloco e por registro prioritário",
        "expor detalhe, árvore e dicionário por dataset canônico",
    ]
    return build_domain_summary(
        domain="efd",
        title="EFD",
        subtitle="Resumo, blocos, registros, árvore e dicionário da escrituração.",
        cnpj=cnpj,
        cards=cards,
        datasets=datasets,
        next_steps=next_steps,
    )


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "domain": "efd"}


@router.get("/resumo")
def resumo(cnpj: str | None = None) -> dict[str, object]:
    return _payload(cnpj)


@router.get("/datasets")
def datasets(cnpj: str | None = None) -> dict[str, object]:
    payload = _payload(cnpj)
    return build_dataset_listing("efd", cnpj, payload["datasets"])
