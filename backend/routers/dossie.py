from fastapi import APIRouter
from typing import List, Optional
from pydantic import BaseModel

from interface_grafica.services.dossie_catalog import listar_secoes_dossie
from interface_grafica.services.dossie_cache_keys import criar_chave_cache_secao

router = APIRouter()

class DossieSectionSummaryResponse(BaseModel):
    id: str
    title: str
    description: str
    sourceType: str
    status: str
    rowCount: Optional[int] = None

@router.get("/{cnpj}/secoes", response_model=List[DossieSectionSummaryResponse])
def get_secoes(cnpj: str):
    """
    Lista as seções do dossiê disponíveis para um CNPJ, com informações básicas
    sobre se estão cacheadas ou vazias.
    """
    secoes = listar_secoes_dossie()
    resultado = []

    for secao in secoes:
        if secao.exige_cnpj and not cnpj:
            continue

        # Verificar o status de cache usando as rotinas de chave
        cache_key = criar_chave_cache_secao(secao.id, cnpj)
        # TODO: Implementar verificação real de arquivo cache se existir.
        # Por enquanto, retorna idle. No futuro, usar dossie_cache para varrer o path.

        resultado.append(
            DossieSectionSummaryResponse(
                id=secao.id,
                title=secao.titulo,
                description=secao.descricao,
                sourceType=secao.tipo_fonte,
                status="idle",
                rowCount=None
            )
        )

    return resultado
