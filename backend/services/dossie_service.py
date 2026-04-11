"""
Facade do backend para os modulos canonicos do dossie.
"""
from __future__ import annotations

from utilitarios.dossie_catalog import (
    listar_secoes_dossie,
    listar_sql_prioritarias,
    obter_secao_dossie,
)
from utilitarios.dossie_extraction_service import (
    executar_sync_secao,
    obter_caminho_historico_comparacao_contato,
)
from utilitarios.dossie_resolution import resolver_secao_dossie

__all__ = [
    "listar_secoes_dossie",
    "listar_sql_prioritarias",
    "obter_secao_dossie",
    "resolver_secao_dossie",
    "obter_caminho_historico_comparacao_contato",
    "executar_sync_secao",
]
