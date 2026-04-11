from __future__ import annotations

from dataclasses import dataclass

from utilitarios.dossie_aliases import listar_aliases_por_secao
from utilitarios.dossie_cache_keys import gerar_chave_cache_dossie
from utilitarios.dossie_cache_keys import gerar_nome_arquivo_cache_dossie
from utilitarios.dossie_catalog import DossieSecao
from utilitarios.dossie_catalog import obter_secao_dossie


@dataclass(frozen=True)
class DossieResolvido:
    """Representa a resolução mínima de uma seção do dossiê."""

    secao_id: str
    titulo: str
    descricao: str
    tipo_fonte: str
    sql_ids: tuple[str, ...]
    cache_key: str
    cache_file_name: str


class DossieResolucaoErro(ValueError):
    """Erro de resolução de seção do dossiê."""



def resolver_secao_dossie(
    cnpj: str,
    secao_id: str,
    parametros: dict | None = None,
    versao_consulta: str | None = None,
) -> DossieResolvido:
    """Resolve a seção do dossiê com aliases SQL e chave de cache."""

    secao: DossieSecao | None = obter_secao_dossie(secao_id)
    if secao is None:
        raise DossieResolucaoErro(f"Seção de dossiê desconhecida: {secao_id}")

    aliases = listar_aliases_por_secao(secao.id)
    sql_ids = tuple(alias.sql_id for alias in aliases) or tuple(secao.sql_ids_prioritarios)
    cache_key = gerar_chave_cache_dossie(
        cnpj=cnpj,
        secao=secao.id,
        parametros=parametros,
        versao_consulta=versao_consulta,
    )
    cache_file_name = gerar_nome_arquivo_cache_dossie(
        cnpj=cnpj,
        secao=secao.id,
        parametros=parametros,
        versao_consulta=versao_consulta,
    )

    return DossieResolvido(
        secao_id=secao.id,
        titulo=secao.titulo,
        descricao=secao.descricao,
        tipo_fonte=secao.tipo_fonte,
        sql_ids=sql_ids,
        cache_key=cache_key,
        cache_file_name=cache_file_name,
    )
