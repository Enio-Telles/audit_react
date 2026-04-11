from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DossieSecao:
    """Representa uma secao navegavel do dossie."""

    id: str
    titulo: str
    descricao: str
    tipo_fonte: str
    sql_ids_prioritarios: tuple[str, ...] = ()
    exige_cnpj: bool = True


SECOES_DOSSIE: tuple[DossieSecao, ...] = (
    DossieSecao(
        id="cadastro",
        titulo="Cadastro",
        descricao="Dados cadastrais, situacao e historico basico do contribuinte.",
        tipo_fonte="mixed",
        sql_ids_prioritarios=("dados_cadastrais.sql",),
    ),
    DossieSecao(
        id="documentos_fiscais",
        titulo="Documentos fiscais",
        descricao="Visoes de NF-e e NFC-e com prioridade para reaproveitamento do catalogo SQL.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("NFe.sql", "NFCe.sql"),
    ),
    DossieSecao(
        id="arrecadacao",
        titulo="Arrecadacao e conta corrente",
        descricao="Situacao fiscal, arrecadacao e conta corrente do contribuinte.",
        tipo_fonte="cache_catalog",
        sql_ids_prioritarios=(),
    ),
    DossieSecao(
        id="enderecos",
        titulo="Enderecos",
        descricao="Historico de enderecos vinculados e mapeados das notas fiscais.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_enderecos.sql",),
    ),
    DossieSecao(
        id="historico_situacao",
        titulo="Historico de Situacao",
        descricao="Historico completo de alteracoes de status e situacao cadastral.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_historico_situacao.sql",),
    ),
    DossieSecao(
        id="regime_pagamento",
        titulo="Regime de Pagamento",
        descricao="Evolucao e periodos de regimes de pagamento.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_regime_pagamento.sql",),
    ),
    DossieSecao(
        id="atividades",
        titulo="Atividades Economicas",
        descricao="Mapeamento de CNAEs principal e secundarias do CNPJ.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_atividades.sql",),
    ),
    DossieSecao(
        id="contador",
        titulo="Contador",
        descricao="Historico de contabilidade vinculada ao contribuinte.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_contador.sql",),
    ),
    DossieSecao(
        id="historico_fac",
        titulo="Historico FAC",
        descricao="Evolucao das fichas de atualizacao cadastral.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_historico_fac.sql",),
    ),
    DossieSecao(
        id="vistorias",
        titulo="Vistorias",
        descricao="Historico de vistorias associadas ao contribuinte.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_vistorias.sql",),
    ),
    DossieSecao(
        id="socios",
        titulo="Quadro Societario",
        descricao="Historico e situacao atual dos socios da empresa.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_historico_socios.sql",),
    ),
    DossieSecao(
        id="contato",
        titulo="Contatos",
        descricao="Agenda consolidada de contatos da empresa, socios e contadores com telefones, emails e enderecos.",
        tipo_fonte="composed",
        sql_ids_prioritarios=(
            "dados_cadastrais.sql",
            "dossie_filiais_raiz.sql",
            "dossie_contador.sql",
            "dossie_historico_fac.sql",
            "dossie_rascunho_fac_contador.sql",
            "dossie_req_inscricao_contador.sql",
            "dossie_historico_socios.sql",
            "NFe.sql",
            "NFCe.sql",
        ),
    ),
    DossieSecao(
        id="estoque",
        titulo="Estoque Analitico",
        descricao="Leitura reutilizavel da `mov_estoque` ja materializada para o CNPJ, sem nova extracao Oracle.",
        tipo_fonte="cache_catalog",
        sql_ids_prioritarios=(),
    ),
    DossieSecao(
        id="ressarcimento_st",
        titulo="Ressarcimento ST",
        descricao="Leitura reutilizavel dos artefatos analiticos de ressarcimento ST ja materializados no workspace.",
        tipo_fonte="cache_catalog",
        sql_ids_prioritarios=(),
    ),
)


def listar_secoes_dossie() -> list[DossieSecao]:
    """Retorna as secoes do dossie em ordem de navegacao."""

    return list(SECOES_DOSSIE)


def obter_secao_dossie(secao_id: str) -> DossieSecao | None:
    """Localiza uma secao do dossie por identificador."""

    chave = secao_id.strip().lower()
    return next((secao for secao in SECOES_DOSSIE if secao.id == chave), None)


def listar_sql_prioritarias(secao_id: str) -> list[str]:
    """Retorna os SQL IDs prioritarios para reuso por secao."""

    secao = obter_secao_dossie(secao_id)
    if secao is None:
        return []
    return list(secao.sql_ids_prioritarios)
