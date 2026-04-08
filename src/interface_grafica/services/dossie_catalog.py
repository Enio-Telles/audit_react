from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DossieSecao:
    """Representa uma seção navegável do dossiê."""

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
        descricao="Dados cadastrais, situação e histórico básico do contribuinte.",
        tipo_fonte="mixed",
        sql_ids_prioritarios=("dados_cadastrais.sql",),
    ),
    DossieSecao(
        id="documentos_fiscais",
        titulo="Documentos fiscais",
        descricao="Visões de NF-e e NFC-e com prioridade para reaproveitamento do catálogo SQL.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("NFe.sql", "NFCe.sql"),
    ),
    DossieSecao(
        id="arrecadacao",
        titulo="Arrecadação e conta corrente",
        descricao="Situação fiscal, arrecadação e conta corrente do contribuinte.",
        tipo_fonte="mixed",
        sql_ids_prioritarios=(),
    ),
    DossieSecao(
        id="enderecos",
        titulo="Endereços",
        descricao="Histórico de endereços vinculados e mapeados das notas fiscais.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_enderecos.sql",),
    ),
    DossieSecao(
        id="historico_situacao",
        titulo="Histórico de Situação",
        descricao="Histórico completo de alterações de status e situação cadastral.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_historico_situacao.sql",),
    ),
    DossieSecao(
        id="regime_pagamento",
        titulo="Regime de Pagamento",
        descricao="Evolução e períodos de regimes de pagamento (ex: Simples Nacional, Normal).",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_regime_pagamento.sql",),
    ),
    DossieSecao(
        id="atividades",
        titulo="Atividades Econômicas",
        descricao="Mapeamento de CNAEs principal e secundárias do CNPJ.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_atividades.sql",),
    ),
    DossieSecao(
        id="contador",
        titulo="Contador",
        descricao="Histórico de contabilidade vinculada ao contribuinte.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_contador.sql",),
    ),
    DossieSecao(
        id="historico_fac",
        titulo="Histórico FAC",
        descricao="Evolução das Fichas de Atualização Cadastral.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_historico_fac.sql",),
    ),
    DossieSecao(
        id="vistorias",
        titulo="Vistorias",
        descricao="Histórico de vistorias associadas ao contribuinte.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_vistorias.sql",),
    ),
    DossieSecao(
        id="socios",
        titulo="Quadro Societário",
        descricao="Histórico e situação atual dos sócios da empresa.",
        tipo_fonte="sql_catalog",
        sql_ids_prioritarios=("dossie_historico_socios.sql",),
    ),
)


def listar_secoes_dossie() -> list[DossieSecao]:
    """Retorna as seções do dossiê em ordem de navegação."""

    return list(SECOES_DOSSIE)


def obter_secao_dossie(secao_id: str) -> DossieSecao | None:
    """Localiza uma seção do dossiê por identificador."""

    chave = secao_id.strip().lower()
    return next((secao for secao in SECOES_DOSSIE if secao.id == chave), None)


def listar_sql_prioritarias(secao_id: str) -> list[str]:
    """Retorna os SQL IDs prioritários para reuso por seção."""

    secao = obter_secao_dossie(secao_id)
    if secao is None:
        return []
    return list(secao.sql_ids_prioritarios)
