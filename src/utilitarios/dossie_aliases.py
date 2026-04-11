from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DossieAliasSql:
    """Mapeia uma secao logica do dossie para SQLs ja existentes no catalogo."""

    secao_id: str
    sql_id: str
    prioridade: int = 0
    observacao: str | None = None


ALIASES_DOSSIE_SQL: tuple[DossieAliasSql, ...] = (
    DossieAliasSql(
        secao_id="cadastro",
        sql_id="dados_cadastrais.sql",
        prioridade=10,
        observacao="Consulta prioritaria para reaproveitamento de dados cadastrais.",
    ),
    DossieAliasSql(
        secao_id="documentos_fiscais",
        sql_id="NFe.sql",
        prioridade=20,
        observacao="Consulta principal de NF-e para o dossie.",
    ),
    DossieAliasSql(
        secao_id="documentos_fiscais",
        sql_id="NFCe.sql",
        prioridade=15,
        observacao="Consulta complementar de NFC-e para o dossie.",
    ),
    DossieAliasSql(secao_id="enderecos", sql_id="dossie_enderecos.sql", prioridade=10),
    DossieAliasSql(secao_id="historico_situacao", sql_id="dossie_historico_situacao.sql", prioridade=10),
    DossieAliasSql(secao_id="regime_pagamento", sql_id="dossie_regime_pagamento.sql", prioridade=10),
    DossieAliasSql(secao_id="atividades", sql_id="dossie_atividades.sql", prioridade=10),
    DossieAliasSql(secao_id="contador", sql_id="dossie_contador.sql", prioridade=10),
    DossieAliasSql(secao_id="historico_fac", sql_id="dossie_historico_fac.sql", prioridade=10),
    DossieAliasSql(secao_id="vistorias", sql_id="dossie_vistorias.sql", prioridade=10),
    DossieAliasSql(secao_id="socios", sql_id="dossie_historico_socios.sql", prioridade=10),
    DossieAliasSql(secao_id="contato", sql_id="dados_cadastrais.sql", prioridade=50),
    DossieAliasSql(secao_id="contato", sql_id="dossie_filiais_raiz.sql", prioridade=45),
    DossieAliasSql(secao_id="contato", sql_id="dossie_contador.sql", prioridade=40),
    DossieAliasSql(secao_id="contato", sql_id="dossie_historico_fac.sql", prioridade=35),
    DossieAliasSql(secao_id="contato", sql_id="dossie_rascunho_fac_contador.sql", prioridade=34),
    DossieAliasSql(secao_id="contato", sql_id="dossie_req_inscricao_contador.sql", prioridade=33),
    DossieAliasSql(secao_id="contato", sql_id="dossie_historico_socios.sql", prioridade=30),
    DossieAliasSql(secao_id="contato", sql_id="NFe.sql", prioridade=20),
    DossieAliasSql(secao_id="contato", sql_id="NFCe.sql", prioridade=10),
)


def listar_aliases_dossie() -> list[DossieAliasSql]:
    """Retorna todos os aliases cadastrados para o dossie."""

    return list(ALIASES_DOSSIE_SQL)


def listar_aliases_por_secao(secao_id: str) -> list[DossieAliasSql]:
    """Retorna aliases ordenados por prioridade decrescente para uma secao."""

    chave = secao_id.strip().lower()
    return sorted(
        [alias for alias in ALIASES_DOSSIE_SQL if alias.secao_id == chave],
        key=lambda item: item.prioridade,
        reverse=True,
    )


def listar_sql_ids_por_secao(secao_id: str) -> list[str]:
    """Retorna somente os SQL IDs associados a secao do dossie."""

    return [alias.sql_id for alias in listar_aliases_por_secao(secao_id)]
