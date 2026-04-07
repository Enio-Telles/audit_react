from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DossieAliasSql:
    """Mapeia uma seção lógica do dossiê para SQLs já existentes no catálogo."""

    secao_id: str
    sql_id: str
    prioridade: int = 0
    observacao: str | None = None


ALIASES_DOSSIE_SQL: tuple[DossieAliasSql, ...] = (
    DossieAliasSql(
        secao_id="cadastro",
        sql_id="dados_cadastrais.sql",
        prioridade=10,
        observacao="Consulta prioritária para reaproveitamento de dados cadastrais.",
    ),
    DossieAliasSql(
        secao_id="documentos_fiscais",
        sql_id="NFe.sql",
        prioridade=20,
        observacao="Consulta principal de NF-e para o dossiê.",
    ),
    DossieAliasSql(
        secao_id="documentos_fiscais",
        sql_id="NFCe.sql",
        prioridade=15,
        observacao="Consulta complementar de NFC-e para o dossiê.",
    ),
)


def listar_aliases_dossie() -> list[DossieAliasSql]:
    """Retorna todos os aliases cadastrados para o dossiê."""

    return list(ALIASES_DOSSIE_SQL)


def listar_aliases_por_secao(secao_id: str) -> list[DossieAliasSql]:
    """Retorna aliases ordenados por prioridade decrescente para uma seção."""

    chave = secao_id.strip().lower()
    return sorted(
        [alias for alias in ALIASES_DOSSIE_SQL if alias.secao_id == chave],
        key=lambda item: item.prioridade,
        reverse=True,
    )


def listar_sql_ids_por_secao(secao_id: str) -> list[str]:
    """Retorna somente os SQL IDs associados à seção do dossiê."""

    return [alias.sql_id for alias in listar_aliases_por_secao(secao_id)]
