"""Cliente Oracle com utilitarios de conexao e diagnostico."""

from __future__ import annotations

from typing import Any

import oracledb

from configuracao_backend import obter_configuracao_oracle


def _configurar_sessao_oracle(conexao: Any) -> None:
    """Aplica configuracoes de sessao necessarias para extracoes fiscais."""
    cursor = conexao.cursor()
    try:
        # XMLs fiscais usam ponto como separador decimal e dependem dessa sessao.
        cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")
    finally:
        cursor.close()


def criar_conexao_oracle(indice: int = 0) -> Any:
    """Cria conexao Oracle com base no .env."""
    configuracao = obter_configuracao_oracle(indice=indice)
    conexao = oracledb.connect(
        user=configuracao.usuario,
        password=configuracao.senha,
        dsn=configuracao.dsn,
    )
    _configurar_sessao_oracle(conexao)
    return conexao


def testar_conexao_oracle(indice: int = 0) -> dict[str, Any]:
    """Testa conexao Oracle e retorna metadados basicos da sessao."""
    conexao = criar_conexao_oracle(indice=indice)
    try:
        cursor = conexao.cursor()
        cursor.execute(
            """
            SELECT
                USER AS usuario,
                SYS_CONTEXT('USERENV', 'DB_NAME') AS banco,
                SYS_CONTEXT('USERENV', 'SERVER_HOST') AS host
            FROM dual
            """
        )
        usuario, banco, host = cursor.fetchone()
        return {
            "status": "ok",
            "usuario": usuario,
            "banco": banco,
            "host": host,
        }
    finally:
        conexao.close()


def listar_objetos_oracle(termo: str | None = None, limite: int = 200, indice: int = 0) -> list[dict[str, Any]]:
    """Lista tabelas/views Oracle acessiveis com filtro opcional por nome."""
    conexao = criar_conexao_oracle(indice=indice)
    try:
        cursor = conexao.cursor()

        termo_normalizado = (termo or "").strip().upper()

        if termo_normalizado:
            cursor.execute(
                """
                SELECT owner, object_name, object_type
                FROM all_objects
                WHERE object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
                  AND object_name LIKE :termo
                ORDER BY owner, object_name
                FETCH FIRST :limite ROWS ONLY
                """,
                {"termo": f"%{termo_normalizado}%", "limite": int(limite)},
            )
        else:
            cursor.execute(
                """
                SELECT owner, object_name, object_type
                FROM all_objects
                WHERE object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')
                ORDER BY owner, object_name
                FETCH FIRST :limite ROWS ONLY
                """,
                {"limite": int(limite)},
            )

        return [
            {
                "owner": owner,
                "object_name": object_name,
                "object_type": object_type,
            }
            for owner, object_name, object_type in cursor.fetchall()
        ]
    finally:
        conexao.close()


def listar_colunas_objeto_oracle(
    objeto: str,
    owner: str | None = None,
    limite: int = 500,
    indice: int = 0,
) -> list[dict[str, Any]]:
    """Lista colunas de tabela/view Oracle."""
    conexao = criar_conexao_oracle(indice=indice)
    try:
        cursor = conexao.cursor()

        nome_objeto = objeto.strip().upper()
        owner_normalizado = owner.strip().upper() if owner else None

        if owner_normalizado:
            cursor.execute(
                """
                SELECT owner, table_name, column_name, data_type, data_length, data_precision, data_scale, nullable
                FROM all_tab_columns
                WHERE owner = :owner
                  AND table_name = :nome
                ORDER BY column_id
                FETCH FIRST :limite ROWS ONLY
                """,
                {"owner": owner_normalizado, "nome": nome_objeto, "limite": int(limite)},
            )
        else:
            cursor.execute(
                """
                SELECT owner, table_name, column_name, data_type, data_length, data_precision, data_scale, nullable
                FROM all_tab_columns
                WHERE table_name = :nome
                ORDER BY owner, column_id
                FETCH FIRST :limite ROWS ONLY
                """,
                {"nome": nome_objeto, "limite": int(limite)},
            )

        return [
            {
                "owner": row[0],
                "object_name": row[1],
                "column_name": row[2],
                "data_type": row[3],
                "data_length": row[4],
                "data_precision": row[5],
                "data_scale": row[6],
                "nullable": row[7],
            }
            for row in cursor.fetchall()
        ]
    finally:
        conexao.close()
