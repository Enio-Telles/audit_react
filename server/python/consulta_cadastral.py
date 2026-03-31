"""Consulta cadastral Oracle por documento fiscal."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import oracledb

from oracle_client import criar_conexao_oracle


CAMINHO_SQL_DADOS_CADASTRAIS = Path(__file__).resolve().parent / "consultas" / "dados_cadastrais.sql"


def normalizar_documento_consulta(documento: str) -> str:
    """Normaliza CPF ou CNPJ removendo caracteres nao numericos."""
    documento_limpo = "".join(caractere for caractere in str(documento) if caractere.isdigit())
    if len(documento_limpo) not in {11, 14}:
        raise ValueError("Documento deve conter 11 digitos de CPF ou 14 digitos de CNPJ")
    return documento_limpo


def classificar_tipo_documento(documento: str) -> str:
    """Classifica documento normalizado como CPF, CNPJ ou desconhecido."""
    if len(documento) == 11:
        return "cpf"
    if len(documento) == 14:
        return "cnpj"
    return "desconhecido"


def carregar_sql_dados_cadastrais() -> str:
    """Carrega SQL cadastral removendo comentarios de linha."""
    linhas_validas: list[str] = []

    with CAMINHO_SQL_DADOS_CADASTRAIS.open("r", encoding="utf-8", errors="replace") as arquivo_sql:
        for linha in arquivo_sql:
            if linha.strip().startswith("--"):
                continue
            linhas_validas.append(linha)

    return "".join(linhas_validas).strip().rstrip(";")


def _normalizar_valor_cadastral(valor: Any) -> Any:
    """Normaliza valores Oracle para serializacao JSON previsivel."""
    if isinstance(valor, datetime):
        return valor.date().isoformat()
    if isinstance(valor, date):
        return valor.isoformat()
    return valor


def _executar_consulta_cadastral(cursor: oracledb.Cursor, documento: str) -> list[dict[str, Any]]:
    """Executa a SQL cadastral para um documento e devolve registros serializaveis."""
    sql = carregar_sql_dados_cadastrais()
    cursor.prepare(sql)

    nomes_binds = {nome.upper() for nome in cursor.bindnames()}
    if "CO_CNPJ_CPF" not in nomes_binds:
        raise ValueError("SQL cadastral sem bind :CO_CNPJ_CPF")

    # Consultar um documento por vez preserva rastreabilidade do resultado no lote.
    cursor.execute(None, {"CO_CNPJ_CPF": documento})

    colunas = [descricao[0].lower() for descricao in (cursor.description or [])]
    registros: list[dict[str, Any]] = []

    for linha in cursor.fetchall():
        registro = {
            coluna: _normalizar_valor_cadastral(valor)
            for coluna, valor in zip(colunas, linha, strict=False)
        }
        registros.append(registro)

    return registros


def consultar_dados_cadastrais_documentos(
    documentos: list[str],
    indice_oracle: int = 0,
) -> list[dict[str, Any]]:
    """Consulta dados cadastrais no Oracle para uma lista de documentos."""
    documentos_normalizados = list(dict.fromkeys(normalizar_documento_consulta(item) for item in documentos))
    conexao = criar_conexao_oracle(indice=indice_oracle)

    try:
        cursor = conexao.cursor()
        resultados: list[dict[str, Any]] = []

        try:
            for documento in documentos_normalizados:
                registros = _executar_consulta_cadastral(cursor, documento)
                resultados.append(
                    {
                        "status": "ok",
                        "tipo_documento": classificar_tipo_documento(documento),
                        "documento_consultado": documento,
                        "origem": "oracle",
                        "encontrado": len(registros) > 0,
                        "mensagem": None if registros else "Nenhum dado cadastral encontrado para o documento informado",
                        "registros": registros,
                    }
                )
        finally:
            cursor.close()

        return resultados
    finally:
        conexao.close()
