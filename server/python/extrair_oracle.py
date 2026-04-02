"""Extracao Oracle para Parquet por CNPJ."""

from __future__ import annotations

import logging
import os
import re
import sys
import time
import json
from pathlib import Path
from typing import Any

import oracledb
import polars as pl

from configuracao_backend import obter_configuracao_backend
from oracle_client import criar_conexao_oracle

logger = logging.getLogger(__name__)

MAPEAMENTO_CONSULTAS = {
    "nfe": "nfe.sql",
    "nfce": "nfce.sql",
    "nfe_dados_st": "nfe_dados_st.sql",
    "c100": "c100.sql",
    "c170": "c170.sql",
    "c176": "c176.sql",
    "bloco_h": "bloco_h.sql",
    "e111": "e111.sql",
    "reg0000": "reg0000.sql",
    "reg0005": "reg0005.sql",
    "reg0190": "reg0190.sql",
    "reg0200": "reg0200.sql",
    "reg0220": "reg0220.sql",
}

CONSULTAS_ESSENCIAIS = [
    "reg0000",
    "reg0005",
    "reg0190",
    "c100",
    "c170",
    "c176",
    "nfe",
    "nfce",
    "bloco_h",
    "reg0200",
    "reg0220",
    "dados_cadastrais",
]

# Placeholders aceitos nos SQLs versionados.
# Mantidos apenas: C170, Bloco H, NFe, NFCe, REG0000, REG0200, REG0220 (essenciais para conversao, agregacao e estoque)
FONTES_ORACLE_PADRAO = {
    "FONTE_NFE": "BI.FATO_NFE_DETALHE",
    "FONTE_NFCE": "BI.FATO_NFCE_DETALHE",
    "FONTE_C170": "SPED.REG_C170",
    "FONTE_C176": "SPED.REG_C176",
    "FONTE_REG0000": "SPED.REG_0000",
    "FONTE_REG0200": "SPED.REG_0200",
    "FONTE_REG0220": "SPED.REG_0220",
    "FONTE_BLOCO_H_CAB": "SPED.REG_H005",
    "FONTE_BLOCO_H_ITEM": "SPED.REG_H010",
    "FONTE_BLOCO_H_TRIB": "SPED.REG_H020",
}


def _obter_arquivo_mapeamento_fontes() -> Path:
    """Resolve arquivo persistido com overrides de fontes Oracle."""
    cfg = obter_configuracao_backend()
    return Path(cfg.diretorio_base_cnpj) / "_sistema" / "fontes_oracle.json"


def _normalizar_cnpj(cnpj_input: str) -> str:
    """Normaliza CNPJ removendo caracteres nao numericos."""
    cnpj = re.sub(r"\D", "", cnpj_input)
    if len(cnpj) != 14:
        raise ValueError(f"CNPJ invalido: {cnpj_input}")
    return cnpj


def _obter_diretorio_consultas(diretorio_consultas: str | None = None) -> Path:
    """Resolve diretorio de consultas SQL versionadas no repositorio."""
    if diretorio_consultas:
        return Path(diretorio_consultas)

    cfg = obter_configuracao_backend()
    return Path(cfg.diretorio_consultas_sql)


def carregar_mapeamento_fontes_persistido() -> dict[str, str]:
    """Carrega overrides persistidos de placeholders Oracle."""
    arquivo_mapeamento = _obter_arquivo_mapeamento_fontes()
    if not arquivo_mapeamento.exists():
        return {}

    with arquivo_mapeamento.open("r", encoding="utf-8") as arquivo:
        conteudo = json.load(arquivo)

    return {
        str(chave).strip().upper(): str(valor).strip()
        for chave, valor in conteudo.items()
        if str(chave).strip() and str(valor).strip()
    }


def salvar_mapeamento_fontes_oracle(mapeamento: dict[str, str | None]) -> dict[str, str]:
    """Persiste overrides de placeholders Oracle e retorna estado final salvo."""
    arquivo_mapeamento = _obter_arquivo_mapeamento_fontes()
    atual = carregar_mapeamento_fontes_persistido()

    for chave, valor in mapeamento.items():
        chave_normalizada = str(chave).strip().upper()
        if chave_normalizada not in FONTES_ORACLE_PADRAO:
            raise ValueError(f"Placeholder Oracle desconhecido: {chave_normalizada}")

        valor_normalizado = str(valor).strip() if valor is not None else ""
        if valor_normalizado:
            atual[chave_normalizada] = valor_normalizado
        else:
            atual.pop(chave_normalizada, None)

    arquivo_mapeamento.parent.mkdir(parents=True, exist_ok=True)
    with arquivo_mapeamento.open("w", encoding="utf-8") as arquivo:
        json.dump(atual, arquivo, ensure_ascii=False, indent=2)

    return atual


def detalhar_mapeamento_fontes_oracle() -> list[dict[str, str]]:
    """Retorna metadados completos do mapeamento efetivo de fontes Oracle."""
    persistido = carregar_mapeamento_fontes_persistido()
    detalhes: list[dict[str, str]] = []

    for chave, fonte_padrao in FONTES_ORACLE_PADRAO.items():
        valor_env = os.getenv(f"ORACLE_{chave}")
        fonte_configurada = persistido.get(chave) or valor_env or fonte_padrao
        origem = "persistido" if chave in persistido else "env" if valor_env else "padrao"
        owner, objeto = separar_owner_objeto_oracle(fonte_configurada)
        detalhes.append(
            {
                "chave": chave,
                "env_var": f"ORACLE_{chave}",
                "fonte_padrao": fonte_padrao,
                "fonte_configurada": fonte_configurada,
                "origem": origem,
                "owner": owner or "",
                "objeto": objeto,
            }
        )

    return detalhes


def obter_mapeamento_fontes_oracle() -> dict[str, str]:
    """Retorna mapeamento efetivo de placeholders para objetos Oracle."""
    return {
        item["chave"]: item["fonte_configurada"]
        for item in detalhar_mapeamento_fontes_oracle()
    }


def separar_owner_objeto_oracle(nome_fonte: str) -> tuple[str | None, str]:
    """Separa owner e objeto quando fonte vier no formato OWNER.OBJETO."""
    valor = nome_fonte.strip().upper()
    if "." not in valor:
        return None, valor

    owner, objeto = valor.split(".", 1)
    return owner, objeto


def listar_consultas_versionadas(diretorio_consultas: str | None = None) -> list[str]:
    """Lista IDs de consultas disponiveis no diretorio configurado."""
    pasta_consultas = _obter_diretorio_consultas(diretorio_consultas)
    disponiveis: list[str] = []

    for consulta_id, arquivo_sql in MAPEAMENTO_CONSULTAS.items():
        if (pasta_consultas / arquivo_sql).exists():
            disponiveis.append(consulta_id)

    return sorted(disponiveis)


def _renderizar_sql_template(sql: str) -> str:
    """Substitui placeholders {{CHAVE}} por fontes Oracle configuradas."""
    fontes_oracle = obter_mapeamento_fontes_oracle()

    def substituir(match: re.Match[str]) -> str:
        chave = match.group(1).strip().upper()
        if chave not in fontes_oracle:
            raise ValueError(f"Placeholder SQL desconhecido: {chave}")
        return fontes_oracle[chave]

    return re.sub(r"\{\{\s*([A-Z0-9_]+)\s*\}\}", substituir, sql)


def _ler_sql(caminho_sql: Path) -> str:
    """Le arquivo SQL removendo comentarios de linha iniciados com --."""
    linhas: list[str] = []

    with caminho_sql.open("r", encoding="utf-8", errors="replace") as arquivo:
        for linha in arquivo:
            texto = linha.strip()
            if texto.startswith("--"):
                continue
            linhas.append(linha)

    sql_template = "".join(linhas).strip().rstrip(";")
    return _renderizar_sql_template(sql_template)


def _resolver_consultas_alvo(consultas_alvo: list[str] | None) -> list[str]:
    """Define lista final de consultas a executar."""
    if not consultas_alvo:
        return CONSULTAS_ESSENCIAIS.copy()

    consultas_unicas: list[str] = []
    for consulta in consultas_alvo:
        consulta_normalizada = consulta.strip().lower()
        if consulta_normalizada and consulta_normalizada not in consultas_unicas:
            consultas_unicas.append(consulta_normalizada)

    return consultas_unicas


def _montar_binds(cursor: oracledb.Cursor, cnpj: str, data_limite: str | None) -> dict[str, Any]:
    """Monta binds obrigatorios para execucao SQL."""
    binds: dict[str, Any] = {}
    bindnames = cursor.bindnames()

    tem_cnpj = False
    for nome_bind in bindnames:
        nome_upper = nome_bind.upper()
        if nome_upper == "CNPJ":
            binds[nome_bind] = cnpj
            tem_cnpj = True
        elif nome_upper == "DATA_LIMITE_PROCESSAMENTO":
            binds[nome_bind] = data_limite
        elif nome_upper in {"DATA_INICIAL", "DATA_INICIO"}:
            binds[nome_bind] = None
        elif nome_upper in {"DATA_FINAL", "DATA_FIM"}:
            binds[nome_bind] = data_limite

    if bindnames and not tem_cnpj:
        raise ValueError("Consulta sem bind :CNPJ. Ajuste o SQL para isolamento por contribuinte.")

    return binds


def _executar_consulta_sql(
    conexao: oracledb.Connection,
    caminho_sql: Path,
    cnpj: str,
    data_limite: str | None,
    diretorio_extraidos: Path,
) -> tuple[int, str]:
    """Executa consulta SQL e grava resultado parquet retornando (linhas, arquivo)."""
    sql = _ler_sql(caminho_sql)
    if not sql:
        raise ValueError(f"Arquivo SQL vazio: {caminho_sql.name}")

    cursor = conexao.cursor()
    cursor.arraysize = 5000

    try:
        cursor.prepare(sql)
        binds = _montar_binds(cursor, cnpj, data_limite)

        inicio = time.time()
        cursor.execute(None, binds)

        colunas = [descricao[0].lower() for descricao in (cursor.description or [])]
        dados = cursor.fetchall()

        duracao = time.time() - inicio
        nome_arquivo = caminho_sql.stem.lower() + ".parquet"
        caminho_saida = diretorio_extraidos / nome_arquivo

        if not dados:
            df_vazio = pl.DataFrame(schema={coluna: pl.String for coluna in colunas})
            df_vazio.write_parquet(caminho_saida, compression="zstd")
            logger.info("Consulta %s retornou 0 linhas (%.2fs)", caminho_sql.name, duracao)
            return 0, nome_arquivo

        dataframe = _montar_dataframe_resultado(colunas, dados)
        dataframe.write_parquet(caminho_saida, compression="zstd")

        logger.info(
            "Consulta %s: %s linhas em %.2fs -> %s",
            caminho_sql.name,
            len(dataframe),
            duracao,
            nome_arquivo,
        )
        return len(dataframe), nome_arquivo
    finally:
        cursor.close()


def _montar_dataframe_resultado(colunas: list[str], dados: list[tuple[Any, ...]]) -> pl.DataFrame:
    """Materializa resultado Oracle lendo todas as linhas para inferir colunas com nulos iniciais."""
    return pl.DataFrame(
        dados,
        schema=colunas,
        orient="row",
        infer_schema_length=None,
    )


def extrair_dados_cnpj(
    cnpj_input: str,
    consultas_alvo: list[str] | None = None,
    data_limite: str | None = None,
    diretorio_consultas: str | None = None,
    indice_oracle: int = 0,
) -> dict[str, Any]:
    """Extrai dados Oracle para parquet no diretorio extraidos do CNPJ."""
    cnpj = _normalizar_cnpj(cnpj_input)
    cfg = obter_configuracao_backend()

    pasta_cnpj = Path(cfg.diretorio_base_cnpj) / cnpj
    pasta_extraidos = pasta_cnpj / "extraidos"
    (pasta_cnpj / "parquets").mkdir(parents=True, exist_ok=True)
    (pasta_cnpj / "edicoes").mkdir(parents=True, exist_ok=True)
    (pasta_cnpj / "exportacoes").mkdir(parents=True, exist_ok=True)
    pasta_extraidos.mkdir(parents=True, exist_ok=True)

    pasta_consultas = _obter_diretorio_consultas(diretorio_consultas)
    consultas_resolvidas = _resolver_consultas_alvo(consultas_alvo)

    resultados: dict[str, Any] = {
        "cnpj": cnpj,
        "diretorio_extraidos": str(pasta_extraidos),
        "consultas": {},
        "erros": [],
        "status": "ok",
        "indice_oracle": indice_oracle,
        "fontes_oracle": obter_mapeamento_fontes_oracle(),
    }

    if not consultas_resolvidas:
        return resultados

    logger.info("Iniciando extracao Oracle para CNPJ %s", cnpj)
    logger.info("Consultas alvo: %s", consultas_resolvidas)
    logger.info("Conexao Oracle ativa: indice=%s", indice_oracle)

    conexao = criar_conexao_oracle(indice=indice_oracle)
    try:
        for consulta_id in consultas_resolvidas:
            arquivo_sql = MAPEAMENTO_CONSULTAS.get(consulta_id)
            if not arquivo_sql:
                erro = f"Consulta desconhecida: {consulta_id}"
                resultados["erros"].append(erro)
                resultados["consultas"][consulta_id] = {
                    "status": "erro",
                    "mensagem": erro,
                    "linhas": 0,
                }
                continue

            caminho_sql = pasta_consultas / arquivo_sql
            if not caminho_sql.exists():
                erro = f"Arquivo SQL nao encontrado: {caminho_sql}"
                resultados["erros"].append(erro)
                resultados["consultas"][consulta_id] = {
                    "status": "erro",
                    "mensagem": erro,
                    "linhas": 0,
                }
                continue

            try:
                linhas, arquivo_saida = _executar_consulta_sql(
                    conexao=conexao,
                    caminho_sql=caminho_sql,
                    cnpj=cnpj,
                    data_limite=data_limite,
                    diretorio_extraidos=pasta_extraidos,
                )
                resultados["consultas"][consulta_id] = {
                    "status": "ok",
                    "linhas": linhas,
                    "arquivo": arquivo_saida,
                }
            except Exception as erro:  # noqa: BLE001
                mensagem = f"Falha na consulta {consulta_id}: {erro}"
                logger.exception(mensagem)
                resultados["erros"].append(mensagem)
                resultados["consultas"][consulta_id] = {
                    "status": "erro",
                    "mensagem": str(erro),
                    "linhas": 0,
                }
    finally:
        conexao.close()

    total_linhas = sum(
        item.get("linhas", 0)
        for item in resultados["consultas"].values()
        if isinstance(item, dict)
    )
    resultados["total_linhas"] = total_linhas

    if resultados["erros"]:
        resultados["status"] = "concluido_com_erros"

    return resultados


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    if len(sys.argv) < 2:
        print("Uso: python extrair_oracle.py <CNPJ> [consulta1,consulta2,...] [DATA_LIMITE]")
        sys.exit(1)

    cnpj_arg = sys.argv[1]
    consultas_arg = sys.argv[2].split(",") if len(sys.argv) > 2 and sys.argv[2] else None
    data_limite_arg = sys.argv[3] if len(sys.argv) > 3 else None

    resposta = extrair_dados_cnpj(cnpj_arg, consultas_arg, data_limite_arg)
    print(resposta)
