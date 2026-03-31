"""Gerador da tabela `nfe_entrada`."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.pipeline_fiscal import (
    criar_dataframe_vazio_contrato,
    escrever_dataframe_ao_contrato,
    mapear_fontes_para_grupos,
)
from ...utils.referencias import (
    enriquecer_com_cfop,
    validar_coluna_cfop,
)

logger = logging.getLogger(__name__)


@registrar_gerador("nfe_entrada")
def gerar_nfe_entrada(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Gera entradas padronizadas ja convertidas para a unidade de referencia do grupo."""
    caminho_fontes = diretorio_cnpj / "silver" / "fontes_produtos.parquet"
    arquivo_produtos = diretorio_parquets / "produtos.parquet"
    arquivo_id_agrupados = diretorio_parquets / "id_agrupados.parquet"
    arquivo_produtos_final = diretorio_parquets / "produtos_final.parquet"
    arquivo_produtos_agrupados = diretorio_parquets / "produtos_agrupados.parquet"

    if not caminho_fontes.exists():
        logger.warning("silver/fontes_produtos.parquet nao encontrado para %s", diretorio_cnpj.name)
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_fontes = pl.read_parquet(caminho_fontes)
    df_produtos = pl.read_parquet(arquivo_produtos)
    df_id_agrupados = pl.read_parquet(arquivo_id_agrupados)
    df_produtos_final = pl.read_parquet(arquivo_produtos_final)
    df_produtos_agrupados = pl.read_parquet(arquivo_produtos_agrupados)

    df_entradas = df_fontes.filter(pl.col("tipo_movimento") == "entrada")
    if df_entradas.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    # Mapeia cada linha documental para o grupo mestre usando os mesmos atributos publicos do catalogo de produtos.
    df_mapeado = mapear_fontes_para_grupos(df_entradas, df_produtos, df_id_agrupados).filter(pl.col("id_agrupado").is_not_null())
    if df_mapeado.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_entradas_enriquecidas = (
        df_mapeado
        .join(
            df_produtos_agrupados.select(["id_agrupado", "unid_compra", "unid_venda"]),
            on="id_agrupado",
            how="left",
        )
        .join(
            df_produtos_final.select(["id_agrupado", "unid_ref", "fator_compra_ref", "fator_venda_ref"]),
            on="id_agrupado",
            how="left",
        )
        .with_columns(
            [
                pl.when(pl.col("unidade") == pl.col("unid_ref"))
                .then(pl.col("quantidade"))
                .when(pl.col("unidade") == pl.col("unid_compra"))
                .then(pl.col("quantidade") * pl.col("fator_compra_ref").fill_null(1.0))
                .when(pl.col("unidade") == pl.col("unid_venda"))
                .then(pl.col("quantidade") * pl.col("fator_venda_ref").fill_null(1.0))
                .otherwise(pl.col("quantidade"))
                .alias("qtd_ref"),
            ]
        )
        .select(
            [
                pl.col("chave_documento").alias("chave_nfe"),
                "id_agrupado",
                pl.col("data_documento").alias("data_emissao"),
                "cfop",
                pl.col("quantidade"),
                pl.col("unidade").alias("unidade"),
                "qtd_ref",
                "valor_unitario",
                pl.col("valor_total"),
                pl.col("cnpj_emitente"),
            ]
        )
        .sort(["id_agrupado", "data_emissao", "chave_nfe"])
    )

    # Enriquecer com descrição do CFOP
    df_entradas_enriquecidas = enriquecer_com_cfop(df_entradas_enriquecidas, "cfop")

    # Validar CFOP das entradas
    df_validado = validar_coluna_cfop(df_entradas_enriquecidas, "cfop")
    cfop_invalidos = int((~df_validado["cfop_valido"]).sum())
    if cfop_invalidos > 0:
        logger.warning(
            "nfe_entrada: %d CFOP(s) invalidos encontrados",
            cfop_invalidos
        )

    total_registros = escrever_dataframe_ao_contrato(df_entradas_enriquecidas, arquivo_saida, contrato)
    logger.info("nfe_entrada: %s registros gerados", total_registros)
    return total_registros
