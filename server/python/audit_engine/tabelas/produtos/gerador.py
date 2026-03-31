"""Gerador da tabela `produtos`."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.pipeline_fiscal import (
    criar_dataframe_vazio_contrato,
    escrever_dataframe_ao_contrato,
)

logger = logging.getLogger(__name__)


@registrar_gerador("produtos")
def gerar_produtos(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Consolida a camada de produtos publicos a partir de `produtos_unidades`."""
    arquivo_entrada = diretorio_parquets / "produtos_unidades.parquet"
    if not arquivo_entrada.exists():
        raise FileNotFoundError("produtos_unidades.parquet nao encontrado")

    df_entrada = pl.read_parquet(arquivo_entrada)
    if df_entrada.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    # Determina a unidade principal preservando a unidade de venda quando existir.
    df_produtos = (
        df_entrada
        .with_columns(
            [
                pl.when(pl.col("unid_venda").is_not_null() & (pl.col("unid_venda") != ""))
                .then(pl.col("unid_venda"))
                .otherwise(pl.col("unid_compra"))
                .alias("unidade_principal"),
                (pl.col("qtd_nfe_compra") + pl.col("qtd_nfe_venda")).alias("qtd_total_nfe"),
                (pl.col("valor_total_compra") + pl.col("valor_total_venda")).alias("valor_total"),
                pl.when((pl.col("qtd_nfe_compra") > 0) & (pl.col("qtd_nfe_venda") > 0))
                .then(pl.lit("ambos"))
                .when(pl.col("qtd_nfe_compra") > 0)
                .then(pl.lit("compra"))
                .otherwise(pl.lit("venda"))
                .alias("tipo"),
            ]
        )
        .select(
            [
                "id_produto",
                "descricao",
                "ncm",
                "cest",
                "unidade_principal",
                "qtd_total_nfe",
                "valor_total",
                "tipo",
            ]
        )
    )

    total_registros = escrever_dataframe_ao_contrato(df_produtos, arquivo_saida, contrato)
    logger.info("produtos: %s registros gerados", total_registros)
    return total_registros
