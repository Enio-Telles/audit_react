"""Gerador da tabela `produtos_unidades`."""

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
from ...utils.referencias import (
    enriquecer_com_ncm,
    enriquecer_com_cest,
    validar_integridade_fiscal,
)

logger = logging.getLogger(__name__)


def _selecionar_unidade_predominante(df_item_unidades: pl.DataFrame, coluna_quantidade: str) -> pl.DataFrame:
    """Seleciona a unidade predominante por produto conforme a maior movimentacao."""
    df_filtrado = df_item_unidades.filter(pl.col(coluna_quantidade) > 0)
    if df_filtrado.is_empty():
        return pl.DataFrame(schema={"codigo_fonte": pl.String, "unidade_predominante": pl.String})

    return (
        df_filtrado
        .sort(["codigo_fonte", coluna_quantidade], descending=[False, True])
        .group_by("codigo_fonte")
        .agg(pl.col("unidade").first().alias("unidade_predominante"))
    )


@registrar_gerador("produtos_unidades")
def gerar_produtos_unidades(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Gera produtos consolidados por origem fiscal e unidades predominantes."""
    caminho_item_unidades = diretorio_cnpj / "silver" / "item_unidades.parquet"

    if not caminho_item_unidades.exists():
        logger.warning("silver/item_unidades.parquet nao encontrado para %s", diretorio_cnpj.name)
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_item_unidades = pl.read_parquet(caminho_item_unidades)
    if df_item_unidades.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    # Identifica a unidade predominante das compras para preservar a referencia operacional de entrada.
    df_unidade_compra = _selecionar_unidade_predominante(df_item_unidades, "qtd_compras").rename(
        {"unidade_predominante": "unid_compra"}
    )
    # Identifica a unidade predominante das vendas para preservar a referencia operacional de saida.
    df_unidade_venda = _selecionar_unidade_predominante(df_item_unidades, "qtd_vendas").rename(
        {"unidade_predominante": "unid_venda"}
    )

    # Consolida cada codigo_fonte em um unico produto auditavel mantendo descricao, classificacao e totais.
    df_produtos = (
        df_item_unidades
        .group_by("codigo_fonte")
        .agg(
            [
                pl.col("descricao").drop_nulls().first().alias("descricao"),
                pl.col("ncm").drop_nulls().first().alias("ncm"),
                pl.col("cest").drop_nulls().first().alias("cest"),
                pl.col("gtin").drop_nulls().first().alias("gtin"),
                pl.col("qtd_compras").sum().alias("__qtd_compras_total__"),
                pl.col("qtd_vendas").sum().alias("__qtd_vendas_total__"),
                pl.col("compras").sum().alias("valor_total_compra"),
                pl.col("vendas").sum().alias("valor_total_venda"),
            ]
        )
        .join(df_unidade_compra, on="codigo_fonte", how="left")
        .join(df_unidade_venda, on="codigo_fonte", how="left")
        .sort("codigo_fonte")
        .with_row_count("id_produto", offset=1)
        .with_columns(
            [
                pl.col("__qtd_compras_total__").round(0).cast(pl.Int64, strict=False).alias("qtd_nfe_compra"),
                pl.col("__qtd_vendas_total__").round(0).cast(pl.Int64, strict=False).alias("qtd_nfe_venda"),
                (pl.col("__qtd_compras_total__") + pl.col("__qtd_vendas_total__")).round(0).cast(pl.Int64, strict=False).alias("qtd_efd"),
            ]
        )
        .drop(["codigo_fonte", "__qtd_compras_total__", "__qtd_vendas_total__"])
    )

    # Enriquecer com descrições de NCM e CEST das tabelas de referência
    df_produtos = enriquecer_com_ncm(df_produtos, "ncm")
    df_produtos = enriquecer_com_cest(df_produtos, "cest")

    # Validar integridade fiscal dos dados
    validacao = validar_integridade_fiscal(df_produtos)
    if validacao.get("ncm_invalidos", 0) > 0:
        logger.warning(
            "produtos_unidades: %d NCM(s) invalidos encontrados",
            validacao.get("ncm_invalidos", 0)
        )
    if validacao.get("cest_invalidos", 0) > 0:
        logger.warning(
            "produtos_unidades: %d CEST(s) invalidos encontrados",
            validacao.get("cest_invalidos", 0)
        )

    total_registros = escrever_dataframe_ao_contrato(df_produtos, arquivo_saida, contrato)
    logger.info("produtos_unidades: %s registros gerados", total_registros)
    return total_registros
