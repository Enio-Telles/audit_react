"""Gerador da tabela `id_agrupados`."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.pipeline_fiscal import criar_dataframe_vazio_contrato, escrever_dataframe_ao_contrato

logger = logging.getLogger(__name__)


def extrair_relacao_id_agrupado(df_agrupados: pl.DataFrame) -> pl.DataFrame:
    """Extrai a relacao de cada id_produto para seu id_agrupado e descricao padrao."""
    # ⚡ BOLT: Otimizacao de performance.
    # Usa pl.List(pl.String) e json_decode nativo que utiliza processamento
    # vetorizado em Rust para parser JSON ao inves de iteracoes Regex lentas
    return df_agrupados.select(
        pl.col("id_agrupado"),
        pl.col("descricao_padrao"),
        pl.col("ids_membros")
          .str.json_decode(pl.List(pl.String))
          .alias("id_produto")
    ).explode("id_produto").with_columns(
        # Castar para inteiro ignorando eventuais espacos
        pl.col("id_produto").str.strip_chars().cast(pl.Int64, strict=False)
    ).drop_nulls("id_produto")


def cruzar_agrupados_com_produtos(df_ponte: pl.DataFrame, df_produtos: pl.DataFrame) -> pl.DataFrame:
    """Cruza a relacao inicial com a tabela de produtos para trazer a descricao original."""
    # Join para enriquecer a ponte com a descricao que o item tinha isoladamente
    return df_ponte.join(
        df_produtos.select(
            pl.col("id_produto").cast(pl.Int64, strict=False),
            pl.col("descricao").alias("descricao_original")
        ),
        on="id_produto",
        how="inner"
    ).select(
        "id_produto",
        "id_agrupado",
        "descricao_original",
        "descricao_padrao"
    ).sort("id_produto")


@registrar_gerador("id_agrupados")
def gerar_id_agrupados(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Gera a ponte rastreavel entre produto publico (id_produto) e grupo mestre (id_agrupado)."""
    arquivo_agrupados = diretorio_parquets / "produtos_agrupados.parquet"
    arquivo_produtos = diretorio_parquets / "produtos.parquet"

    if not arquivo_agrupados.exists() or not arquivo_produtos.exists():
        logger.warning("Dependencias id_agrupados ausentes.")
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_agrupados = pl.read_parquet(arquivo_agrupados)
    df_produtos = pl.read_parquet(arquivo_produtos)

    if df_agrupados.is_empty() or df_produtos.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    # 1. Transformar coluna consolidada em linhas de mapa De/Para
    df_ponte = extrair_relacao_id_agrupado(df_agrupados)

    # 2. Enriquecer com a descricao fiscal original garantindo a integridade
    df_final = cruzar_agrupados_com_produtos(df_ponte, df_produtos)

    if df_final.is_empty():
        df_final = criar_dataframe_vazio_contrato(contrato)

    total_registros = escrever_dataframe_ao_contrato(df_final, arquivo_saida, contrato)
    logger.info("id_agrupados: %s registros gerados", total_registros)
    return total_registros
