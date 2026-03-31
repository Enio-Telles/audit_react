"""Gerador da tabela `produtos_final`."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.pipeline_fiscal import criar_dataframe_vazio_contrato, escrever_dataframe_ao_contrato

logger = logging.getLogger(__name__)


@registrar_gerador("produtos_final")
def gerar_produtos_final(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Junta agrupamentos e fatores preservando o contrato publico de produtos finais."""
    arquivo_agrupados = diretorio_parquets / "produtos_agrupados.parquet"
    arquivo_fatores = diretorio_parquets / "fatores_conversao.parquet"

    if not arquivo_agrupados.exists():
        raise FileNotFoundError("produtos_agrupados.parquet nao encontrado")
    if not arquivo_fatores.exists():
        raise FileNotFoundError("fatores_conversao.parquet nao encontrado")

    df_agrupados = pl.read_parquet(arquivo_agrupados)
    df_fatores = pl.read_parquet(arquivo_fatores)

    if df_agrupados.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_produtos_final = (
        df_agrupados
        .join(
            df_fatores.select(
                [
                    "id_agrupado",
                    "unid_ref",
                    "fator_compra_ref",
                    "fator_venda_ref",
                    pl.col("status").alias("status_conversao"),
                ]
            ),
            on="id_agrupado",
            how="left",
        )
        .with_columns(pl.col("status").alias("status_agregacao"))
        .drop("status")
    )

    total_registros = escrever_dataframe_ao_contrato(df_produtos_final, arquivo_saida, contrato)
    logger.info("produtos_final: %s registros gerados", total_registros)
    return total_registros
