"""Gerador da tabela `ajustes_e111`."""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.pipeline_fiscal import criar_dataframe_vazio_contrato, escrever_dataframe_ao_contrato

logger = logging.getLogger(__name__)


def _carregar_ajustes_silver(diretorio_cnpj: Path) -> pl.DataFrame:
    """Carrega a camada silver de ajustes E111 quando ela existir."""
    caminho_ajustes = diretorio_cnpj / "silver" / "e111_ajustes.parquet"
    if not caminho_ajustes.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho_ajustes)


def _enriquecer_com_ano_fiscal(df_ajustes: pl.DataFrame) -> pl.DataFrame:
    """Deriva o ano fiscal da competencia para facilitar consultas anuais."""
    if df_ajustes.is_empty():
        return df_ajustes

    return (
        df_ajustes
        .with_columns(
            # O ano fiscal deriva diretamente da competencia declarada no EFD.
            pl.col("periodo_efd")
            .cast(pl.String, strict=False)
            .str.slice(0, 4)
            .alias("ano")
        )
        .select(
            [
                "periodo_efd",
                "ano",
                "cnpj_referencia",
                "codigo_ajuste",
                "descricao_codigo_ajuste",
                "descricao_complementar",
                "valor_ajuste",
                "data_entrega_efd_periodo",
                "cod_fin_efd",
            ]
        )
    )


@registrar_gerador("ajustes_e111")
def gerar_ajustes_e111(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Materializa a trilha gold dos ajustes E111 preservando o valor original declarado."""
    df_ajustes = _carregar_ajustes_silver(diretorio_cnpj)
    if df_ajustes.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_final = _enriquecer_com_ano_fiscal(df_ajustes)
    total_registros = escrever_dataframe_ao_contrato(df_final, arquivo_saida, contrato)
    logger.info("ajustes_e111: %s registros gerados", total_registros)
    return total_registros
