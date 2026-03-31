"""Gerador da tabela `st_itens`."""

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
    carregar_ncm,
    carregar_cest,
    validar_integridade_fiscal,
)

logger = logging.getLogger(__name__)


def _carregar_parquet_se_existir(caminho: Path) -> pl.DataFrame:
    """Lê um parquet somente quando ele existir em disco."""
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def _padronizar_base_c176(df_c176: pl.DataFrame) -> pl.DataFrame:
    """Padroniza a trilha C176 para o contrato comum de conciliacao ST."""
    if df_c176.is_empty():
        return pl.DataFrame()

    return (
        df_c176
        .select(
            [
                "id_linha_origem",
                "chave_documento",
                "item_documento",
                "cnpj_referencia",
                "data_documento",
                "codigo_fonte",
                "codigo_produto",
                "descricao",
                pl.lit(None, dtype=pl.String).alias("ncm"),
                pl.lit(None, dtype=pl.String).alias("cest"),
                "cfop",
                pl.lit(None, dtype=pl.String).alias("cst"),
                pl.lit(None, dtype=pl.String).alias("csosn"),
                pl.col("quantidade").cast(pl.Float64, strict=False).alias("quantidade"),
                pl.col("valor_total").cast(pl.Float64, strict=False).alias("valor_total"),
                pl.lit(0.0).alias("bc_st_xml"),
                pl.lit(0.0).alias("vl_st_xml"),
                pl.lit(0.0).alias("vl_icms_substituto"),
                pl.lit(0.0).alias("vl_st_retido"),
                pl.lit(0.0).alias("bc_fcp_st"),
                pl.lit(0.0).alias("p_fcp_st"),
                pl.lit(0.0).alias("vl_fcp_st"),
                pl.col("vl_ressarc_credito_proprio").cast(pl.Float64, strict=False).alias("vl_ressarc_credito_proprio"),
                pl.col("vl_ressarc_st_retido").cast(pl.Float64, strict=False).alias("vl_ressarc_st_retido"),
                pl.col("vr_total_ressarcimento").cast(pl.Float64, strict=False).alias("vl_total_ressarcimento"),
                pl.lit(1).alias("__tem_c176__"),
                pl.lit(0).alias("__tem_xml__"),
            ]
        )
    )


def _padronizar_base_xml(df_st_xml: pl.DataFrame) -> pl.DataFrame:
    """Padroniza a base XML de ST/FCP para o mesmo contrato de conciliacao."""
    if df_st_xml.is_empty():
        return pl.DataFrame()

    return (
        df_st_xml
        .select(
            [
                "id_linha_origem",
                "chave_documento",
                "item_documento",
                "cnpj_referencia",
                "data_documento",
                "codigo_fonte",
                "codigo_produto",
                "descricao",
                "ncm",
                "cest",
                "cfop",
                "cst",
                "csosn",
                pl.col("quantidade").cast(pl.Float64, strict=False).alias("quantidade"),
                pl.col("valor_total").cast(pl.Float64, strict=False).alias("valor_total"),
                pl.col("bc_st").cast(pl.Float64, strict=False).alias("bc_st_xml"),
                pl.col("vl_st").cast(pl.Float64, strict=False).alias("vl_st_xml"),
                pl.col("vl_icms_substituto").cast(pl.Float64, strict=False).alias("vl_icms_substituto"),
                pl.col("vl_st_retido").cast(pl.Float64, strict=False).alias("vl_st_retido"),
                pl.col("bc_fcp_st").cast(pl.Float64, strict=False).alias("bc_fcp_st"),
                pl.col("p_fcp_st").cast(pl.Float64, strict=False).alias("p_fcp_st"),
                pl.col("vl_fcp_st").cast(pl.Float64, strict=False).alias("vl_fcp_st"),
                pl.lit(0.0).alias("vl_ressarc_credito_proprio"),
                pl.lit(0.0).alias("vl_ressarc_st_retido"),
                pl.lit(0.0).alias("vl_total_ressarcimento"),
                pl.lit(0).alias("__tem_c176__"),
                pl.lit(1).alias("__tem_xml__"),
            ]
        )
    )


def _consolidar_fontes_st(df_c176: pl.DataFrame, df_st_xml: pl.DataFrame) -> pl.DataFrame:
    """Consolida C176 e XML de ST por linha documental preservando a melhor informacao de cada lado."""
    bases: list[pl.DataFrame] = []

    if not df_c176.is_empty():
        bases.append(_padronizar_base_c176(df_c176))
    if not df_st_xml.is_empty():
        bases.append(_padronizar_base_xml(df_st_xml))

    if not bases:
        return pl.DataFrame()

    df_consolidado = (
        pl.concat(bases, how="vertical_relaxed")
        .group_by(["id_linha_origem", "chave_documento", "item_documento"], maintain_order=True)
        .agg(
            [
                pl.col("cnpj_referencia").drop_nulls().first().alias("cnpj_referencia"),
                pl.col("data_documento").drop_nulls().first().alias("data_documento"),
                pl.col("codigo_fonte").drop_nulls().first().alias("codigo_fonte"),
                pl.col("codigo_produto").drop_nulls().first().alias("codigo_produto"),
                pl.col("descricao").drop_nulls().first().alias("descricao"),
                pl.col("ncm").drop_nulls().first().alias("ncm"),
                pl.col("cest").drop_nulls().first().alias("cest"),
                pl.col("cfop").drop_nulls().first().alias("cfop"),
                pl.col("cst").drop_nulls().first().alias("cst"),
                pl.col("csosn").drop_nulls().first().alias("csosn"),
                pl.col("quantidade").max().alias("quantidade"),
                pl.col("valor_total").max().alias("valor_total"),
                pl.col("bc_st_xml").max().alias("bc_st_xml"),
                pl.col("vl_st_xml").max().alias("vl_st_xml"),
                pl.col("vl_icms_substituto").max().alias("vl_icms_substituto"),
                pl.col("vl_st_retido").max().alias("vl_st_retido"),
                pl.col("bc_fcp_st").max().alias("bc_fcp_st"),
                pl.col("p_fcp_st").max().alias("p_fcp_st"),
                pl.col("vl_fcp_st").max().alias("vl_fcp_st"),
                pl.col("vl_ressarc_credito_proprio").max().alias("vl_ressarc_credito_proprio"),
                pl.col("vl_ressarc_st_retido").max().alias("vl_ressarc_st_retido"),
                pl.col("vl_total_ressarcimento").max().alias("vl_total_ressarcimento"),
                pl.col("__tem_c176__").max().alias("__tem_c176__"),
                pl.col("__tem_xml__").max().alias("__tem_xml__"),
            ]
        )
        .with_columns(
            [
                # Classifica o status da linha conforme a presenca de C176 e XML de ST.
                pl.when((pl.col("__tem_c176__") == 1) & (pl.col("__tem_xml__") == 1))
                .then(pl.lit("conciliado"))
                .when(pl.col("__tem_c176__") == 1)
                .then(pl.lit("somente_c176"))
                .otherwise(pl.lit("somente_xml"))
                .alias("status_conciliacao"),
                # A origem predominante resume qual base efetivamente sustentou a linha de ST.
                pl.when((pl.col("__tem_c176__") == 1) & (pl.col("__tem_xml__") == 1))
                .then(pl.lit("c176+xml"))
                .when(pl.col("__tem_c176__") == 1)
                .then(pl.lit("c176"))
                .otherwise(pl.lit("xml"))
                .alias("origem_st"),
            ]
        )
        .drop(["__tem_c176__", "__tem_xml__"])
    )

    return df_consolidado


def _conciliar_com_matriz_sefin(df_st: pl.DataFrame, diretorio_referencias: Path) -> pl.DataFrame:
    """Concilia NCM/CEST da base ST com a matriz SEFIN de produtos.

    Args:
        df_st: DataFrame consolidado de ST.
        diretorio_referencias: Caminho para diretório de tabelas de referência.

    Returns:
        DataFrame com colunas adicionais de conciliação SEFIN.
    """
    if df_st.is_empty():
        return df_st

    # Carregar matriz SEFIN
    caminho_matriz = diretorio_referencias / "CO_SEFIN" / "sitafe_produto_sefin_aux.parquet"
    if not caminho_matriz.exists():
        # Sem matriz, retorna com indicadores nulos
        return df_st.with_columns(
            pl.lit(None, dtype=pl.Boolean).alias("ncm_na_matriz"),
            pl.lit(None, dtype=pl.Boolean).alias("cest_na_matriz"),
            pl.lit(None, dtype=pl.String).alias("descricao_matriz"),
        )

    df_matriz = pl.read_parquet(caminho_matriz)
    if df_matriz.is_empty():
        return df_st.with_columns(
            pl.lit(None, dtype=pl.Boolean).alias("ncm_na_matriz"),
            pl.lit(None, dtype=pl.Boolean).alias("cest_na_matriz"),
            pl.lit(None, dtype=pl.String).alias("descricao_matriz"),
        )

    # Normalizar colunas da matriz para join
    # Colunas esperadas: NCM, CEST, DESCRICAO
    df_matriz_norm = df_matriz.select([
        pl.col("NCM").cast(pl.String, strict=False).alias("ncm_join") if "NCM" in df_matriz.columns else pl.lit(None).alias("ncm_join"),
        pl.col("CEST").cast(pl.String, strict=False).alias("cest_join") if "CEST" in df_matriz.columns else pl.lit(None).alias("cest_join"),
        pl.col("DESCRICAO").cast(pl.String, strict=False).alias("descricao_matriz") if "DESCRICAO" in df_matriz.columns else pl.lit(None).alias("descricao_matriz"),
    ]).unique()

    # Cruzar com base ST por NCM e CEST
    df_conciliado = (
        df_st
        .join(
            df_matriz_norm,
            left_on=["ncm", "cest"],
            right_on=["ncm_join", "cest_join"],
            how="left",
        )
        .drop(["ncm_join", "cest_join"])
        .with_columns(
            [
                # Verifica se NCM+CEST foram encontrados na matriz
                pl.col("descricao_matriz").is_not_null().alias("ncm_na_matriz"),
                pl.when(pl.col("descricao_matriz").is_not_null())
                .then(True)
                .otherwise(False)
                .alias("cest_na_matriz"),
            ]
        )
    )

    return df_conciliado


def _enriquecer_com_id_agrupado(
    df_st: pl.DataFrame,
    df_produtos: pl.DataFrame,
    df_id_agrupados: pl.DataFrame,
    df_produtos_final: pl.DataFrame,
) -> pl.DataFrame:
    """Mapeia as linhas de ST para o produto agrupado sem alterar o contrato do core."""
    if df_st.is_empty():
        return df_st

    # O mapeamento reutiliza a mesma heuristica do core para vincular documento ao produto mestre.
    df_mapeado = mapear_fontes_para_grupos(df_st, df_produtos, df_id_agrupados)

    # O enriquecimento final injeta a descricao padronizada oficial do grupo.
    return (
        df_mapeado
        .join(
            df_produtos_final.select(["id_agrupado", "descricao_padrao"]),
            on="id_agrupado",
            how="left",
            suffix="_produto_final",
        )
        .with_columns(
            pl.coalesce([pl.col("descricao_padrao_produto_final"), pl.col("descricao_padrao")]).alias("descricao_padrao")
        )
        .drop("descricao_padrao_produto_final")
    )


@registrar_gerador("st_itens")
def gerar_st_itens(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Consolida a trilha complementar de ST por documento e item sem quebrar o core homologado.

    Etapas:
    1. Carregar C176 e XML de ST
    2. Padronizar e consolidar bases
    3. Enriquecer com id_agrupado e descricao_padrao
    4. Conciliar com matriz SEFIN
    5. Validar integridade fiscal (NCM, CEST, CFOP)
    """
    caminho_c176 = diretorio_cnpj / "silver" / "c176_xml.parquet"
    caminho_st_xml = diretorio_cnpj / "silver" / "nfe_dados_st.parquet"
    caminho_produtos = diretorio_parquets / "produtos.parquet"
    caminho_id_agrupados = diretorio_parquets / "id_agrupados.parquet"
    caminho_produtos_final = diretorio_parquets / "produtos_final.parquet"
    diretorio_referencias = diretorio_cnpj.parent.parent / "dados" / "referencias"

    df_c176 = _carregar_parquet_se_existir(caminho_c176)
    df_st_xml = _carregar_parquet_se_existir(caminho_st_xml)
    if df_c176.is_empty() and df_st_xml.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    df_produtos = _carregar_parquet_se_existir(caminho_produtos)
    df_id_agrupados = _carregar_parquet_se_existir(caminho_id_agrupados)
    df_produtos_final = _carregar_parquet_se_existir(caminho_produtos_final)

    df_consolidado = _consolidar_fontes_st(df_c176, df_st_xml)
    df_enriquecido = _enriquecer_com_id_agrupado(df_consolidado, df_produtos, df_id_agrupados, df_produtos_final)

    # Conciliar com matriz SEFIN
    df_conciliado = _conciliar_com_matriz_sefin(df_enriquecido, diretorio_referencias)

    # Validar integridade fiscal
    validacao = validar_integridade_fiscal(df_conciliado)
    if validacao.get("ncm_invalidos", 0) > 0:
        logger.warning(
            "st_itens: %d NCM(s) invalidos encontrados",
            validacao.get("ncm_invalidos", 0)
        )
    if validacao.get("cst_invalidos", 0) > 0:
        logger.warning(
            "st_itens: %d CST(s) invalidos encontrados",
            validacao.get("cst_invalidos", 0)
        )

    # Log de conciliação SEFIN
    if "ncm_na_matriz" in df_conciliado.columns:
        na_matriz = int(df_conciliado["ncm_na_matriz"].sum())
        total = len(df_conciliado)
        logger.info(
            "st_itens: %d/%d itens conciliados com matriz SEFIN (%.1f%%)",
            na_matriz, total, (na_matriz / total * 100) if total > 0 else 0
        )

    total_registros = escrever_dataframe_ao_contrato(df_conciliado, arquivo_saida, contrato)
    logger.info("st_itens: %s registros gerados", total_registros)
    return total_registros
