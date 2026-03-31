"""Gerador da tabela `aba_mensal`."""

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


def carregar_movimentacoes_estoque(diretorio_parquets: Path) -> pl.DataFrame:
    """Carrega a trilha materializada de estoque que alimenta os resumos mensais."""
    arquivo_mov_estoque = diretorio_parquets / "mov_estoque.parquet"
    if not arquivo_mov_estoque.exists():
        return pl.DataFrame()
    return pl.read_parquet(arquivo_mov_estoque)


def aplicar_agrega_mensal(df_mov_estoque: pl.DataFrame) -> pl.DataFrame:
    """Agrupa por produto e mes, sumariamente consolidando entradas, saidas e calculos de precificacao."""
    # A base mov_estoque ja deve vir com `ano` e `mes`
    # Filtro pme/pms: apenas entradas legitimas (excluir_estoque=False) e valor > 0
    return df_mov_estoque.sort(["id_agrupado", "data", "tipo"]).group_by(["id_agrupado", "ano", "mes"], maintain_order=True).agg(
        [
            pl.col("descricao").first().alias("descricao"),
            # Resumo Físico e Financeiro
            pl.col("valor_total").filter(pl.col("tipo") == "1 - ENTRADA").sum().fill_null(0.0).alias("valor_entradas"),
            pl.col("q_conv").filter(pl.col("tipo") == "1 - ENTRADA").sum().fill_null(0.0).alias("qtd_entradas"),
            pl.col("valor_total").filter(pl.col("tipo") == "2 - SAIDAS").sum().fill_null(0.0).alias("valor_saidas"),
            pl.col("q_conv").filter(pl.col("tipo") == "2 - SAIDAS").sum().fill_null(0.0).alias("qtd_saidas"),
            
            # Entradas Desacobertadas do mes
            pl.col("entr_desac_anual").sum().fill_null(0.0).alias("entradas_desacob"),
            
            # Saldos em linha do tempo (o ultimo registrado na janela de tempo de fechamento)
            pl.col("saldo_estoque_anual").last().alias("saldo_mes"),
            pl.col("custo_medio_anual").last().alias("custo_medio_mes"),
            
            # PME / PMS calculados por divisoes exclusivas
            pl.col("valor_total").filter((pl.col("tipo") == "1 - ENTRADA") & (~pl.col("excluir_estoque"))).sum().alias("_pme_val"),
            pl.col("q_conv").filter((pl.col("tipo") == "1 - ENTRADA") & (~pl.col("excluir_estoque"))).sum().alias("_pme_qtd"),
            pl.col("valor_total").filter((pl.col("tipo") == "2 - SAIDAS") & (~pl.col("excluir_estoque"))).sum().alias("_pms_val"),
            pl.col("q_conv").filter((pl.col("tipo") == "2 - SAIDAS") & (~pl.col("excluir_estoque"))).sum().alias("_pms_qtd"),
        ]
    ).with_columns([
        (pl.col("saldo_mes") * pl.col("custo_medio_mes")).alias("valor_estoque"),
        pl.when(pl.col("_pme_qtd") > 0).then(pl.col("_pme_val") / pl.col("_pme_qtd")).otherwise(0.0).alias("pme_mes"),
        pl.when(pl.col("_pms_qtd") > 0).then(pl.col("_pms_val") / pl.col("_pms_qtd")).otherwise(0.0).alias("pms_mes"),
    ]).drop(["_pme_val", "_pme_qtd", "_pms_val", "_pms_qtd"])


def calcular_icms_omissao_mensal(df_resumo: pl.DataFrame) -> pl.DataFrame:
    """Aplica aliquotas de infracao e flag de substituicao tributaria no mes fiscalizado."""
    # # TODO: implementar cruzamento de regencia ST contra `sitafe_produto_sefin_aux`
    # Para o escopo de baseline, implementamos isenção/falta dela fixa s/ MVA e MVA ajustado.
    
    aliq_mes = 0.205 # Baseline standard ICMS (20.5% ou param global estado) - # TODO: Mover parametro
    MVA_efetivo = 1.0 # Sem aplicacao MVA no momento ate integracao da extracao ST
    
    return df_resumo.with_columns(
        [
            pl.lit("SEM INCIDENCIA NA BASE MOCK").cast(pl.Utf8, strict=False).alias("ST"),
            pl.lit(0.0).alias("MVA"),
            pl.lit(0.0).alias("MVA_ajustado"),
            
            # Base SAIDAS desacobertadas (ICMS_entr_desacob)
            pl.when(pl.col("entradas_desacob") > 0)
            .then(
                pl.when(pl.col("pms_mes") > 0)
                .then(pl.col("pms_mes") * pl.col("entradas_desacob") * aliq_mes)
                .otherwise(pl.col("pme_mes") * pl.col("entradas_desacob") * aliq_mes * MVA_efetivo)
            )
            .otherwise(0.0)
            .alias("ICMS_entr_desacob")
        ]
    )


@registrar_gerador("aba_mensal")
def gerar_aba_mensal(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Gera o resumo mensal de estoque validando operacoes omissas contra parametros MVA e ICMS."""
    df_mov_estoque = carregar_movimentacoes_estoque(diretorio_parquets)
    if df_mov_estoque.is_empty():
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    # 1. Agregacao cronologica pura perimetrizada pelo calendario
    df_resumo_mensal = aplicar_agrega_mensal(df_mov_estoque)
    
    # 2. Computo logico-matematico de bases, limites solidarios e substituicao
    df_resumo_tributado = calcular_icms_omissao_mensal(df_resumo_mensal)
    
    # Adicionado formatacao ano-mes UI string padrao
    df_final = df_resumo_tributado.with_columns(
        (pl.col("ano").cast(pl.Utf8) + "-" + pl.col("mes").cast(pl.Utf8).str.pad_start(2, "0")).alias("ano_mes")
    )

    total_registros = escrever_dataframe_ao_contrato(df_final, arquivo_saida, contrato)
    logger.info("aba_mensal: %s registros gerados", total_registros)
    return total_registros
