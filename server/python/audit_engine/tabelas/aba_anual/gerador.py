"""Gerador da tabela `aba_anual`."""

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
    """Carrega a tabela de movimentacao basilar que contem furos logiciados e quantitativos exatos."""
    arquivo_mov_estoque = diretorio_parquets / "mov_estoque.parquet"
    if not arquivo_mov_estoque.exists():
        return pl.DataFrame()
    return pl.read_parquet(arquivo_mov_estoque)


def aplicar_agrega_anual(df_mov_estoque: pl.DataFrame) -> pl.DataFrame:
    """Agrupa por produto e ano as grandezas fisicas do estoque testado vs apontado em Balanco."""
    return df_mov_estoque.sort(["id_agrupado", "data", "tipo"]).group_by(["id_agrupado", "ano"], maintain_order=True).agg(
        [
            pl.col("descricao").first().alias("descricao"),
            # Fisico de Livro/Calculo
            pl.col("q_conv").filter(pl.col("tipo") == "0 - ESTOQUE INICIAL").sum().fill_null(0.0).alias("estoque_inicial"),
            pl.col("q_conv").filter(pl.col("tipo") == "1 - ENTRADA").sum().fill_null(0.0).alias("entradas"),
            pl.col("q_conv").filter(pl.col("tipo") == "2 - SAIDAS").sum().fill_null(0.0).alias("saidas"),
            pl.col("entr_desac_anual").sum().fill_null(0.0).alias("entradas_desacob"),
            
            # Balanço do Fim
            pl.col("__qtd_decl_final_audit__").filter(pl.col("tipo") == "3 - ESTOQUE FINAL").sum().fill_null(0.0).alias("estoque_final"),
            pl.col("saldo_estoque_anual").last().alias("saldo_final"),

            # Medicão de Precificação Ignorando devolucoes (excluir_estoque = True)
            pl.col("valor_total").filter((pl.col("tipo") == "1 - ENTRADA") & (~pl.col("excluir_estoque"))).sum().alias("_pme_val"),
            pl.col("q_conv").filter((pl.col("tipo") == "1 - ENTRADA") & (~pl.col("excluir_estoque"))).sum().alias("_pme_qtd"),
            pl.col("valor_total").filter((pl.col("tipo") == "2 - SAIDAS") & (~pl.col("excluir_estoque"))).sum().alias("_pms_val"),
            pl.col("q_conv").filter((pl.col("tipo") == "2 - SAIDAS") & (~pl.col("excluir_estoque"))).sum().alias("_pms_qtd"),
        ]
    ).with_columns([
        pl.when(pl.col("_pme_qtd") > 0).then(pl.col("_pme_val") / pl.col("_pme_qtd")).otherwise(0.0).alias("pme"),
        pl.when(pl.col("_pms_qtd") > 0).then(pl.col("_pms_val") / pl.col("_pms_qtd")).otherwise(0.0).alias("pms"),
    ]).drop(["_pme_val", "_pme_qtd", "_pms_val", "_pms_qtd"])


def carregar_e_cruzar_st_anual(df_anual: pl.DataFrame) -> pl.DataFrame:
    """Consolida as validações de ST do exercício a partir da matriz da SEFIN."""
    caminho_matriz = Path(__file__).resolve().parents[3] / "dados" / "referencias" / "sitafe_produto_sefin_aux.parquet"
    
    if not caminho_matriz.exists():
        logger.warning("aba_anual: Matriz ST ausente. Isentando validação de ST no cômputo (fallback mock mode).")
        return df_anual.with_columns(pl.lit(False).alias("tem_st_ano"))

    try:
        df_matriz = pl.read_parquet(caminho_matriz)
        
        # Filtra períodos com ST e converte as datas de vigência
        df_st = df_matriz.select([
            pl.col("it_co_sefin").alias("id_agrupado"),
            pl.col("it_in_st"),
            pl.col("it_da_inicio").dt.year().alias("ano_inicio"),
            pl.col("it_da_final").dt.year().alias("ano_final"),
        ]).filter(pl.col("it_in_st") == "S")
        
        # Tabela com as chaves a verificar
        df_cruzamento = df_anual.select(["id_agrupado", "ano"]).unique()
        
        # Junta com os períodos de ST baseados no código
        df_com_vigencia = df_cruzamento.join(df_st, on="id_agrupado", how="left")
        
        # Avalia sobreposição de datas e se tem validade neste ano
        df_valida_st = df_com_vigencia.filter(
            (pl.col("ano") >= pl.col("ano_inicio")) &
            (pl.col("ano") <= pl.col("ano_final"))
        ).with_columns(pl.lit(True).alias("st_vigente"))
        
        # Consolida se houve qualquer intercepção da ST
        df_resumo_st = df_valida_st.group_by(["id_agrupado", "ano"]).agg(
            pl.col("st_vigente").any().alias("tem_st_ano")
        )
        
        # O merge final resgata a flag, isentando o restante
        df_retorno = df_anual.join(df_resumo_st, on=["id_agrupado", "ano"], how="left").with_columns(
            pl.col("tem_st_ano").fill_null(False)
        )
        return df_retorno
        
    except Exception as e:
        logger.error("aba_anual: Erro ao cruzar ST: %s. Isentando validação de ST (fallback mock mode).", e)
        return df_anual.with_columns(pl.lit(False).alias("tem_st_ano"))


def calcular_omissao_por_confronto_anual(df_anual: pl.DataFrame) -> pl.DataFrame:
    """Efetiva o balancete fiscal de confronto Inventario X Movimentacao Real para apuracao do furo e abate ST."""
    
    aliq_interna = 0.205 # Baseline 20.5% (TBD external param)
    
    df_calc = df_anual.with_columns(
        [
            pl.when(pl.col("estoque_final") > pl.col("saldo_final"))
            .then(pl.col("estoque_final") - pl.col("saldo_final"))
            .otherwise(0.0)
            .alias("saidas_desacob"),
            
            pl.when(pl.col("saldo_final") > pl.col("estoque_final"))
            .then(pl.col("saldo_final") - pl.col("estoque_final"))
            .otherwise(0.0)
            .alias("estoque_final_desacob"),
        ]
    )
    
    return df_calc.with_columns(
        [
            pl.when(pl.col("tem_st_ano") == True)
            .then(0.0)
            .when(pl.col("saidas_desacob") > 0)
            .then(
                pl.when(pl.col("pms") > 0)
                .then(pl.col("pms") * pl.col("saidas_desacob") * aliq_interna)
                .otherwise(pl.col("pme") * 1.30 * pl.col("saidas_desacob") * aliq_interna)
            )
            .otherwise(0.0)
            .alias("ICMS_saidas_desac"),

            pl.when(pl.col("estoque_final_desacob") > 0)
            .then(
                pl.when(pl.col("pms") > 0)
                .then(pl.col("pms") * pl.col("estoque_final_desacob") * aliq_interna)
                .otherwise(pl.col("pme") * 1.30 * pl.col("estoque_final_desacob") * aliq_interna)
            )
            .otherwise(0.0)
            .alias("ICMS_estoque_desac"),
        ]
    )


@registrar_gerador("aba_anual")
def gerar_aba_anual(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """Gera o fechamento anual a partir do fluxo interino total apontando o furo ou excesso inventarial."""
    df_mov_estoque = carregar_movimentacoes_estoque(diretorio_parquets)
    if df_mov_estoque.is_empty():
        logger.warning("aba_anual: Dependência mov_estoque vazia ou ausente.")
        return escrever_dataframe_ao_contrato(criar_dataframe_vazio_contrato(contrato), arquivo_saida, contrato)

    # 1. Agrega as dimensoes fisicas na borda do limite do ano civil
    df_resumo_anual = aplicar_agrega_anual(df_mov_estoque)
    
    # 2. Avalia matriz do sistema base e averigua abatimento por regime ST
    df_com_st = carregar_e_cruzar_st_anual(df_resumo_anual)
    
    # 3. Computa omissões de encerramento baseadas nos furos de declaracao comparada com a apurada
    df_final = calcular_omissao_por_confronto_anual(df_com_st)

    total_registros = escrever_dataframe_ao_contrato(df_final, arquivo_saida, contrato)
    logger.info("aba_anual: %s registros gerados", total_registros)
    return total_registros
