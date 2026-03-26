import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.polars_utils import tipo_para_polars

logger = logging.getLogger(__name__)

@registrar_gerador("aba_mensal")
def gerar_aba_mensal(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela aba_mensal com consolidação mensal de estoque.

    Processo:
    1. Lê mov_estoque
    2. Agrupa por id_agrupado + mês
    3. Calcula saldos, entradas, saídas, custo médio
    4. Detecta omissões (saldo negativo)

    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_mov = diretorio_parquets / "mov_estoque.parquet"

    if not arquivo_mov.exists():
        raise FileNotFoundError("mov_estoque.parquet não encontrado")

    df_mov = pl.read_parquet(arquivo_mov)

    if len(df_mov) == 0:
        df = pl.DataFrame(
            schema={col.nome: tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Extrair mês da data
    df_mov = df_mov.with_columns(
        pl.col("data").str.slice(0, 7).alias("mes")
    )

    # Agrupar por id_agrupado + mês
    df = (
        df_mov
        .group_by(["id_agrupado", "mes"])
        .agg([
            pl.col("descricao").first(),
            pl.col("quantidade").filter(pl.col("tipo") == "ENTRADA").sum().alias("entradas"),
            pl.col("quantidade").filter(pl.col("tipo") == "SAIDA").sum().alias("saidas"),
            pl.col("saldo").first().alias("saldo_inicial"),
            pl.col("saldo").last().alias("saldo_final"),
            pl.col("custo_medio").last(),
            (pl.col("saldo").last() * pl.col("custo_medio").last()).alias("valor_estoque"),
            pl.len().alias("qtd_movimentos"),
        ])
        .with_columns(
            (pl.col("saldo_final") < 0).alias("omissao")
        )
        .sort(["id_agrupado", "mes"])
    )

    df.write_parquet(arquivo_saida)
    logger.info(f"aba_mensal: {len(df)} registros gerados")
    return len(df)



