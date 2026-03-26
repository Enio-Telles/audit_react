import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador

logger = logging.getLogger(__name__)

@registrar_gerador("aba_anual")
def gerar_aba_anual(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela aba_anual com consolidação anual de estoque.

    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_mensal = diretorio_parquets / "aba_mensal.parquet"

    if not arquivo_mensal.exists():
        raise FileNotFoundError("aba_mensal.parquet não encontrado")

    df_mensal = pl.read_parquet(arquivo_mensal)

    if len(df_mensal) == 0:
        df = pl.DataFrame(
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Extrair ano do mês
    df_mensal = df_mensal.with_columns(
        pl.col("mes").str.slice(0, 4).alias("ano")
    )

    # Agrupar por id_agrupado + ano
    df = (
        df_mensal
        .group_by(["id_agrupado", "ano"])
        .agg([
            pl.col("descricao").first(),
            pl.col("saldo_inicial").first().alias("saldo_inicial_ano"),
            pl.col("entradas").sum().alias("total_entradas"),
            pl.col("saidas").sum().alias("total_saidas"),
            pl.col("saldo_final").last().alias("saldo_final_ano"),
            pl.col("custo_medio").last().alias("custo_medio_anual"),
            pl.col("valor_estoque").last().alias("valor_estoque_final"),
            pl.col("omissao").sum().cast(pl.Int64).alias("meses_com_omissao"),
        ])
        .with_columns(
            pl.when(pl.col("saldo_final_ano") < 0)
            .then(pl.col("saldo_final_ano").abs())
            .otherwise(pl.lit(0.0))
            .alias("total_omissao")
        )
        .sort(["id_agrupado", "ano"])
    )

    df.write_parquet(arquivo_saida)
    logger.info(f"aba_anual: {len(df)} registros gerados")
    return len(df)



def _tipo_para_polars(tipo: str):
    import polars as pl
    mapa = {
        "string": pl.Utf8,
        "int": pl.Int64,
        "float": pl.Float64,
        "date": pl.Utf8,
        "bool": pl.Boolean,
    }
    return mapa.get(tipo, pl.Utf8)
