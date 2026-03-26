import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.polars_utils import tipo_para_polars

logger = logging.getLogger(__name__)

@registrar_gerador("produtos_selecionados")
def gerar_produtos_selecionados(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela produtos_selecionados para análise detalhada.

    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_final = diretorio_parquets / "produtos_final.parquet"

    if not arquivo_final.exists():
        raise FileNotFoundError("produtos_final.parquet não encontrado")

    df_final = pl.read_parquet(arquivo_final)

    if len(df_final) == 0:
        df = pl.DataFrame(
            schema={col.nome: tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Selecionar colunas relevantes e adicionar flag de seleção
    df = df_final.select([
        "id_agrupado",
        "descricao_padrao",
        "ncm_padrao",
    ]).with_columns([
        pl.lit(True).alias("selecionado"),
        pl.lit("").alias("motivo"),
    ])

    df.write_parquet(arquivo_saida)
    logger.info(f"produtos_selecionados: {len(df)} registros gerados")
    return len(df)



