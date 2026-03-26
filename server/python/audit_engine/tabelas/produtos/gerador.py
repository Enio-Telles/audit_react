import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador

logger = logging.getLogger(__name__)

@registrar_gerador("produtos")
def gerar_produtos(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela produtos a partir de produtos_unidades.
    
    Processo:
    1. Lê produtos_unidades
    2. Agrupa por id_produto (remove duplicatas de unidade)
    3. Determina unidade principal (mais frequente)
    4. Calcula totais consolidados
    
    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_entrada = diretorio_parquets / "produtos_unidades.parquet"
    
    if not arquivo_entrada.exists():
        raise FileNotFoundError("produtos_unidades.parquet não encontrado")

    df_entrada = pl.read_parquet(arquivo_entrada)
    
    if len(df_entrada) == 0:
        df = pl.DataFrame(
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Agrupar por id_produto
    df = (
        df_entrada
        .group_by("id_produto")
        .agg([
            pl.col("descricao").first(),
            pl.col("ncm").first(),
            pl.col("cest").first(),
            pl.col("unid_venda").first().alias("unidade_principal"),
            (pl.col("qtd_nfe_compra").sum() + pl.col("qtd_nfe_venda").sum()).alias("qtd_total_nfe"),
            (pl.col("valor_total_compra").sum() + pl.col("valor_total_venda").sum()).alias("valor_total"),
        ])
        .with_columns(
            pl.when(pl.col("qtd_total_nfe") > 0)
            .then(pl.lit("ambos"))
            .otherwise(pl.lit("venda"))
            .alias("tipo")
        )
    )
    
    df.write_parquet(arquivo_saida)
    logger.info(f"produtos: {len(df)} registros gerados")
    return len(df)



def _tipo_para_polars(tipo: str):
    """Converte tipo do contrato para tipo Polars."""
    import polars as pl
    mapa = {
        "string": pl.Utf8,
        "int": pl.Int64,
        "float": pl.Float64,
        "date": pl.Utf8,  # Datas como string ISO
        "bool": pl.Boolean,
    }
    return mapa.get(tipo, pl.Utf8)
