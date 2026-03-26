import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador

logger = logging.getLogger(__name__)

@registrar_gerador("mov_estoque")
def gerar_mov_estoque(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Movimentação de estoque consolidada (entradas + saídas + inventário).

    Processo:
    1. Carregar dependências (nfe_entrada, produtos_final)
    2. Consolidar entradas e saídas e inventário (Bloco H)
    3. Calcular movimentação
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")


    arquivo_nfe = diretorio_parquets / "nfe_entrada.parquet"
    arquivo_produtos = diretorio_parquets / "produtos_final.parquet"

    schema = {col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}

    # Prepara a leitura, mesmo que seja apenas a casca por enquanto
    if arquivo_nfe.exists() and arquivo_produtos.exists():
        try:
            df_nfe = pl.read_parquet(arquivo_nfe)
            df_prod = pl.read_parquet(arquivo_produtos)
            logger.info("Processando mov_estoque a partir de entradas e produtos finais.")
            df = pl.DataFrame(schema=schema)
        except Exception as e:
            logger.warning(f"Erro ao processar mov_estoque. Retornando vazio. {e}")
            df = pl.DataFrame(schema=schema)
    else:
        logger.info("Dependências para mov_estoque não encontradas. Gerando tabela vazia.")
        df = pl.DataFrame(schema=schema)

    df.write_parquet(arquivo_saida)
    logger.info(f"mov_estoque: {len(df)} registros gerados")
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
