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

    # TODO: Implementar consolidação de entradas e saídas e inventário
    # Por enquanto, retorna um DataFrame vazio respeitando o contrato
    df = pl.DataFrame(
        schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
    )
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
