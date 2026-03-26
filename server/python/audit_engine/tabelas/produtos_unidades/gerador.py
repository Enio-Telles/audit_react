import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador

logger = logging.getLogger(__name__)

@registrar_gerador("produtos_unidades")
def gerar_produtos_unidades(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela produtos_unidades a partir dos dados extraídos.

    Processo:
    1. Ler NFe de compra e venda
    2. Ler registros EFD (Reg0200)
    3. Consolidar produtos e unidades
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    # TODO: Implementar leitura real dos dados extraídos de NFe e EFD.
    # Por enquanto, retorna um DataFrame vazio respeitando o contrato
    df = pl.DataFrame(
        schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
    )
    df.write_parquet(arquivo_saida)
    logger.info(f"produtos_unidades: {len(df)} registros gerados")
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
