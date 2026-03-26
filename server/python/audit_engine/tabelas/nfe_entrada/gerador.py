import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.polars_utils import tipo_para_polars

logger = logging.getLogger(__name__)

@registrar_gerador("nfe_entrada")
def gerar_nfe_entrada(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    NFe de entrada enriquecidas com classificação CO SEFIN.

    Processo:
    1. Carregar dependências (produtos_final)
    2. Ler NFe extraídas
    3. Aplicar fatores de conversão
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    # TODO: Implementar leitura real de NFe extraídas e cruzamento com fatores.
    # Por enquanto, retorna um DataFrame vazio respeitando o contrato
    df = pl.DataFrame(
        schema={col.nome: tipo_para_polars(col.tipo.value) for col in contrato.colunas}
    )
    df.write_parquet(arquivo_saida)
    logger.info(f"nfe_entrada: {len(df)} registros gerados")
    return len(df)

