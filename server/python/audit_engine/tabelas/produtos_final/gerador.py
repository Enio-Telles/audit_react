import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador

logger = logging.getLogger(__name__)

@registrar_gerador("produtos_final")
def gerar_produtos_final(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela produtos_final juntando agrupados + fatores.

    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_agrupados = diretorio_parquets / "produtos_agrupados.parquet"
    arquivo_fatores = diretorio_parquets / "fatores_conversao.parquet"

    if not arquivo_agrupados.exists():
        raise FileNotFoundError("produtos_agrupados.parquet não encontrado")
    if not arquivo_fatores.exists():
        raise FileNotFoundError("fatores_conversao.parquet não encontrado")

    df_agrupados = pl.read_parquet(arquivo_agrupados)
    df_fatores = pl.read_parquet(arquivo_fatores)

    if len(df_agrupados) == 0:
        df = pl.DataFrame(
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Join agrupados com fatores
    df = df_agrupados.join(
        df_fatores.select([
            "id_agrupado", "unid_ref", "fator_compra_ref", "fator_venda_ref", "status"
        ]),
        on="id_agrupado",
        how="left",
    ).rename({"status_right": "status_conversao", "status": "status_agregacao"})

    # Selecionar colunas do contrato
    colunas_disponiveis = [c for c in [col.nome for col in contrato.colunas] if c in df.columns]
    df = df.select(colunas_disponiveis)

    df.write_parquet(arquivo_saida)
    logger.info(f"produtos_final: {len(df)} registros gerados")
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
