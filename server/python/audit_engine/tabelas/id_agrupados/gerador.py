import json
import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.polars_utils import tipo_para_polars

logger = logging.getLogger(__name__)

@registrar_gerador("id_agrupados")
def gerar_id_agrupados(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela de mapeamento id_produto → id_agrupado.
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_agrupados = diretorio_parquets / "produtos_agrupados.parquet"
    arquivo_produtos = diretorio_parquets / "produtos.parquet"

    if not arquivo_agrupados.exists():
        raise FileNotFoundError("produtos_agrupados.parquet não encontrado")

    df_agrupados = pl.read_parquet(arquivo_agrupados)
    df_produtos = pl.read_parquet(arquivo_produtos)

    if len(df_agrupados) == 0:
        df = pl.DataFrame(
            schema={col.nome: tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # ⚡ Bolt: Optimize product description lookup
    # Converting DataFrame to dictionary for O(1) lookups instead of O(N) filtering inside nested loops.
    prod_desc_map = dict(zip(df_produtos["id_produto"], df_produtos["descricao"]))

    # Expandir mapeamento
    registros = []
    for row in df_agrupados.iter_rows(named=True):
        ids_membros = json.loads(row["ids_membros"]) if isinstance(row["ids_membros"], str) else []
        for id_prod in ids_membros:
            desc_original = prod_desc_map.get(id_prod, "")
            registros.append({
                "id_produto": id_prod,
                "id_agrupado": row["id_agrupado"],
                "descricao_original": desc_original,
                "descricao_padrao": row["descricao_padrao"],
            })

    if registros:
        df = pl.DataFrame(registros)
    else:
        df = pl.DataFrame(
            schema={col.nome: tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )

    df.write_parquet(arquivo_saida)
    logger.info(f"id_agrupados: {len(df)} mapeamentos gerados")
    return len(df)



