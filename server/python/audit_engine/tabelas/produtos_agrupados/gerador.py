import logging
from pathlib import Path
from typing import Optional

from ...contratos.base import ContratoTabela
from ...pipeline.orquestrador import registrar_gerador
from ...utils.polars_utils import tipo_para_polars

logger = logging.getLogger(__name__)

@registrar_gerador("produtos_agrupados")
def gerar_produtos_agrupados(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela produtos_agrupados a partir de produtos.

    Processo:
    1. Lê tabela de produtos
    2. Aplica regras de agregação (NCM + similaridade de descrição)
    3. Carrega edições manuais se existirem
    4. Gera grupos com descrição padrão

    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_produtos = diretorio_parquets / "produtos.parquet"
    arquivo_edicoes = diretorio_cnpj / "edicoes" / "agregacao.json"

    if not arquivo_produtos.exists():
        raise FileNotFoundError("produtos.parquet não encontrado")

    df_produtos = pl.read_parquet(arquivo_produtos)

    if len(df_produtos) == 0:
        df = pl.DataFrame(
            schema={col.nome: tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Carregar edições manuais
    edicoes_manuais: Dict[str, List[int]] = {}
    if arquivo_edicoes.exists():
        with open(arquivo_edicoes) as f:
            edicoes_manuais = json.load(f)

    # Agrupar por NCM (agrupamento automático básico)
    grupos = []
    grupo_id = 1

    # Primeiro: aplicar edições manuais
    ids_ja_agrupados: Set[int] = set()
    for desc_padrao, ids in edicoes_manuais.items():
        membros = df_produtos.filter(pl.col("id_produto").is_in(ids))
        if len(membros) > 0:
            grupos.append(_criar_grupo(
                id_grupo=f"G{grupo_id:04d}",
                membros=membros,
                descricao_padrao=desc_padrao,
                origem="manual",
            ))
            ids_ja_agrupados.update(ids)
            grupo_id += 1

    # Depois: agrupar restantes por NCM
    restantes = df_produtos.filter(~pl.col("id_produto").is_in(list(ids_ja_agrupados)))

    # ⚡ Bolt: Use group_by instead of iterative filtering to avoid O(N*M) performance bottleneck
    for key_tuple, membros_ncm in restantes.group_by("ncm", maintain_order=True):
        ncm = key_tuple[0] if isinstance(key_tuple, tuple) else key_tuple
        grupos.append(_criar_grupo(
            id_grupo=f"G{grupo_id:04d}",
            membros=membros_ncm,
            descricao_padrao=membros_ncm["descricao"][0] if len(membros_ncm) > 0 else "",
            origem="automatico",
        ))
        grupo_id += 1

    if grupos:
        df = pl.DataFrame(grupos)
    else:
        df = pl.DataFrame(
            schema={col.nome: tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )

    df.write_parquet(arquivo_saida)
    logger.info(f"produtos_agrupados: {len(df)} grupos gerados")
    return len(df)



