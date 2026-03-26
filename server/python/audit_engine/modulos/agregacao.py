"""
Módulo de Agregação — audit_engine
Gera tabelas: produtos_agrupados, id_agrupados
Baseado nos módulos do audit_pyside.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from ..contratos.tabelas import ContratoTabela
from ..pipeline.orquestrador import registrar_gerador

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
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
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
    
    for ncm in restantes["ncm"].unique().to_list():
        membros_ncm = restantes.filter(pl.col("ncm") == ncm)
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
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
    
    df.write_parquet(arquivo_saida)
    logger.info(f"produtos_agrupados: {len(df)} grupos gerados")
    return len(df)


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
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Expandir mapeamento
    registros = []
    for row in df_agrupados.iter_rows(named=True):
        ids_membros = json.loads(row["ids_membros"]) if isinstance(row["ids_membros"], str) else []
        for id_prod in ids_membros:
            produto = df_produtos.filter(pl.col("id_produto") == id_prod)
            desc_original = produto["descricao"][0] if len(produto) > 0 else ""
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
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
    
    df.write_parquet(arquivo_saida)
    logger.info(f"id_agrupados: {len(df)} mapeamentos gerados")
    return len(df)


def _criar_grupo(id_grupo: str, membros, descricao_padrao: str, origem: str) -> dict:
    """Cria um registro de grupo a partir de um DataFrame de membros."""
    import polars as pl
    
    ids = membros["id_produto"].to_list()
    agora = datetime.now().isoformat()
    
    return {
        "id_agrupado": id_grupo,
        "descricao_padrao": descricao_padrao,
        "ncm_padrao": membros["ncm"][0] if len(membros) > 0 else "",
        "cest_padrao": membros["cest"][0] if len(membros) > 0 and "cest" in membros.columns else "",
        "ids_membros": json.dumps(ids),
        "qtd_membros": len(ids),
        "qtd_total_nfe": int(membros["qtd_total_nfe"].sum()) if "qtd_total_nfe" in membros.columns else 0,
        "valor_total": float(membros["valor_total"].sum()) if "valor_total" in membros.columns else 0.0,
        "unid_compra": membros["unidade_principal"][0] if len(membros) > 0 and "unidade_principal" in membros.columns else "",
        "unid_venda": membros["unidade_principal"][0] if len(membros) > 0 and "unidade_principal" in membros.columns else "",
        "origem": origem,
        "criado_em": agora,
        "editado_em": agora,
        "status": "ativo",
    }


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
