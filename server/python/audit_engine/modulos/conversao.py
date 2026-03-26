"""
Módulo de Conversão — audit_engine
Gera tabelas: fatores_conversao, produtos_final
Baseado nos módulos do audit_pyside.
"""
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from ..contratos.tabelas import ContratoTabela
from ..pipeline.orquestrador import registrar_gerador

logger = logging.getLogger(__name__)


@registrar_gerador("fatores_conversao")
def gerar_fatores_conversao(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela fatores_conversao a partir de produtos_agrupados.
    
    Processo:
    1. Lê produtos_agrupados
    2. Tenta obter fatores do Reg0220 (EFD)
    3. Calcula fatores automáticos quando possível
    4. Marca como 'pendente' fatores que precisam edição manual
    5. Aplica edições manuais salvas
    
    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_agrupados = diretorio_parquets / "produtos_agrupados.parquet"
    arquivo_reg0220 = diretorio_cnpj / "extraidos" / "reg0220.parquet"
    arquivo_edicoes = diretorio_cnpj / "edicoes" / "fatores.json"
    
    if not arquivo_agrupados.exists():
        raise FileNotFoundError("produtos_agrupados.parquet não encontrado")

    df_agrupados = pl.read_parquet(arquivo_agrupados)
    
    if len(df_agrupados) == 0:
        df = pl.DataFrame(
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Carregar Reg0220 se disponível
    fatores_reg0220: Dict[str, float] = {}
    if arquivo_reg0220.exists():
        df_reg = pl.read_parquet(arquivo_reg0220)
        # TODO: Mapear fatores do Reg0220 para IDs agrupados
        logger.info(f"Reg0220 carregado: {len(df_reg)} registros")

    # Carregar edições manuais
    edicoes: Dict[str, dict] = {}
    if arquivo_edicoes.exists():
        with open(arquivo_edicoes) as f:
            edicoes = json.load(f)

    # Gerar fatores
    registros = []
    for row in df_agrupados.iter_rows(named=True):
        id_agrupado = row["id_agrupado"]
        unid_compra = row.get("unid_compra", "")
        unid_venda = row.get("unid_venda", "")
        
        # Determinar unidade de referência e fatores
        unid_ref = unid_venda if unid_venda else unid_compra
        fator_compra = 1.0
        fator_venda = 1.0
        origem = "calculado"
        status = "ok"

        # Verificar se unidades são diferentes → precisa fator
        if unid_compra and unid_venda and unid_compra != unid_venda:
            if id_agrupado in fatores_reg0220:
                fator_compra = fatores_reg0220[id_agrupado]
                origem = "reg0220"
            else:
                status = "pendente"
                origem = "calculado"

        # Aplicar edições manuais
        if id_agrupado in edicoes:
            edicao = edicoes[id_agrupado]
            unid_ref = edicao.get("unid_ref", unid_ref)
            fator_compra = edicao.get("fator_compra_ref", fator_compra)
            fator_venda = edicao.get("fator_venda_ref", fator_venda)
            origem = "manual"
            status = "ok"

        registros.append({
            "id_agrupado": id_agrupado,
            "descricao_padrao": row["descricao_padrao"],
            "unid_compra": unid_compra,
            "unid_venda": unid_venda,
            "unid_ref": unid_ref,
            "fator_compra_ref": fator_compra,
            "fator_venda_ref": fator_venda,
            "origem_fator": origem,
            "status": status,
            "editado_em": datetime.now().isoformat(),
        })

    if registros:
        df = pl.DataFrame(registros)
    else:
        df = pl.DataFrame(
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
    
    df.write_parquet(arquivo_saida)
    logger.info(f"fatores_conversao: {len(df)} registros gerados")
    return len(df)


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
