"""
Módulo de Estoque — audit_engine
Gera tabelas: nfe_entrada, mov_estoque, aba_mensal, aba_anual, produtos_selecionados
Baseado nos módulos do audit_pyside.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from ..contratos.tabelas import ContratoTabela
from ..pipeline.orquestrador import registrar_gerador

logger = logging.getLogger(__name__)


@registrar_gerador("nfe_entrada")
def gerar_nfe_entrada(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela nfe_entrada enriquecida com fatores de conversão.
    
    Processo:
    1. Lê NFe de entrada extraídas
    2. Aplica fatores de conversão para unidade de referência
    3. Classifica por CFOP (CO SEFIN)
    
    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_produtos_final = diretorio_parquets / "produtos_final.parquet"
    
    if not arquivo_produtos_final.exists():
        raise FileNotFoundError("produtos_final.parquet não encontrado")

    # TODO: Implementar leitura de NFe extraídas e enriquecimento
    df = pl.DataFrame(
        schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
    )
    df.write_parquet(arquivo_saida)
    
    logger.info(f"nfe_entrada: {len(df)} registros gerados")
    return len(df)


@registrar_gerador("mov_estoque")
def gerar_mov_estoque(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela mov_estoque consolidando entradas, saídas e inventário.
    
    Processo:
    1. Lê nfe_entrada (entradas)
    2. Lê NFe de saída (saídas)
    3. Lê Bloco H (inventário)
    4. Ordena cronologicamente
    5. Calcula saldo acumulado e custo médio ponderado
    
    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_nfe_entrada = diretorio_parquets / "nfe_entrada.parquet"
    arquivo_produtos_final = diretorio_parquets / "produtos_final.parquet"
    
    if not arquivo_nfe_entrada.exists():
        raise FileNotFoundError("nfe_entrada.parquet não encontrado")

    # TODO: Implementar consolidação de movimentos
    df = pl.DataFrame(
        schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
    )
    df.write_parquet(arquivo_saida)
    
    logger.info(f"mov_estoque: {len(df)} registros gerados")
    return len(df)


@registrar_gerador("aba_mensal")
def gerar_aba_mensal(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela aba_mensal com consolidação mensal de estoque.
    
    Processo:
    1. Lê mov_estoque
    2. Agrupa por id_agrupado + mês
    3. Calcula saldos, entradas, saídas, custo médio
    4. Detecta omissões (saldo negativo)
    
    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_mov = diretorio_parquets / "mov_estoque.parquet"
    
    if not arquivo_mov.exists():
        raise FileNotFoundError("mov_estoque.parquet não encontrado")

    df_mov = pl.read_parquet(arquivo_mov)
    
    if len(df_mov) == 0:
        df = pl.DataFrame(
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Extrair mês da data
    df_mov = df_mov.with_columns(
        pl.col("data").str.slice(0, 7).alias("mes")
    )

    # Agrupar por id_agrupado + mês
    df = (
        df_mov
        .group_by(["id_agrupado", "mes"])
        .agg([
            pl.col("descricao").first(),
            pl.col("quantidade").filter(pl.col("tipo") == "ENTRADA").sum().alias("entradas"),
            pl.col("quantidade").filter(pl.col("tipo") == "SAIDA").sum().alias("saidas"),
            pl.col("saldo").first().alias("saldo_inicial"),
            pl.col("saldo").last().alias("saldo_final"),
            pl.col("custo_medio").last(),
            (pl.col("saldo").last() * pl.col("custo_medio").last()).alias("valor_estoque"),
            pl.len().alias("qtd_movimentos"),
        ])
        .with_columns(
            (pl.col("saldo_final") < 0).alias("omissao")
        )
        .sort(["id_agrupado", "mes"])
    )
    
    df.write_parquet(arquivo_saida)
    logger.info(f"aba_mensal: {len(df)} registros gerados")
    return len(df)


@registrar_gerador("aba_anual")
def gerar_aba_anual(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela aba_anual com consolidação anual de estoque.
    
    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_mensal = diretorio_parquets / "aba_mensal.parquet"
    
    if not arquivo_mensal.exists():
        raise FileNotFoundError("aba_mensal.parquet não encontrado")

    df_mensal = pl.read_parquet(arquivo_mensal)
    
    if len(df_mensal) == 0:
        df = pl.DataFrame(
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Extrair ano do mês
    df_mensal = df_mensal.with_columns(
        pl.col("mes").str.slice(0, 4).alias("ano")
    )

    # Agrupar por id_agrupado + ano
    df = (
        df_mensal
        .group_by(["id_agrupado", "ano"])
        .agg([
            pl.col("descricao").first(),
            pl.col("saldo_inicial").first().alias("saldo_inicial_ano"),
            pl.col("entradas").sum().alias("total_entradas"),
            pl.col("saidas").sum().alias("total_saidas"),
            pl.col("saldo_final").last().alias("saldo_final_ano"),
            pl.col("custo_medio").last().alias("custo_medio_anual"),
            pl.col("valor_estoque").last().alias("valor_estoque_final"),
            pl.col("omissao").sum().cast(pl.Int64).alias("meses_com_omissao"),
        ])
        .with_columns(
            pl.when(pl.col("saldo_final_ano") < 0)
            .then(pl.col("saldo_final_ano").abs())
            .otherwise(pl.lit(0.0))
            .alias("total_omissao")
        )
        .sort(["id_agrupado", "ano"])
    )
    
    df.write_parquet(arquivo_saida)
    logger.info(f"aba_anual: {len(df)} registros gerados")
    return len(df)


@registrar_gerador("produtos_selecionados")
def gerar_produtos_selecionados(
    diretorio_cnpj: Path,
    diretorio_parquets: Path,
    arquivo_saida: Path,
    contrato: ContratoTabela,
) -> int:
    """
    Gera tabela produtos_selecionados para análise detalhada.
    
    Returns:
        Número de registros gerados
    """
    try:
        import polars as pl
    except ImportError:
        raise RuntimeError("Polars não instalado")

    arquivo_final = diretorio_parquets / "produtos_final.parquet"
    
    if not arquivo_final.exists():
        raise FileNotFoundError("produtos_final.parquet não encontrado")

    df_final = pl.read_parquet(arquivo_final)
    
    if len(df_final) == 0:
        df = pl.DataFrame(
            schema={col.nome: _tipo_para_polars(col.tipo.value) for col in contrato.colunas}
        )
        df.write_parquet(arquivo_saida)
        return 0

    # Selecionar colunas relevantes e adicionar flag de seleção
    df = df_final.select([
        "id_agrupado",
        "descricao_padrao",
        "ncm_padrao",
    ]).with_columns([
        pl.lit(True).alias("selecionado"),
        pl.lit("").alias("motivo"),
    ])
    
    df.write_parquet(arquivo_saida)
    logger.info(f"produtos_selecionados: {len(df)} registros gerados")
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
