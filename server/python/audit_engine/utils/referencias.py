"""Carregamento de tabelas de referência (NCM, CEST, CFOP, CST, NFe)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

DIRETORIO_REFERENCIAS = Path(__file__).parent.parent / "dados" / "referencias"


def carregar_ncm() -> pl.DataFrame:
    """Carrega tabela completa de NCM.

    Returns:
        DataFrame com colunas: codigo, descricao, vigencia, info_adicional,
        tipo_ato, numero_ato, ano_ato, art_31, art_31_desc, art_89,
        art_89_desc, fim_31, fim_31_desc, fim_89, fim_89_desc, ibpt.
    """
    caminho = DIRETORIO_REFERENCIAS / "NCM" / "tabela_ncm.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def carregar_ncm_capitulos() -> pl.DataFrame:
    """Carrega capítulos de NCM para agrupamento.

    Returns:
        DataFrame com colunas: capitulo, descricao.
    """
    caminho = DIRETORIO_REFERENCIAS / "NCM" / "ncm_capitulos.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def carregar_cest() -> pl.DataFrame:
    """Carrega tabela CEST (Código Especificador da Substituição Tributária).

    Returns:
        DataFrame com colunas: codigo, descricao, ncm, observacao.
    """
    caminho = DIRETORIO_REFERENCIAS / "CEST" / "cest.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def carregar_segmentos_mercadorias() -> pl.DataFrame:
    """Carrega segmentos de mercadorias para classificação.

    Returns:
        DataFrame com informações de segmentos.
    """
    caminho = DIRETORIO_REFERENCIAS / "CEST" / "segmentos_mercadorias.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def carregar_cfop() -> pl.DataFrame:
    """Carrega tabela de CFOP (Código Fiscal de Operações e Prestações).

    Returns:
        DataFrame com colunas: codigo, descricao, tipo, esfera.
    """
    caminho = DIRETORIO_REFERENCIAS / "cfop" / "cfop.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def carregar_cfop_resumo() -> pl.DataFrame:
    """Carrega resumo de CFOP por primeiro dígito.

    Returns:
        DataFrame com colunas: digito, descricao, tipo_operacao.
    """
    caminho = DIRETORIO_REFERENCIAS / "cfop" / "cfop_1_digito.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def carregar_cst() -> pl.DataFrame:
    """Carrega tabela de CST (Código de Situação Tributária).

    Returns:
        DataFrame com colunas: codigo, descricao, tipo_tributacao.
    """
    caminho = DIRETORIO_REFERENCIAS / "cst" / "cst.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def carregar_cst_resumo() -> pl.DataFrame:
    """Carrega resumo de CST por primeiro dígito.

    Returns:
        DataFrame com colunas: digito, descricao, aplicacao.
    """
    caminho = DIRETORIO_REFERENCIAS / "cst" / "cst_1_dig.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def buscar_ncm_por_codigo(codigo: str) -> pl.DataFrame:
    """Busca NCM por código exato ou parcial.

    Args:
        codigo: Código NCM (8 dígitos) ou prefixo.

    Returns:
        DataFrame com registros matching.
    """
    df = carregar_ncm()
    if df.is_empty():
        return df

    codigo_normalizado = codigo.replace(".", "").replace("-", "").strip()
    # Coluna real: Codigo_NCM
    if len(codigo_normalizado) == 8:
        return df.filter(pl.col("Codigo_NCM") == codigo_normalizado)
    return df.filter(pl.col("Codigo_NCM").str.starts_with(codigo_normalizado))


def buscar_cest_por_codigo(codigo: str) -> pl.DataFrame:
    """Busca CEST por código exato.

    Args:
        codigo: Código CEST (7 dígitos).

    Returns:
        DataFrame com registro matching.
    """
    df = carregar_cest()
    if df.is_empty():
        return df

    codigo_normalizado = codigo.replace(".", "").strip()
    # Coluna real: Codigo_CEST
    if "Codigo_CEST" in df.columns:
        return df.filter(pl.col("Codigo_CEST") == codigo_normalizado)
    elif "codigo" in df.columns:
        return df.filter(pl.col("codigo") == codigo_normalizado)
    return df


def buscar_cfop_por_codigo(codigo: str) -> pl.DataFrame:
    """Busca CFOP por código exato.

    Args:
        codigo: Código CFOP (4 dígitos).

    Returns:
        DataFrame com registro matching.
    """
    df = carregar_cfop()
    if df.is_empty():
        return df

    codigo_normalizado = codigo.replace(".", "").strip()
    # Coluna real: CFOP
    if "CFOP" in df.columns:
        return df.filter(pl.col("CFOP") == codigo_normalizado)
    elif "codigo" in df.columns:
        return df.filter(pl.col("codigo") == codigo_normalizado)
    return df


def enriquecer_com_ncm(df: pl.DataFrame, coluna_ncm: str = "ncm") -> pl.DataFrame:
    """Enriquece DataFrame com descrições de NCM.

    Args:
        df: DataFrame com coluna de NCM.
        coluna_ncm: Nome da coluna de NCM no DataFrame.

    Returns:
        DataFrame com colunas adicionais: ncm_descricao, ncm_vigencia, ncm_info_adicional.
    """
    df_ncm = carregar_ncm()
    if df_ncm.is_empty():
        return df.with_columns(
            pl.lit(None).alias("ncm_descricao"),
            pl.lit(None).alias("ncm_vigencia"),
            pl.lit(None).alias("ncm_info_adicional"),
        )

    # Mapear colunas reais: Codigo_NCM -> codigo, Descricao -> descricao
    df_ncm_mapped = df_ncm.select([
        pl.col("Codigo_NCM").alias("codigo"),
        pl.col("Descricao").alias("descricao"),
        pl.col("Data_Inicio").alias("vigencia"),
        pl.col("Ato_Legal").alias("info_adicional"),
    ])

    return df.join(
        df_ncm_mapped.rename(
            {
                "codigo": coluna_ncm,
                "descricao": "ncm_descricao",
                "vigencia": "ncm_vigencia",
                "info_adicional": "ncm_info_adicional",
            },
        ),
        on=coluna_ncm,
        how="left",
    )


def enriquecer_com_cest(df: pl.DataFrame, coluna_cest: str = "cest") -> pl.DataFrame:
    """Enriquece DataFrame com descrições de CEST.

    Args:
        df: DataFrame com coluna de CEST.
        coluna_cest: Nome da coluna de CEST no DataFrame.

    Returns:
        DataFrame com coluna adicional: cest_descricao.
    """
    df_cest = carregar_cest()
    if df_cest.is_empty():
        return df.with_columns(pl.lit(None).alias("cest_descricao"))

    # Mapear colunas reais - CEST tem: ITEM, CEST, NCM, DESCRICAO
    if "CEST" in df_cest.columns and "DESCRICAO" in df_cest.columns:
        df_cest_mapped = df_cest.select([
            pl.col("CEST").alias("codigo"),
            pl.col("DESCRICAO").alias("descricao"),
        ])
    elif "Codigo_CEST" in df_cest.columns and "Descricao" in df_cest.columns:
        df_cest_mapped = df_cest.select([
            pl.col("Codigo_CEST").alias("codigo"),
            pl.col("Descricao").alias("descricao"),
        ])
    elif "codigo" in df_cest.columns and "descricao" in df_cest.columns:
        df_cest_mapped = df_cest.select(["codigo", "descricao"])
    else:
        return df.with_columns(pl.lit(None).alias("cest_descricao"))

    return df.join(
        df_cest_mapped.rename(
            {
                "codigo": coluna_cest,
                "descricao": "cest_descricao",
            },
        ),
        on=coluna_cest,
        how="left",
    )


def enriquecer_com_cfop(df: pl.DataFrame, coluna_cfop: str = "cfop") -> pl.DataFrame:
    """Enriquece DataFrame com descrições de CFOP.

    Args:
        df: DataFrame com coluna de CFOP.
        coluna_cfop: Nome da coluna de CFOP no DataFrame.

    Returns:
        DataFrame com colunas adicionais: cfop_descricao, cfop_tipo, cfop_esfera.
    """
    df_cfop = carregar_cfop()
    if df_cfop.is_empty():
        return df.with_columns(
            pl.lit(None).alias("cfop_descricao"),
            pl.lit(None).alias("cfop_tipo"),
            pl.lit(None).alias("cfop_esfera"),
        )

    # Mapear colunas reais - CFOP tem: id, co_cfop, descricao, codigo_tributacao, ...
    if "co_cfop" in df_cfop.columns and "descricao" in df_cfop.columns:
        df_cfop_mapped = df_cfop.select([
            pl.col("co_cfop").alias("codigo"),
            pl.col("descricao").alias("descricao"),
            pl.col("codigo_tributacao").alias("tipo"),
            pl.lit(None).alias("esfera"),  # CFOP não tem esfera
        ])
    elif "CFOP" in df_cfop.columns and "Descricao" in df_cfop.columns:
        df_cfop_mapped = df_cfop.select([
            pl.col("CFOP").alias("codigo"),
            pl.col("Descricao").alias("descricao"),
            pl.col("Tipo").alias("tipo"),
            pl.col("Esfera").alias("esfera"),
        ])
    elif "codigo" in df_cfop.columns and "descricao" in df_cfop.columns:
        df_cfop_mapped = df_cfop.select(["codigo", "descricao", "tipo", "esfera"])
    else:
        return df.with_columns(
            pl.lit(None).alias("cfop_descricao"),
            pl.lit(None).alias("cfop_tipo"),
            pl.lit(None).alias("cfop_esfera"),
        )

    return df.join(
        df_cfop_mapped.rename(
            {
                "codigo": coluna_cfop,
                "descricao": "cfop_descricao",
                "tipo": "cfop_tipo",
                "esfera": "cfop_esfera",
            },
        ),
        on=coluna_cfop,
        how="left",
    )


def validar_ncm(ncm: str) -> bool:
    """Valida se código NCM existe na tabela de referência.

    Args:
        ncm: Código NCM a validar.

    Returns:
        True se NCM existe, False caso contrário.
    """
    if not ncm:
        return False
    df = buscar_ncm_por_codigo(ncm)
    # Verifica se o NCM tem descrição (campo Descricao não nulo)
    return not df.is_empty() and df["Descricao"].is_not_null().any()


def validar_cest(cest: str) -> bool:
    """Valida se código CEST existe na tabela de referência.

    Args:
        cest: Código CEST a validar.

    Returns:
        True se CEST existe, False caso contrário.
    """
    if not cest:
        return False
    df = buscar_cest_por_codigo(cest)
    return not df.is_empty()


def validar_cfop(cfop: str) -> bool:
    """Valida se código CFOP existe na tabela de referência.

    Args:
        cfop: Código CFOP a validar.

    Returns:
        True se CFOP existe, False caso contrário.
    """
    if not cfop:
        return False
    df = buscar_cfop_por_codigo(cfop)
    return not df.is_empty()


# =============================================================================
# NFe - Domínios e Mapeamentos
# =============================================================================


def carregar_dominios_nfe() -> dict[str, pl.DataFrame]:
    """Carrega todos os domínios de NFe.

    Returns:
        Dicionário com DataFrames de cada domínio.
    """
    diretorio = DIRETORIO_REFERENCIAS / "NFe"
    dominios = {}

    if not diretorio.exists():
        return dominios

    for arquivo in diretorio.glob("dominio_*.parquet"):
        chave = arquivo.stem.replace("dominio_", "").lower()
        dominios[chave] = pl.read_parquet(arquivo)

    return dominios


def carregar_mapeamento_nfe() -> pl.DataFrame:
    """Carrega mapeamento de campos de NFe.

    Returns:
        DataFrame com mapeamento de campos.
    """
    caminho = DIRETORIO_REFERENCIAS / "NFe" / "mapeamento_NFe.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


def carregar_dominios_eventos_nfe() -> dict[str, pl.DataFrame]:
    """Carrega domínios de eventos de NFe.

    Returns:
        Dicionário com DataFrames de eventos.
    """
    diretorio = DIRETORIO_REFERENCIAS / "NFE_eventos"
    dominios = {}

    if not diretorio.exists():
        return dominios

    for arquivo in diretorio.glob("dominio_*.parquet"):
        chave = arquivo.stem.replace("dominio_", "").lower()
        dominios[chave] = pl.read_parquet(arquivo)

    return dominios


def carregar_malhas_fisconforme() -> pl.DataFrame:
    """Carrega malhas de fiscalização do Fisconforme.

    Returns:
        DataFrame com malhas de fiscalização.
    """
    caminho = DIRETORIO_REFERENCIAS / "Fisconforme" / "malhas.parquet"
    if not caminho.exists():
        return pl.DataFrame()
    return pl.read_parquet(caminho)


# =============================================================================
# Validação em Lote para Pipeline
# =============================================================================


def validar_coluna_ncm(df: pl.DataFrame, coluna: str = "ncm") -> pl.DataFrame:
    """Valida coluna de NCM e adiciona indicador de validade.

    Args:
        df: DataFrame com coluna de NCM.
        coluna: Nome da coluna de NCM.

    Returns:
        DataFrame com coluna adicional: ncm_valido (bool).
    """
    df_ncm = carregar_ncm()
    if df_ncm.is_empty():
        # Sem referência, assume todos válidos
        return df.with_columns(pl.lit(True).alias("ncm_valido"))

    # Extrair códigos válidos
    codigos_validos = df_ncm["Codigo_NCM"].unique().to_list()

    return df.with_columns(
        pl.col(coluna).is_in(codigos_validos).alias("ncm_valido")
    )


def validar_coluna_cest(df: pl.DataFrame, coluna: str = "cest") -> pl.DataFrame:
    """Valida coluna de CEST e adiciona indicador de validade.

    Args:
        df: DataFrame com coluna de CEST.
        coluna: Nome da coluna de CEST.

    Returns:
        DataFrame com coluna adicional: cest_valido (bool).
    """
    df_cest = carregar_cest()
    if df_cest.is_empty():
        return df.with_columns(pl.lit(True).alias("cest_valido"))

    # Extrair códigos válidos - colunas reais: ITEM, CEST, NCM, DESCRICAO
    if "CEST" in df_cest.columns:
        codigos_validos = df_cest["CEST"].unique().to_list()
    elif "Codigo_CEST" in df_cest.columns:
        codigos_validos = df_cest["Codigo_CEST"].unique().to_list()
    elif "codigo" in df_cest.columns:
        codigos_validos = df_cest["codigo"].unique().to_list()
    else:
        return df.with_columns(pl.lit(True).alias("cest_valido"))

    return df.with_columns(
        pl.col(coluna).is_in(codigos_validos).alias("cest_valido")
    )


def validar_coluna_cfop(df: pl.DataFrame, coluna: str = "cfop") -> pl.DataFrame:
    """Valida coluna de CFOP e adiciona indicador de validade.

    Args:
        df: DataFrame com coluna de CFOP.
        coluna: Nome da coluna de CFOP.

    Returns:
        DataFrame com coluna adicional: cfop_valido (bool).
    """
    df_cfop = carregar_cfop()
    if df_cfop.is_empty():
        return df.with_columns(pl.lit(True).alias("cfop_valido"))

    # Extrair códigos válidos - colunas reais: id, co_cfop, descricao, ...
    if "co_cfop" in df_cfop.columns:
        codigos_validos = df_cfop["co_cfop"].unique().to_list()
    elif "CFOP" in df_cfop.columns:
        codigos_validos = df_cfop["CFOP"].unique().to_list()
    elif "codigo" in df_cfop.columns:
        codigos_validos = df_cfop["codigo"].unique().to_list()
    else:
        return df.with_columns(pl.lit(True).alias("cfop_valido"))

    return df.with_columns(
        pl.col(coluna).is_in(codigos_validos).alias("cfop_valido")
    )


def validar_integridade_fiscal(df: pl.DataFrame) -> dict[str, int]:
    """Valida integridade fiscal de DataFrame com colunas NCM, CEST, CFOP.

    Args:
        df: DataFrame com colunas fiscais.

    Returns:
        Dicionário com contagens de válidos/inválidos por tipo.
    """
    resultado = {}

    if "ncm" in df.columns:
        df_validado = validar_coluna_ncm(df, "ncm")
        resultado["ncm_validos"] = int(df_validado["ncm_valido"].sum())
        resultado["ncm_invalidos"] = int((~df_validado["ncm_valido"]).sum())

    if "cest" in df.columns:
        df_validado = validar_coluna_cest(df, "cest")
        resultado["cest_validos"] = int(df_validado["cest_valido"].sum())
        resultado["cest_invalidos"] = int((~df_validado["cest_valido"]).sum())

    if "cfop" in df.columns:
        df_validado = validar_coluna_cfop(df, "cfop")
        resultado["cfop_validos"] = int(df_validado["cfop_valido"].sum())
        resultado["cfop_invalidos"] = int((~df_validado["cfop_valido"]).sum())

    return resultado
