"""Funcoes auxiliares compartilhadas do pipeline fiscal."""

from __future__ import annotations

import json
import unicodedata
from pathlib import Path
from typing import Any

import polars as pl

from ..contratos.base import ContratoTabela


def tipo_contrato_para_polars(tipo: str) -> pl.DataType:
    """Converte o tipo declarado no contrato para o tipo equivalente do Polars."""
    mapa = {
        "string": pl.Utf8,
        "int": pl.Int64,
        "float": pl.Float64,
        "date": pl.Utf8,
        "bool": pl.Boolean,
    }
    return mapa.get(tipo, pl.Utf8)


def criar_dataframe_vazio_contrato(contrato: ContratoTabela) -> pl.DataFrame:
    """Cria DataFrame vazio obedecendo o schema do contrato."""
    return pl.DataFrame(
        schema={coluna.nome: tipo_contrato_para_polars(coluna.tipo.value) for coluna in contrato.colunas}
    )


def alinhar_dataframe_ao_contrato(df: pl.DataFrame, contrato: ContratoTabela) -> pl.DataFrame:
    """Alinha um DataFrame ao schema e a ordem de colunas definidos no contrato."""
    if df.is_empty() and not df.columns:
        return criar_dataframe_vazio_contrato(contrato)

    expressoes: list[pl.Expr] = []
    for coluna in contrato.colunas:
        tipo_destino = tipo_contrato_para_polars(coluna.tipo.value)
        if coluna.nome in df.columns:
            expressoes.append(pl.col(coluna.nome).cast(tipo_destino, strict=False).alias(coluna.nome))
        else:
            expressoes.append(pl.lit(None, dtype=tipo_destino).alias(coluna.nome))

    return df.select(expressoes)


def escrever_dataframe_ao_contrato(df: pl.DataFrame, arquivo_saida: Path, contrato: ContratoTabela) -> int:
    """Alinha e persiste um DataFrame conforme o contrato da tabela."""
    df_alinhado = alinhar_dataframe_ao_contrato(df, contrato)
    arquivo_saida.parent.mkdir(parents=True, exist_ok=True)
    df_alinhado.write_parquet(arquivo_saida, compression="zstd")
    return len(df_alinhado)


def normalizar_descricao(texto: str | None) -> str:
    """Normaliza descricoes em ASCII maiusculo para agrupamentos e joins."""
    if texto is None:
        return ""

    texto_sem_acento = unicodedata.normalize("NFKD", str(texto))
    texto_ascii = "".join(caractere for caractere in texto_sem_acento if not unicodedata.combining(caractere))
    return " ".join(texto_ascii.upper().strip().split())


def carregar_json_se_existir(caminho: Path, padrao: Any) -> Any:
    """Carrega JSON com fallback para valor padrao quando o arquivo nao existir."""
    if not caminho.exists():
        return padrao

    with caminho.open("r", encoding="utf-8") as arquivo:
        return json.load(arquivo)


def normalizar_lista_ids(valores: list[Any]) -> list[str]:
    """Normaliza listas de identificadores para strings sem vazios."""
    return [str(valor).strip() for valor in valores if str(valor).strip()]


def selecionar_primeiro_texto_preenchido(*valores: Any) -> str | None:
    """Retorna o primeiro texto nao vazio dentre os candidatos."""
    for valor in valores:
        texto = str(valor).strip() if valor is not None else ""
        if texto:
            return texto
    return None


def montar_lookup_produtos_agrupados(
    df_produtos: pl.DataFrame,
    df_id_agrupados: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Monta lookups unicos por descricao/NCM e por descricao para vincular fontes ao grupo."""
    if df_produtos.is_empty() or df_id_agrupados.is_empty():
        schema_lookup = {
            "descricao_normalizada": pl.Utf8,
            "ncm": pl.Utf8,
            "id_agrupado": pl.Utf8,
            "descricao_padrao": pl.Utf8,
        }
        return pl.DataFrame(schema=schema_lookup), pl.DataFrame(schema={k: v for k, v in schema_lookup.items() if k != "ncm"})

    df_produtos_normalizados = (
        df_produtos
        .with_columns(
            [
                pl.col("id_produto").cast(pl.Int64, strict=False),
                pl.col("descricao").cast(pl.Utf8, strict=False),
                pl.col("descricao").map_elements(normalizar_descricao, return_dtype=pl.String).alias("descricao_normalizada"),
                pl.col("ncm").cast(pl.Utf8, strict=False),
            ]
        )
        .join(
            df_id_agrupados.select(
                [
                    pl.col("id_produto").cast(pl.Int64, strict=False),
                    pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                    pl.col("descricao_padrao").cast(pl.Utf8, strict=False),
                ]
            ),
            on="id_produto",
            how="inner",
        )
    )

    df_lookup_descricao_ncm = (
        df_produtos_normalizados
        .group_by(["descricao_normalizada", "ncm"])
        .agg(
            [
                pl.col("id_agrupado").n_unique().alias("__qtd_grupos__"),
                pl.col("id_agrupado").first().alias("id_agrupado"),
                pl.col("descricao_padrao").first().alias("descricao_padrao"),
            ]
        )
        .filter(pl.col("__qtd_grupos__") == 1)
        .drop("__qtd_grupos__")
    )

    df_lookup_descricao = (
        df_produtos_normalizados
        .group_by("descricao_normalizada")
        .agg(
            [
                pl.col("id_agrupado").n_unique().alias("__qtd_grupos__"),
                pl.col("id_agrupado").first().alias("id_agrupado"),
                pl.col("descricao_padrao").first().alias("descricao_padrao"),
            ]
        )
        .filter(pl.col("__qtd_grupos__") == 1)
        .drop("__qtd_grupos__")
    )

    return df_lookup_descricao_ncm, df_lookup_descricao


def mapear_fontes_para_grupos(
    df_fontes: pl.DataFrame,
    df_produtos: pl.DataFrame,
    df_id_agrupados: pl.DataFrame,
) -> pl.DataFrame:
    """Mapeia movimentos documentais para `id_agrupado` via descricao e NCM."""
    if df_fontes.is_empty():
        return df_fontes.with_columns(
            [
                pl.lit(None, dtype=pl.Utf8).alias("id_agrupado"),
                pl.lit(None, dtype=pl.Utf8).alias("descricao_padrao"),
            ]
        )

    df_lookup_descricao_ncm, df_lookup_descricao = montar_lookup_produtos_agrupados(df_produtos, df_id_agrupados)

    df_fontes_normalizadas = df_fontes.with_columns(
        [
            pl.col("descricao").cast(pl.Utf8, strict=False).map_elements(normalizar_descricao, return_dtype=pl.String).alias("descricao_normalizada"),
            pl.col("ncm").cast(pl.Utf8, strict=False),
        ]
    )

    df_mapeado = (
        df_fontes_normalizadas
        .join(
            df_lookup_descricao_ncm.rename(
                {
                    "id_agrupado": "id_agrupado_por_ncm",
                    "descricao_padrao": "descricao_padrao_por_ncm",
                }
            ),
            on=["descricao_normalizada", "ncm"],
            how="left",
        )
        .join(
            df_lookup_descricao.rename(
                {
                    "id_agrupado": "id_agrupado_por_descricao",
                    "descricao_padrao": "descricao_padrao_por_descricao",
                }
            ),
            on="descricao_normalizada",
            how="left",
        )
        .with_columns(
            [
                pl.coalesce([pl.col("id_agrupado_por_ncm"), pl.col("id_agrupado_por_descricao")]).alias("id_agrupado"),
                pl.coalesce([pl.col("descricao_padrao_por_ncm"), pl.col("descricao_padrao_por_descricao")]).alias("descricao_padrao"),
            ]
        )
        .drop(["id_agrupado_por_ncm", "descricao_padrao_por_ncm", "id_agrupado_por_descricao", "descricao_padrao_por_descricao"])
    )

    return df_mapeado
