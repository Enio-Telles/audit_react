"""
Composicao Polars do historico de enderecos do Dossie.

Observacao importante:
- a parte historica derivada de NF-e pode ser reproduzida integralmente;
- o registro oficial de `DM_PESSOA` so pode ser composto localmente quando o
  dataset cadastral expuser os campos detalhados de endereco.

Quando essa granularidade nao estiver disponivel, a funcao falha de forma
conservadora para manter o fallback Oracle da SQL `dossie_enderecos.sql`.
"""
from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from utilitarios.dataset_registry import carregar_lazyframe
from utilitarios.dataset_registry import criar_metadata
from utilitarios.dataset_registry import registrar_dataset

logger = logging.getLogger(__name__)

COLUNAS_ENDERECOS = [
    "origem",
    "ano_mes",
    "logradouro",
    "numero",
    "complemento",
    "bairro",
    "fone",
    "cep",
    "municipio",
    "uf",
]

COLUNAS_CADASTRO_DETALHADAS = {"desc_endereco", "bairro", "nu_cep", "no_municipio", "co_uf"}


def _garantir_colunas_finais(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Mantem o contrato identico ao da SQL legada."""

    resultado = dataframe
    for coluna in COLUNAS_ENDERECOS:
        if coluna not in resultado.columns:
            resultado = resultado.with_columns(pl.lit(None, dtype=pl.Utf8).alias(coluna))
    return resultado.select(COLUNAS_ENDERECOS)


def _normalizar_texto(coluna: str) -> pl.Expr:
    """Aplica o mesmo comportamento de `UPPER` usado na SQL original."""

    return pl.col(coluna).cast(pl.Utf8, strict=False).str.to_uppercase()


def _montar_endereco_oficial(cadastral_lazyframe: pl.LazyFrame) -> pl.LazyFrame:
    """
    Monta a linha oficial equivalente ao primeiro bloco da SQL.

    A composicao so e segura quando os campos detalhados da DM_PESSOA ja
    estiverem materializados no dataset compartilhado.
    """

    mapa_colunas = {
        str(coluna).strip().lower(): coluna
        for coluna in cadastral_lazyframe.collect_schema().names()
    }
    if not COLUNAS_CADASTRO_DETALHADAS.issubset(mapa_colunas):
        faltantes = sorted(COLUNAS_CADASTRO_DETALHADAS - set(mapa_colunas))
        raise ValueError(
            "Dataset cadastral compartilhado ainda nao possui granularidade "
            f"suficiente para compor o endereco oficial sem perda funcional: {faltantes}"
        )

    return cadastral_lazyframe.select(
        [
            pl.lit("DM_PESSOA/SITAFE").alias("origem"),
            pl.lit("ATUAL").alias("ano_mes"),
            pl.col(mapa_colunas["desc_endereco"]).cast(pl.Utf8, strict=False).alias("logradouro"),
            pl.lit(None, dtype=pl.Utf8).alias("numero"),
            pl.lit(None, dtype=pl.Utf8).alias("complemento"),
            pl.col(mapa_colunas["bairro"]).cast(pl.Utf8, strict=False).alias("bairro"),
            pl.lit(None, dtype=pl.Utf8).alias("fone"),
            pl.col(mapa_colunas["nu_cep"]).cast(pl.Utf8, strict=False).alias("cep"),
            pl.col(mapa_colunas["no_municipio"]).cast(pl.Utf8, strict=False).alias("municipio"),
            pl.col(mapa_colunas["co_uf"]).cast(pl.Utf8, strict=False).alias("uf"),
        ]
    )


def _montar_historico_nfe(nfe_lazyframe: pl.LazyFrame, cnpj: str) -> pl.LazyFrame:
    """Reproduz o bloco NF-e da SQL com agrupamento e ordenacao equivalentes."""

    return (
        nfe_lazyframe
        .filter(
            pl.all_horizontal(
                [
                    pl.col("co_destinatario").cast(pl.Utf8, strict=False) == cnpj,
                    pl.col("dhemi").cast(pl.Datetime, strict=False).dt.date() <= pl.lit(date.today()),
                ]
            )
        )
        .with_columns(
            (
                pl.col("dhemi").cast(pl.Datetime, strict=False).dt.year().cast(pl.Utf8)
                + pl.lit("/")
                + pl.col("dhemi").cast(pl.Datetime, strict=False).dt.month().cast(pl.Utf8)
            ).alias("ano_mes")
        )
        .select(
            [
                pl.lit("NFE").alias("origem"),
                pl.col("ano_mes"),
                _normalizar_texto("xlgr_dest").alias("logradouro"),
                _normalizar_texto("nro_dest").alias("numero"),
                _normalizar_texto("xcpl_dest").alias("complemento"),
                _normalizar_texto("xbairro_dest").alias("bairro"),
                _normalizar_texto("fone_dest").alias("fone"),
                _normalizar_texto("cep_dest").alias("cep"),
                _normalizar_texto("xmun_dest").alias("municipio"),
                _normalizar_texto("co_uf_dest").alias("uf"),
            ]
        )
        .unique(maintain_order=False)
        .sort("ano_mes", descending=True)
    )


def composicao_enderecos(cnpj: str) -> pl.DataFrame:
    """
    Avalia e, quando seguro, materializa a composicao de enderecos.

    Se o cadastro compartilhado nao trouxer os campos detalhados da
    `DM_PESSOA`, a funcao falha de forma conservadora para preservar o
    resultado funcional da SQL Oracle original.
    """

    resultado_cadastral = carregar_lazyframe(cnpj, "cadastral")
    resultado_nfe = carregar_lazyframe(cnpj, "nfe_base")

    if resultado_cadastral is None:
        raise FileNotFoundError("Dataset cadastral ausente para composicao de enderecos.")
    if resultado_nfe is None:
        raise FileNotFoundError("Dataset nfe_base ausente para composicao de enderecos.")

    cadastral_lazyframe, _ = resultado_cadastral
    nfe_lazyframe, _ = resultado_nfe

    endereco_oficial = _montar_endereco_oficial(cadastral_lazyframe)
    historico_nfe = _montar_historico_nfe(nfe_lazyframe, cnpj)

    dataframe = pl.concat([endereco_oficial, historico_nfe], how="vertical_relaxed").collect()
    return _garantir_colunas_finais(dataframe)


def atualizar_composicao_enderecos(cnpj: str) -> Path | None:
    """Tenta materializar a composicao conservadora de enderecos."""

    try:
        dataframe = composicao_enderecos(cnpj)
        metadata = criar_metadata(
            cnpj=cnpj,
            dataset_id="composicao_enderecos",
            sql_id="dossie_enderecos.sql",
            parametros={
                "modo": "composicao_polars",
                "data_limite_processamento": date.today().isoformat(),
                "datasets_origem": ["cadastral", "nfe_base"],
                "tabela_origem": ["BI.DM_PESSOA", "BI.DM_LOCALIDADE", "BI.FATO_NFE_DETALHE"],
                "observacao": (
                    "Composicao ativada apenas quando o dataset cadastral "
                    "expuser os campos detalhados do endereco oficial."
                ),
            },
            linhas=dataframe.height,
        )
        return registrar_dataset(cnpj, "composicao_enderecos", dataframe, metadata=metadata)
    except Exception:
        logger.exception("Composicao de enderecos indisponivel para %s", cnpj)
        return None
