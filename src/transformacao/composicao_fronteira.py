"""
Composicao Polars do relatorio de fronteira.

Substitui a consulta `fronteira.sql` por joins entre datasets ja
materializados no registry centralizado.
"""
from __future__ import annotations

import logging
from datetime import date
from datetime import datetime
from pathlib import Path

import polars as pl

from utilitarios.dataset_registry import carregar_lazyframe
from utilitarios.dataset_registry import criar_metadata
from utilitarios.dataset_registry import registrar_dataset

logger = logging.getLogger(__name__)

DATA_INICIO_PADRAO = date(2020, 1, 1)


def _normalizar_data_limite(data_limite_processamento: str | date | datetime | None) -> date:
    """Aceita os formatos usados pelo contrato SQL legado e padroniza para date."""

    data_atual = date.today()
    if data_limite_processamento is None:
        return data_atual
    if isinstance(data_limite_processamento, datetime):
        return min(data_limite_processamento.date(), data_atual)
    if isinstance(data_limite_processamento, date):
        return min(data_limite_processamento, data_atual)
    return min(datetime.strptime(str(data_limite_processamento), "%d/%m/%Y").date(), data_atual)


def _montar_tipo_operacao(cnpj: str) -> pl.Expr:
    """Replica o `CASE` da SQL legado sem alterar a classificacao funcional."""

    return (
        pl.when(
            (pl.col("co_emitente").cast(pl.Utf8, strict=False) == cnpj)
            & (pl.col("co_tp_nf").cast(pl.Int64, strict=False) == 1)
        )
        .then(pl.lit("1 - SAIDA"))
        .when(
            (pl.col("co_emitente").cast(pl.Utf8, strict=False) == cnpj)
            & (pl.col("co_tp_nf").cast(pl.Int64, strict=False) == 0)
        )
        .then(pl.lit("0 - ENTRADA"))
        .when(
            (pl.col("co_destinatario").cast(pl.Utf8, strict=False) == cnpj)
            & (pl.col("co_tp_nf").cast(pl.Int64, strict=False) == 1)
        )
        .then(pl.lit("0 - ENTRADA"))
        .when(
            (pl.col("co_destinatario").cast(pl.Utf8, strict=False) == cnpj)
            & (pl.col("co_tp_nf").cast(pl.Int64, strict=False) == 0)
        )
        .then(pl.lit("1 - SAIDA"))
        .otherwise(pl.lit("INDEFINIDO"))
    )


def composicao_fronteira(
    cnpj: str,
    data_limite_processamento: str | date | datetime | None = None,
) -> pl.DataFrame:
    """
    Reproduz a SQL `fronteira.sql` com filtros temporais e join local.

    O corte por data continua opcional para manter compatibilidade com o
    contrato atual do backend, que nem sempre envia o parametro.
    """

    resultado_nfe = carregar_lazyframe(cnpj, "nfe_base")
    resultado_sitafe = carregar_lazyframe(cnpj, "sitafe_calculo_item")

    if resultado_nfe is None or resultado_sitafe is None:
        faltantes = [
            nome
            for nome, resultado in {
                "nfe_base": resultado_nfe,
                "sitafe_calculo_item": resultado_sitafe,
            }.items()
            if resultado is None
        ]
        raise FileNotFoundError(f"Datasets base ausentes para composicao fronteira: {faltantes}")

    data_limite = _normalizar_data_limite(data_limite_processamento)
    nfe_lazyframe, _ = resultado_nfe
    sitafe_lazyframe, _ = resultado_sitafe

    nfe_preparada = (
        nfe_lazyframe
        .filter(
            pl.all_horizontal(
                [
                    pl.any_horizontal(
                        [
                            pl.col("co_emitente").cast(pl.Utf8, strict=False) == cnpj,
                            pl.col("co_destinatario").cast(pl.Utf8, strict=False) == cnpj,
                        ]
                    ),
                    pl.col("dhemi").cast(pl.Datetime, strict=False).dt.date() >= pl.lit(DATA_INICIO_PADRAO),
                    pl.col("dhemi").cast(pl.Datetime, strict=False).dt.date() <= pl.lit(data_limite),
                ]
            )
        )
        .select(
            [
                _montar_tipo_operacao(cnpj).alias("tipo_operacao"),
                pl.col("chave_acesso").cast(pl.Utf8, strict=False),
                pl.col("seq_nitem").cast(pl.Utf8, strict=False).alias("num_item"),
                pl.col("prod_cprod").cast(pl.Utf8, strict=False).alias("cod_item"),
                pl.col("prod_xprod").cast(pl.Utf8, strict=False).alias("desc_item"),
                pl.col("prod_ncm").cast(pl.Utf8, strict=False).alias("ncm"),
                pl.col("prod_cest").cast(pl.Utf8, strict=False).alias("cest"),
                pl.col("prod_qcom").cast(pl.Float64, strict=False).alias("qtd_comercial"),
                pl.col("prod_vprod").cast(pl.Float64, strict=False).alias("valor_produto"),
                pl.col("icms_vbcst").cast(pl.Float64, strict=False).alias("bc_icms_st_destacado"),
                pl.col("icms_vicmsst").cast(pl.Float64, strict=False).alias("icms_st_destacado"),
            ]
        )
    )

    return (
        nfe_preparada
        .join(
            sitafe_lazyframe.select(
                [
                    pl.col("it_nu_chave_acesso").cast(pl.Utf8, strict=False),
                    pl.col("it_nu_item").cast(pl.Utf8, strict=False),
                    pl.col("it_co_sefin").cast(pl.Utf8, strict=False).alias("co_sefin"),
                    pl.col("it_co_rotina_calculo").cast(pl.Utf8, strict=False).alias("cod_rotina_calculo"),
                    pl.col("it_vl_icms").cast(pl.Float64, strict=False).alias("valor_icms_fronteira"),
                ]
            ),
            left_on=["chave_acesso", "num_item"],
            right_on=["it_nu_chave_acesso", "it_nu_item"],
            how="inner",
        )
        .sort(["chave_acesso", "num_item"])
        .collect()
    )


def atualizar_composicao_fronteira(
    cnpj: str,
    data_limite_processamento: str | date | datetime | None = None,
) -> Path | None:
    """Materializa a composicao de fronteira no registry canonico."""

    try:
        data_limite = _normalizar_data_limite(data_limite_processamento)
        dataframe = composicao_fronteira(cnpj, data_limite_processamento=data_limite)
        metadata = criar_metadata(
            cnpj=cnpj,
            dataset_id="composicao_fronteira",
            sql_id="fronteira.sql",
            parametros={
                "modo": "composicao_polars",
                "data_inicio": DATA_INICIO_PADRAO.isoformat(),
                "data_limite_processamento": data_limite.isoformat(),
                "datasets_origem": ["nfe_base", "sitafe_calculo_item"],
                "tabela_origem": [
                    "BI.FATO_NFE_DETALHE",
                    "SITAFE.SITAFE_NFE_CALCULO_ITEM",
                ],
            },
            linhas=dataframe.height,
        )
        return registrar_dataset(cnpj, "composicao_fronteira", dataframe, metadata=metadata)
    except Exception:
        logger.exception("Erro na composicao fronteira para %s", cnpj)
        return None
