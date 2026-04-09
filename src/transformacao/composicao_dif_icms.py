"""
Composicao Polars da diferenca de ICMS entre XML e EFD.

A composicao substitui a consulta `dif_ICMS_NFe_EFD.sql` usando apenas
datasets ja materializados no registry centralizado.
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

DATA_INICIO_PADRAO = date(2020, 1, 1)
STATUS_AUDITORIA = "Debito a Menor na EFD"


def _coluna_data_emissao(coluna: str) -> pl.Expr:
    """Normaliza a data de emissao para comparacoes consistentes."""

    return pl.col(coluna).cast(pl.Datetime, strict=False).dt.date()


def _coluna_decimal(coluna: str) -> pl.Expr:
    """Padroniza colunas numericas da composicao sem forcar erro em valores nulos."""

    return pl.col(coluna).cast(pl.Float64, strict=False)


def _carregar_documentos_saida(
    lazyframe: pl.LazyFrame,
    *,
    modelo: str,
    exigir_tp_nf_saida: bool,
) -> pl.LazyFrame:
    """Aplica os mesmos filtros funcionais da SQL original para XML de saida."""

    filtros = [
        pl.col("infprot_cstat").cast(pl.Utf8, strict=False).is_in(["100", "150"]),
        pl.col("seq_nitem").cast(pl.Utf8, strict=False) == "1",
        _coluna_data_emissao("dhemi") >= pl.lit(DATA_INICIO_PADRAO),
        _coluna_data_emissao("dhemi") <= pl.lit(date.today()),
    ]

    if exigir_tp_nf_saida:
        filtros.append(pl.col("co_tp_nf").cast(pl.Int64, strict=False) == 1)

    return (
        lazyframe
        .filter(pl.all_horizontal(filtros))
        .select(
            [
                pl.col("chave_acesso").cast(pl.Utf8, strict=False),
                pl.col("ide_serie").cast(pl.Utf8, strict=False).alias("serie"),
                pl.col("nnf").cast(pl.Utf8, strict=False).alias("num_doc"),
                _coluna_decimal("tot_vnf").alias("valor_total_nfe"),
                _coluna_decimal("tot_vicms").alias("icms_destacado_nfe"),
                pl.col("dhemi").cast(pl.Datetime, strict=False).alias("data_emissao"),
                pl.lit(modelo).alias("modelo"),
            ]
        )
    )


def _carregar_efd_saida(lazyframe: pl.LazyFrame) -> pl.LazyFrame:
    """Mantem o filtro de saida da EFD, aceitando schema legado ou atual."""

    colunas = set(lazyframe.collect_schema().names())
    if "ind_oper" in colunas:
        filtro_saida = pl.col("ind_oper").cast(pl.Utf8, strict=False) == "1"
    else:
        filtro_saida = pl.col("ind_oper_desc").cast(pl.Utf8, strict=False).str.starts_with("1")

    coluna_data_referencia = "dt_doc" if "dt_doc" in colunas else None
    filtros = [filtro_saida]
    if coluna_data_referencia is not None:
        filtros.append(
            pl.col(coluna_data_referencia).cast(pl.Date, strict=False) >= pl.lit(DATA_INICIO_PADRAO)
        )
        filtros.append(
            pl.col(coluna_data_referencia).cast(pl.Date, strict=False) <= pl.lit(date.today())
        )

    return (
        lazyframe
        .filter(pl.all_horizontal(filtros))
        .select(
            [
                pl.col("chv_nfe").cast(pl.Utf8, strict=False),
                _coluna_decimal("vl_icms").alias("icms_escriturado_efd"),
            ]
        )
    )


def composicao_dif_icms(cnpj: str) -> pl.DataFrame:
    """
    Reproduz em Polars a malha de debito a menor da SQL `dif_ICMS_NFe_EFD.sql`.

    Regras preservadas:
    - considera apenas XMLs autorizados;
    - usa somente o item `seq_nitem = 1` para capturar totais por documento;
    - compara contra saidas escrituradas na EFD;
    - retorna apenas notas com ICMS escriturado menor que o destacado.
    """

    resultado_nfe = carregar_lazyframe(cnpj, "nfe_base")
    resultado_nfce = carregar_lazyframe(cnpj, "nfce_base")
    resultado_efd = carregar_lazyframe(cnpj, "efd_c100")

    if resultado_nfe is None or resultado_efd is None:
        faltantes = [
            nome
            for nome, resultado in {
                "nfe_base": resultado_nfe,
                "efd_c100": resultado_efd,
            }.items()
            if resultado is None
        ]
        raise FileNotFoundError(f"Datasets base ausentes para composicao dif_icms: {faltantes}")

    nfe_lazyframe, _ = resultado_nfe
    efd_lazyframe, _ = resultado_efd

    documentos_saida: list[pl.LazyFrame] = [
        _carregar_documentos_saida(
            nfe_lazyframe,
            modelo="NF-e (Mod 55)",
            exigir_tp_nf_saida=True,
        )
    ]

    if resultado_nfce is not None:
        nfce_lazyframe, _ = resultado_nfce
        documentos_saida.append(
            _carregar_documentos_saida(
                nfce_lazyframe,
                modelo="NFC-e (Mod 65)",
                exigir_tp_nf_saida=False,
            )
        )

    xml_saida = pl.concat(documentos_saida, how="vertical_relaxed")
    efd_saida = _carregar_efd_saida(efd_lazyframe)

    return (
        xml_saida
        .join(
            efd_saida,
            left_on="chave_acesso",
            right_on="chv_nfe",
            how="inner",
        )
        .with_columns(
            [
                pl.col("icms_escriturado_efd").fill_null(0.0),
                (
                    pl.col("icms_destacado_nfe").fill_null(0.0)
                    - pl.col("icms_escriturado_efd").fill_null(0.0)
                ).alias("diferenca_icms_nao_debitado"),
                pl.lit(STATUS_AUDITORIA).alias("status_auditoria"),
            ]
        )
        .filter(pl.col("icms_escriturado_efd") < pl.col("icms_destacado_nfe"))
        .sort(["data_emissao", "num_doc"])
        .collect()
    )


def atualizar_composicao_dif_icms(cnpj: str) -> Path | None:
    """Materializa o parquet composto no registry canonico."""

    try:
        dataframe = composicao_dif_icms(cnpj)
        metadata = criar_metadata(
            cnpj=cnpj,
            dataset_id="dif_icms_nfe_efd",
            sql_id="dif_ICMS_NFe_EFD.sql",
            parametros={
                "modo": "composicao_polars",
                "data_inicio": DATA_INICIO_PADRAO.isoformat(),
                "data_limite_processamento": date.today().isoformat(),
                "datasets_origem": ["nfe_base", "nfce_base", "efd_c100"],
                "tabela_origem": [
                    "BI.FATO_NFE_DETALHE",
                    "BI.FATO_NFCE_DETALHE",
                    "SPED.REG_C100",
                ],
            },
            linhas=dataframe.height,
        )
        return registrar_dataset(cnpj, "dif_icms_nfe_efd", dataframe, metadata=metadata)
    except Exception:
        logger.exception("Erro na composicao dif_icms para %s", cnpj)
        return None
