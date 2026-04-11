"""
Builders da camada base EFD.

Implementa a próxima etapa da arquitetura:
- `base__efd__arquivos_validos`
- `base__efd__reg_c100_tipado`
- `base__efd__reg_c170_tipado`
- `base__efd__reg_c176_tipado`
- `base__efd__bloco_h_tipado`

Princípios adotados:
- partir sempre de Parquet já extraído;
- resolver alias e tipagem em Polars;
- filtrar cedo;
- preservar rastreabilidade;
- evitar agregações desnecessárias.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import polars as pl

from .base_utils import (
    COMMON_ALIASES,
    add_lineage_columns,
    alias_columns,
    cast_if_exists,
    normalize_period_column,
    write_partitioned_parquet,
)


def _scan(path: str | Path) -> pl.LazyFrame:
    path = Path(path)
    if path.is_dir():
        return pl.scan_parquet(str(path / "**" / "*.parquet"))
    return pl.scan_parquet(str(path))


def build_base_arquivos_validos(
    reg_0000_path: str | Path,
    output_dir: str | Path,
) -> Path:
    lf = _scan(reg_0000_path)
    lf = alias_columns(lf, COMMON_ALIASES)
    lf = normalize_period_column(lf)
    lf = cast_if_exists(
        lf,
        {
            "cnpj": pl.Utf8,
            "periodo": pl.Utf8,
            "dt_ini": pl.Utf8,
            "dt_fin": pl.Utf8,
        },
    )

    schema_names = lf.collect_schema().names()
    order_candidates = [c for c in ("dt_fin", "dt_ini", "periodo", "id_arquivo") if c in schema_names]
    if not order_candidates:
        raise ValueError("reg_0000 não possui colunas suficientes para versionamento.")

    # Heurística simples: manter a última linha por cnpj+periodo
    df = (
        lf.sort(order_candidates)
        .group_by([c for c in ("cnpj", "periodo") if c in schema_names])
        .tail(1)
        .pipe(lambda x: add_lineage_columns(
            x,
            dataset_id="base__efd__arquivos_validos",
            camada="base",
            upstream_datasets=("raw__efd__reg_0000",),
        ))
        .collect()
    )

    partitions = [c for c in ("cnpj", "periodo") if c in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_reg_c100_tipado(
    c100_path: str | Path,
    arquivos_validos_path: str | Path,
    output_dir: str | Path,
) -> Path:
    lf = alias_columns(_scan(c100_path), COMMON_ALIASES)
    lf = normalize_period_column(lf)
    lf = cast_if_exists(
        lf,
        {
            "cnpj": pl.Utf8,
            "periodo": pl.Utf8,
            "chv_nfe": pl.Utf8,
            "num_doc": pl.Utf8,
            "dt_doc": pl.Utf8,
            "vl_doc": pl.Float64,
        },
    )

    valid_lf = alias_columns(_scan(arquivos_validos_path), COMMON_ALIASES).select(
        [c for c in ("cnpj", "periodo") if c in alias_columns(_scan(arquivos_validos_path), COMMON_ALIASES).collect_schema().names()]
    ).unique()

    join_keys = [c for c in ("cnpj", "periodo") if c in lf.collect_schema().names() and c in valid_lf.collect_schema().names()]
    if join_keys:
        lf = lf.join(valid_lf, on=join_keys, how="inner")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_c100_tipado",
        camada="base",
        upstream_datasets=("base__efd__arquivos_validos", "raw__efd__reg_c100"),
    )
    df = lf.collect()
    partitions = [c for c in ("cnpj", "periodo") if c in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_reg_c170_tipado(
    c170_path: str | Path,
    c100_base_path: str | Path,
    output_dir: str | Path,
) -> Path:
    lf = alias_columns(_scan(c170_path), COMMON_ALIASES)
    lf = normalize_period_column(lf)
    lf = cast_if_exists(
        lf,
        {
            "cnpj": pl.Utf8,
            "periodo": pl.Utf8,
            "chv_nfe": pl.Utf8,
            "cod_item": pl.Utf8,
            "num_item": pl.Int64,
            "qtd": pl.Float64,
            "vl_item": pl.Float64,
        },
    )

    c100_lf = alias_columns(_scan(c100_base_path), COMMON_ALIASES).select(
        [c for c in ("cnpj", "periodo", "chv_nfe", "num_doc", "dt_doc") if c in alias_columns(_scan(c100_base_path), COMMON_ALIASES).collect_schema().names()]
    ).unique()

    join_keys = [c for c in ("cnpj", "periodo", "chv_nfe") if c in lf.collect_schema().names() and c in c100_lf.collect_schema().names()]
    if join_keys:
        lf = lf.join(c100_lf, on=join_keys, how="left")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_c170_tipado",
        camada="base",
        upstream_datasets=("base__efd__reg_c100_tipado", "raw__efd__reg_c170"),
    )
    df = lf.collect()
    partitions = [c for c in ("cnpj", "periodo") if c in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_reg_c176_tipado(
    c176_path: str | Path,
    c170_base_path: str | Path,
    output_dir: str | Path,
) -> Path:
    lf = alias_columns(_scan(c176_path), COMMON_ALIASES)
    lf = normalize_period_column(lf)
    lf = cast_if_exists(
        lf,
        {
            "cnpj": pl.Utf8,
            "periodo": pl.Utf8,
            "chv_nfe": pl.Utf8,
            "num_item": pl.Int64,
            "vl_item": pl.Float64,
        },
    )

    c170_lf = alias_columns(_scan(c170_base_path), COMMON_ALIASES).select(
        [c for c in ("cnpj", "periodo", "chv_nfe", "num_item", "cod_item") if c in alias_columns(_scan(c170_base_path), COMMON_ALIASES).collect_schema().names()]
    ).unique()

    join_keys = [c for c in ("cnpj", "periodo", "chv_nfe", "num_item") if c in lf.collect_schema().names() and c in c170_lf.collect_schema().names()]
    if join_keys:
        lf = lf.join(c170_lf, on=join_keys, how="left")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_c176_tipado",
        camada="base",
        upstream_datasets=("base__efd__reg_c170_tipado", "raw__efd__reg_c176"),
    )
    df = lf.collect()
    partitions = [c for c in ("cnpj", "periodo") if c in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_bloco_h_tipado(
    h005_path: str | Path,
    h010_path: str | Path,
    h020_path: str | Path,
    output_dir: str | Path,
) -> Path:
    h005 = normalize_period_column(alias_columns(_scan(h005_path), COMMON_ALIASES))
    h010 = normalize_period_column(alias_columns(_scan(h010_path), COMMON_ALIASES))
    h020 = normalize_period_column(alias_columns(_scan(h020_path), COMMON_ALIASES))

    h005 = cast_if_exists(h005, {"cnpj": pl.Utf8, "periodo": pl.Utf8, "dt_inv": pl.Utf8})
    h010 = cast_if_exists(h010, {"cnpj": pl.Utf8, "periodo": pl.Utf8, "dt_inv": pl.Utf8, "cod_item": pl.Utf8, "qtd": pl.Float64, "vl_item": pl.Float64})
    h020 = cast_if_exists(h020, {"cnpj": pl.Utf8, "periodo": pl.Utf8, "dt_inv": pl.Utf8, "cod_item": pl.Utf8})

    join_keys = [c for c in ("cnpj", "periodo", "dt_inv", "cod_item") if c in h010.collect_schema().names() and c in h020.collect_schema().names()]
    lf = h010
    if join_keys:
        lf = lf.join(h020, on=join_keys, how="left", suffix="_h020")

    head_keys = [c for c in ("cnpj", "periodo", "dt_inv") if c in lf.collect_schema().names() and c in h005.collect_schema().names()]
    if head_keys:
        lf = lf.join(h005, on=head_keys, how="left", suffix="_h005")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__bloco_h_tipado",
        camada="base",
        upstream_datasets=("raw__efd__reg_h005", "raw__efd__reg_h010", "raw__efd__reg_h020"),
    )
    df = lf.collect()
    partitions = [c for c in ("cnpj", "periodo") if c in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)
