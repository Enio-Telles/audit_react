"""
Builders da camada base EFD.
"""
from __future__ import annotations

from pathlib import Path

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
    path_obj = Path(path)
    if path_obj.is_dir():
        parquet_files = sorted(path_obj.rglob("*.parquet"))
        if not parquet_files:
            raise FileNotFoundError(f"Nenhum parquet encontrado em {path_obj}")
        return pl.scan_parquet([str(item) for item in parquet_files])
    return pl.scan_parquet(str(path_obj))


def build_base_arquivos_validos(reg_0000_path: str | Path, output_dir: str | Path) -> Path:
    lf = alias_columns(_scan(reg_0000_path), COMMON_ALIASES)
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
    group_keys = [column for column in ("cnpj", "periodo") if column in schema_names]
    order_candidates = [column for column in ("dt_fin", "dt_ini", "periodo", "id_arquivo") if column in schema_names]
    if not group_keys or not order_candidates:
        raise ValueError("reg_0000 nao possui colunas suficientes para versionamento.")

    df = (
        lf.sort(order_candidates)
        .group_by(group_keys)
        .tail(1)
        .pipe(
            lambda item: add_lineage_columns(
                item,
                dataset_id="base__efd__arquivos_validos",
                camada="base",
                upstream_datasets=("raw__efd__reg_0000",),
            )
        )
        .collect()
    )

    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_reg_0190_tipado(
    reg_0190_path: str | Path,
    output_dir: str | Path,
) -> Path:
    lf = alias_columns(_scan(reg_0190_path), COMMON_ALIASES)
    lf = normalize_period_column(lf)
    lf = cast_if_exists(
        lf,
        {
            "cnpj": pl.Utf8,
            "periodo": pl.Utf8,
            "cod_unid": pl.Utf8,
            "descr": pl.Utf8,
        },
    )
    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_0190_tipado",
        camada="base",
        upstream_datasets=("raw__efd__reg_0190",),
    )
    df = lf.collect()
    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_reg_0200_tipado(
    reg_0200_path: str | Path,
    output_dir: str | Path,
) -> Path:
    lf = alias_columns(_scan(reg_0200_path), COMMON_ALIASES)
    lf = normalize_period_column(lf)
    lf = cast_if_exists(
        lf,
        {
            "cnpj": pl.Utf8,
            "periodo": pl.Utf8,
            "cod_item": pl.Utf8,
            "descr_item": pl.Utf8,
            "unid_inv": pl.Utf8,
            "cod_ncm": pl.Utf8,
            "cest": pl.Utf8,
            "cod_barra": pl.Utf8,
        },
    )
    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_0200_tipado",
        camada="base",
        upstream_datasets=("raw__efd__reg_0200",),
    )
    df = lf.collect()
    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_reg_0220_tipado(
    reg_0220_path: str | Path,
    output_dir: str | Path,
) -> Path:
    lf = alias_columns(_scan(reg_0220_path), COMMON_ALIASES)
    lf = normalize_period_column(lf)
    lf = cast_if_exists(
        lf,
        {
            "cnpj": pl.Utf8,
            "periodo": pl.Utf8,
            "cod_item": pl.Utf8,
            "unid_conv": pl.Utf8,
            "cod_unid_conv": pl.Utf8,
            "fat_conv": pl.Float64,
        },
    )
    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_0220_tipado",
        camada="base",
        upstream_datasets=("raw__efd__reg_0220",),
    )
    df = lf.collect()
    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
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

    valid_lf = alias_columns(_scan(arquivos_validos_path), COMMON_ALIASES)
    valid_schema = valid_lf.collect_schema().names()
    valid_lf = valid_lf.select([column for column in ("cnpj", "periodo") if column in valid_schema]).unique()

    join_keys = [column for column in ("cnpj", "periodo") if column in lf.collect_schema().names() and column in valid_lf.collect_schema().names()]
    if join_keys:
        lf = lf.join(valid_lf, on=join_keys, how="inner")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_c100_tipado",
        camada="base",
        upstream_datasets=("base__efd__arquivos_validos", "raw__efd__reg_c100"),
    )
    df = lf.collect()
    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
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

    c100_lf = alias_columns(_scan(c100_base_path), COMMON_ALIASES)
    c100_schema = c100_lf.collect_schema().names()
    c100_lf = c100_lf.select(
        [column for column in ("cnpj", "periodo", "chv_nfe", "num_doc", "dt_doc") if column in c100_schema]
    ).unique()

    join_keys = [
        column
        for column in ("cnpj", "periodo", "chv_nfe")
        if column in lf.collect_schema().names() and column in c100_lf.collect_schema().names()
    ]
    if join_keys:
        lf = lf.join(c100_lf, on=join_keys, how="left")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_c170_tipado",
        camada="base",
        upstream_datasets=("base__efd__reg_c100_tipado", "raw__efd__reg_c170"),
    )
    df = lf.collect()
    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_reg_c190_tipado(
    c190_path: str | Path,
    c100_base_path: str | Path,
    output_dir: str | Path,
) -> Path:
    lf = alias_columns(_scan(c190_path), COMMON_ALIASES)
    lf = normalize_period_column(lf)
    lf = cast_if_exists(
        lf,
        {
            "cnpj": pl.Utf8,
            "periodo": pl.Utf8,
            "chv_nfe": pl.Utf8,
            "cfop": pl.Utf8,
            "cst_icms": pl.Utf8,
            "vl_opr": pl.Float64,
            "vl_bc_icms": pl.Float64,
            "vl_icms": pl.Float64,
        },
    )

    c100_lf = alias_columns(_scan(c100_base_path), COMMON_ALIASES)
    c100_schema = c100_lf.collect_schema().names()
    c100_lf = c100_lf.select(
        [column for column in ("cnpj", "periodo", "chv_nfe", "num_doc", "dt_doc") if column in c100_schema]
    ).unique()

    join_keys = [
        column
        for column in ("cnpj", "periodo", "chv_nfe")
        if column in lf.collect_schema().names() and column in c100_lf.collect_schema().names()
    ]
    if join_keys:
        lf = lf.join(c100_lf, on=join_keys, how="left")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_c190_tipado",
        camada="base",
        upstream_datasets=("base__efd__reg_c100_tipado", "raw__efd__reg_c190"),
    )
    df = lf.collect()
    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
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

    c170_lf = alias_columns(_scan(c170_base_path), COMMON_ALIASES)
    c170_schema = c170_lf.collect_schema().names()
    c170_lf = c170_lf.select(
        [column for column in ("cnpj", "periodo", "chv_nfe", "num_item", "cod_item") if column in c170_schema]
    ).unique()

    join_keys = [
        column
        for column in ("cnpj", "periodo", "chv_nfe", "num_item")
        if column in lf.collect_schema().names() and column in c170_lf.collect_schema().names()
    ]
    if join_keys:
        lf = lf.join(c170_lf, on=join_keys, how="left")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__reg_c176_tipado",
        camada="base",
        upstream_datasets=("base__efd__reg_c170_tipado", "raw__efd__reg_c176"),
    )
    df = lf.collect()
    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)


def build_base_bloco_h_tipado(
    h005_path: str | Path,
    h010_path: str | Path,
    h020_path: str | Path,
    output_dir: str | Path,
) -> Path:
    h005 = cast_if_exists(normalize_period_column(alias_columns(_scan(h005_path), COMMON_ALIASES)), {"cnpj": pl.Utf8, "periodo": pl.Utf8, "dt_inv": pl.Utf8})
    h010 = cast_if_exists(normalize_period_column(alias_columns(_scan(h010_path), COMMON_ALIASES)), {"cnpj": pl.Utf8, "periodo": pl.Utf8, "dt_inv": pl.Utf8, "cod_item": pl.Utf8, "qtd": pl.Float64, "vl_item": pl.Float64})
    h020 = cast_if_exists(normalize_period_column(alias_columns(_scan(h020_path), COMMON_ALIASES)), {"cnpj": pl.Utf8, "periodo": pl.Utf8, "dt_inv": pl.Utf8, "cod_item": pl.Utf8})

    join_keys = [column for column in ("cnpj", "periodo", "dt_inv", "cod_item") if column in h010.collect_schema().names() and column in h020.collect_schema().names()]
    lf = h010
    if join_keys:
        lf = lf.join(h020, on=join_keys, how="left", suffix="_h020")

    head_keys = [column for column in ("cnpj", "periodo", "dt_inv") if column in lf.collect_schema().names() and column in h005.collect_schema().names()]
    if head_keys:
        lf = lf.join(h005, on=head_keys, how="left", suffix="_h005")

    lf = add_lineage_columns(
        lf,
        dataset_id="base__efd__bloco_h_tipado",
        camada="base",
        upstream_datasets=("raw__efd__reg_h005", "raw__efd__reg_h010", "raw__efd__reg_h020"),
    )
    df = lf.collect()
    partitions = [column for column in ("cnpj", "periodo") if column in df.columns]
    return write_partitioned_parquet(df, output_dir, partition_cols=partitions or None)
