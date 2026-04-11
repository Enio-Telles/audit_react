"""
Utilitarios compartilhados para construcao da camada base EFD.
"""
from __future__ import annotations

from pathlib import Path
from typing import Mapping, Sequence

import polars as pl


COMMON_ALIASES: dict[str, tuple[str, ...]] = {
    "cnpj": ("cnpj", "cnpj_raiz", "cnpj_emit", "emit_cnpj", "co_cnpj_cpf"),
    "periodo": ("periodo", "mes_ref", "aaaamm", "competencia"),
    "chv_nfe": ("chv_nfe", "chave_nfe", "id_doc"),
    "num_doc": ("num_doc", "numero_doc", "documento"),
    "cod_item": ("cod_item", "item_id", "produto_id"),
    "num_item": ("num_item", "item_num", "seq_item"),
    "dt_doc": ("dt_doc", "dt_e_s", "data_doc", "data_emissao", "dt_doc_raw", "dt_e_s_raw"),
    "dt_inv": ("dt_inv", "data_inv", "data_inventario", "dt_inv_raw"),
    "qtd": ("qtd", "quantidade"),
    "vl_item": ("vl_item", "valor_item"),
}


def pick_existing(columns: Sequence[str], aliases: Sequence[str]) -> str | None:
    lower_map = {column.lower(): column for column in columns}
    for alias in aliases:
        if alias.lower() in lower_map:
            return lower_map[alias.lower()]
    return None


def alias_columns(df: pl.DataFrame | pl.LazyFrame, mapping: Mapping[str, Sequence[str]]) -> pl.LazyFrame:
    lf = df.lazy() if isinstance(df, pl.DataFrame) else df
    schema_names = lf.collect_schema().names()
    expressions: list[pl.Expr] = []
    used: set[str] = set()

    for canonical, aliases in mapping.items():
        column = pick_existing(schema_names, aliases)
        if column is not None:
            used.add(column)
            expressions.append(pl.col(column).alias(canonical))

    for name in schema_names:
        if name not in used:
            expressions.append(pl.col(name))
    return lf.select(expressions)


def cast_if_exists(lf: pl.LazyFrame, casts: Mapping[str, pl.DataType]) -> pl.LazyFrame:
    schema_names = set(lf.collect_schema().names())
    expressions = [
        pl.col(name).cast(dtype, strict=False).alias(name)
        for name, dtype in casts.items()
        if name in schema_names
    ]
    return lf.with_columns(expressions) if expressions else lf


def add_lineage_columns(
    lf: pl.LazyFrame,
    dataset_id: str,
    camada: str,
    upstream_datasets: Sequence[str],
) -> pl.LazyFrame:
    return lf.with_columns(
        [
            pl.lit(dataset_id).alias("dataset_id"),
            pl.lit(camada).alias("camada"),
            pl.lit(",".join(upstream_datasets)).alias("upstream_datasets"),
        ]
    )


def write_partitioned_parquet(
    df: pl.DataFrame,
    target_dir: str | Path,
    partition_cols: Sequence[str] | None = None,
) -> Path:
    target = Path(target_dir)
    target.mkdir(parents=True, exist_ok=True)

    if not partition_cols:
        output = target / "part-000.parquet"
        df.write_parquet(output, compression="zstd")
        return output

    for row in df.select(list(partition_cols)).unique().to_dicts():
        part_df = df
        part_path = target
        for column in partition_cols:
            value = row.get(column)
            part_df = part_df.filter(pl.col(column) == value)
            part_path = part_path / f"{column}={value}"
        part_path.mkdir(parents=True, exist_ok=True)
        part_df.write_parquet(part_path / "part-000.parquet", compression="zstd")
    return target


def normalize_period_column(lf: pl.LazyFrame) -> pl.LazyFrame:
    schema_names = lf.collect_schema().names()
    if "periodo" in schema_names:
        return lf.with_columns(pl.col("periodo").cast(pl.Utf8))
    if "dt_doc" in schema_names:
        return lf.with_columns(
            pl.col("dt_doc").cast(pl.Utf8).str.replace_all("-", "").str.slice(0, 6).alias("periodo")
        )
    if "dt_inv" in schema_names:
        return lf.with_columns(
            pl.col("dt_inv").cast(pl.Utf8).str.replace_all("-", "").str.slice(0, 6).alias("periodo")
        )
    return lf
