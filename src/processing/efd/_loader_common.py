from __future__ import annotations

from pathlib import Path

import polars as pl

from utilitarios.project_paths import DATA_ROOT


def load_partitioned_efd(name: str, ano: int | None = None, mes: int | None = None) -> pl.DataFrame:
    base = DATA_ROOT / "raw" / "efd" / name
    if not base.exists():
        raise FileNotFoundError(f"Base path not found: {base}")

    if ano is not None:
        base = base / f"ano={ano}"
        if mes is not None:
            base = base / f"mes={mes:02d}"

    if not base.exists():
        raise FileNotFoundError(f"Partition path not found: {base}")

    parquet_files = sorted(base.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in: {base}")

    return pl.scan_parquet([str(path) for path in parquet_files]).collect()
