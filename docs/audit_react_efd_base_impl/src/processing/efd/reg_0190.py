"""
Loader para REG_0190.

Unidades declaradas na EFD.
"""

from __future__ import annotations

import os
from typing import Optional

import polars as pl


def _base_path() -> str:
    return os.path.join(os.getcwd(), "data", "raw", "efd", "reg_0190")


def load_reg_0190(ano: Optional[int] = None, mes: Optional[int] = None) -> pl.DataFrame:
    base = _base_path()
    if not os.path.isdir(base):
        raise FileNotFoundError(f"Base path not found: {base}")
    if ano is not None:
        base = os.path.join(base, f"ano={ano}")
        if mes is not None:
            base = os.path.join(base, f"mes={mes:02d}")
    return pl.scan_parquet(os.path.join(base, "*.parquet")).collect()
