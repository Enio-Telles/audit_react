"""Loader para REG_0000."""
from __future__ import annotations

import polars as pl

from ._loader_common import load_partitioned_efd


def load_reg_0000(ano: int | None = None, mes: int | None = None) -> pl.DataFrame:
    return load_partitioned_efd("reg_0000", ano=ano, mes=mes)
