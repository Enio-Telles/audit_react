"""Loader para REG_0200."""
from __future__ import annotations

import polars as pl

from ._loader_common import load_partitioned_efd


def load_reg_0200(ano: int | None = None, mes: int | None = None) -> pl.DataFrame:
    return load_partitioned_efd("reg_0200", ano=ano, mes=mes)
