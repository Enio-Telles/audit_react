"""Loader para C170."""
from __future__ import annotations

import polars as pl

from ._loader_common import load_partitioned_efd


def load_c170(ano: int | None = None, mes: int | None = None) -> pl.DataFrame:
    return load_partitioned_efd("c170", ano=ano, mes=mes)
