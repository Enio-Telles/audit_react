"""Compatibility wrapper with canonical materialization for fatores_conversao."""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from transformacao.rastreabilidade_produtos.fatores_conversao import *  # noqa: F401,F403
from transformacao.rastreabilidade_produtos.fatores_conversao import calcular_fatores_conversao as _calcular_fatores_conversao
from utilitarios.dataset_registry import criar_metadata, registrar_dataset
from utilitarios.project_paths import CNPJ_ROOT


def _materializar_dataset_canonico(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj
    legado = pasta_cnpj / "analises" / "produtos" / f"fatores_conversao_{cnpj}.parquet"
    if not legado.exists():
        return False
    df = pl.read_parquet(legado)
    metadata = criar_metadata(
        cnpj=cnpj,
        dataset_id="fatores_conversao",
        linhas=df.height,
        parametros={"origem_legada": str(legado)},
    )
    return registrar_dataset(cnpj, "fatores_conversao", df, metadata=metadata) is not None


def calcular_fatores_conversao(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    ok = _calcular_fatores_conversao(cnpj_limpo, pasta_cnpj)
    if not ok:
        return False
    return _materializar_dataset_canonico(cnpj_limpo, pasta_cnpj)


__all__ = [name for name in globals() if not name.startswith("_")]
