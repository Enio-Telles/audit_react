"""Compatibility wrapper with canonical materialization for produtos_final."""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from transformacao.rastreabilidade_produtos.produtos_final_v2 import *  # noqa: F401,F403
from transformacao.rastreabilidade_produtos.produtos_final_v2 import (
    gerar_produtos_final as _gerar_produtos_final,
    produtos_agrupados as _produtos_agrupados,
)
from utilitarios.dataset_registry import criar_metadata, registrar_dataset
from utilitarios.project_paths import CNPJ_ROOT


def _materializar_saida(cnpj: str, dataset_id: str, legado: Path) -> bool:
    if not legado.exists():
        return False
    df = pl.read_parquet(legado)
    metadata = criar_metadata(
        cnpj=cnpj,
        dataset_id=dataset_id,
        linhas=df.height,
        parametros={"origem_legada": str(legado)},
    )
    return registrar_dataset(cnpj, dataset_id, df, metadata=metadata) is not None


def _materializar_datasets_canonicos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj
    pasta_produtos = pasta_cnpj / "analises" / "produtos"
    ok_agr = _materializar_saida(cnpj, "produtos_agrupados", pasta_produtos / f"produtos_agrupados_{cnpj}.parquet")
    ok_final = _materializar_saida(cnpj, "produtos_final", pasta_produtos / f"produtos_final_{cnpj}.parquet")
    return ok_agr and ok_final


def produtos_agrupados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    ok = _produtos_agrupados(cnpj_limpo, pasta_cnpj)
    if not ok:
        return False
    return _materializar_datasets_canonicos(cnpj_limpo, pasta_cnpj)


def gerar_produtos_final(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    ok = _gerar_produtos_final(cnpj_limpo, pasta_cnpj)
    if not ok:
        return False
    return _materializar_datasets_canonicos(cnpj_limpo, pasta_cnpj)


__all__ = [name for name in globals() if not name.startswith("_")]
