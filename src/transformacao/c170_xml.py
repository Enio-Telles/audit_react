"""Compatibility wrapper with canonical materialization for c170_xml."""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from transformacao.movimentacao_estoque_pkg.c170_xml import *  # noqa: F401,F403
from transformacao.movimentacao_estoque_pkg.c170_xml import gerar_c170_xml as _gerar_c170_xml
from utilitarios.dataset_registry import criar_metadata, registrar_dataset
from utilitarios.project_paths import CNPJ_ROOT


def _materializar_dataset_canonico(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj
    legado = pasta_cnpj / "arquivos_parquet" / f"c170_xml_{cnpj}.parquet"
    if not legado.exists():
        return False
    df = pl.read_parquet(legado)
    metadata = criar_metadata(
        cnpj=cnpj,
        dataset_id="c170_xml",
        linhas=df.height,
        parametros={"origem_legada": str(legado)},
    )
    return registrar_dataset(cnpj, "c170_xml", df, metadata=metadata) is not None


def gerar_c170_xml(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    ok = _gerar_c170_xml(cnpj_limpo, pasta_cnpj)
    if not ok:
        return False
    return _materializar_dataset_canonico(cnpj_limpo, pasta_cnpj)


__all__ = [name for name in globals() if not name.startswith("_")]
