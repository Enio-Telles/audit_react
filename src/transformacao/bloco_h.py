"""Materialização canônica para o artefato legado de bloco_h."""
from __future__ import annotations

import re
from pathlib import Path

import polars as pl

from utilitarios.dataset_registry import criar_metadata, registrar_dataset
from utilitarios.project_paths import CNPJ_ROOT


def _localizar_bloco_h_legado(cnpj: str, pasta_cnpj: Path) -> Path | None:
    candidatos = [
        pasta_cnpj / "analises" / "produtos" / f"bloco_h_{cnpj}.parquet",
        pasta_cnpj / "arquivos_parquet" / f"bloco_h_{cnpj}.parquet",
        pasta_cnpj / "arquivos_parquet" / "fiscal" / "efd" / f"bloco_h_{cnpj}.parquet",
    ]
    for caminho in candidatos:
        if caminho.exists():
            return caminho
    return None


def materializar_bloco_h(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj_limpo = re.sub(r"\D", "", cnpj or "")
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj_limpo

    legado = _localizar_bloco_h_legado(cnpj_limpo, pasta_cnpj)
    if legado is None:
        return False

    df = pl.read_parquet(legado)
    metadata = criar_metadata(
        cnpj=cnpj_limpo,
        dataset_id="bloco_h",
        linhas=df.height,
        parametros={"origem_legada": str(legado)},
    )
    return registrar_dataset(cnpj_limpo, "bloco_h", df, metadata=metadata) is not None


gerar_bloco_h = materializar_bloco_h
