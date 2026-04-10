from pathlib import Path

import polars as pl

from utilitarios.delta_lake import resolve_delta_target


def test_resolve_delta_target_from_parquet_file(tmp_path: Path):
    source = tmp_path / "tb_documentos.parquet"
    assert resolve_delta_target(source) == tmp_path / "tb_documentos"


def test_resolve_delta_target_from_directory(tmp_path: Path):
    source = tmp_path / "tb_documentos"
    assert resolve_delta_target(source) == source


def test_delta_helper_accepts_lazyframe_name_resolution(tmp_path: Path):
    df = pl.DataFrame({"id": [1], "valor": [10.0]})
    lazy = df.lazy()
    assert lazy.collect().shape == (1, 2)
