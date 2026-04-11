from pathlib import Path

import polars as pl

from utilitarios.delta_lake import (
    get_delta_runtime_config,
    resolve_delta_target,
    resolve_storage_format,
)


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


def test_resolve_storage_format_defaults_to_parquet(monkeypatch, tmp_path: Path):
    monkeypatch.delenv("DATA_LAKE_FORMAT", raising=False)
    monkeypatch.delenv("DELTA_ENABLED_TABLES", raising=False)
    target = tmp_path / "tb_documentos.parquet"
    assert resolve_storage_format(target) == "parquet"


def test_resolve_storage_format_selective_delta(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("DATA_LAKE_FORMAT", "delta")
    monkeypatch.setenv("DELTA_ENABLED_TABLES", "tb_documentos,movimentacao_estoque")
    assert resolve_storage_format(tmp_path / "tb_documentos.parquet") == "delta"
    assert resolve_storage_format(tmp_path / "calculos_mensais.parquet") == "parquet"


def test_get_delta_runtime_config(monkeypatch):
    monkeypatch.setenv("DATA_LAKE_FORMAT", "delta")
    monkeypatch.setenv("DELTA_WRITE_MODE", "append")
    monkeypatch.setenv("DELTA_ENABLED_TABLES", "tb_documentos,calculos_mensais")
    cfg = get_delta_runtime_config()
    assert cfg == {
        "global_format": "delta",
        "delta_enabled": True,
        "delta_write_mode": "append",
        "delta_enabled_tables": ["calculos_mensais", "tb_documentos"],
    }
