from pathlib import Path

from backend.routers.fiscal_storage import is_delta_materialized, resolve_materialized_path


def test_resolve_materialized_path_prefers_existing_delta_dir(tmp_path: Path):
    parquet_path = tmp_path / "tb_documentos_123.parquet"
    delta_dir = tmp_path / "tb_documentos_123"
    delta_dir.mkdir()

    assert resolve_materialized_path(parquet_path) == delta_dir
    assert is_delta_materialized(parquet_path) is True


def test_resolve_materialized_path_keeps_existing_parquet(tmp_path: Path):
    parquet_path = tmp_path / "tb_documentos_123.parquet"
    parquet_path.write_text("dummy", encoding="utf-8")

    assert resolve_materialized_path(parquet_path) == parquet_path
    assert is_delta_materialized(parquet_path) is False
