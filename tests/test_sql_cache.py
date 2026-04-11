from pathlib import Path

from utilitarios.ler_sql import ler_sql
from utilitarios.schema_registry import SchemaRegistry
from utilitarios.sql_cache import SqlCatalogCache


def test_sql_cache_hits_after_first_read(tmp_path: Path):
    sql_path = tmp_path / "consulta.sql"
    sql_path.write_text("SELECT * FROM tabela;\n", encoding="utf-8")

    cache = SqlCatalogCache(cache_root=tmp_path / "cache", ttl_seconds=3600)
    first = cache.get("consulta.sql", sql_path)
    assert first is None

    cache.set("consulta.sql", sql_path, "SELECT * FROM tabela")
    second = cache.get("consulta.sql", sql_path)
    assert second == "SELECT * FROM tabela"

    stats = cache.stats()
    assert stats["hits"] >= 1
    assert stats["memory_entries"] == 1


def test_ler_sql_reads_and_cleans_content(tmp_path: Path):
    sql_path = tmp_path / "arquivo.sql"
    sql_path.write_text("\n SELECT 1; \n", encoding="utf-8")

    result = ler_sql(sql_path, use_cache=False)
    assert result == "SELECT 1"


def test_schema_registry_records_and_diffs(tmp_path: Path):
    registry = SchemaRegistry(tmp_path / "schema_registry.json")

    first = registry.record_schema("tb_documentos", {"cnpj": "Utf8", "valor": "Float64"})
    assert first.version == 1

    diff_same = registry.diff_latest("tb_documentos", {"cnpj": "Utf8", "valor": "Float64"})
    assert diff_same == {"added": [], "removed": [], "type_changed": []}

    diff_changed = registry.diff_latest(
        "tb_documentos",
        {"cnpj": "Utf8", "valor": "Int64", "chave": "Utf8"},
    )
    assert diff_changed == {
        "added": ["chave"],
        "removed": [],
        "type_changed": ["valor"],
    }
