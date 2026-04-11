from pathlib import Path

from backend.routers import fiscal_catalog_inspector as inspector


class _Localizado:
    def __init__(self, caminho: Path, metadata=None, reutilizado=False, dataset_id="mov_estoque"):
        self.caminho = caminho
        self.metadata = metadata
        self.reutilizado = reutilizado
        self.dataset_id = dataset_id


def test_availability_for_cnpj_usa_diagnostico(monkeypatch):
    monkeypatch.setattr(inspector, "diagnosticar_disponibilidade", lambda cnpj: [{"dataset_id": "mov_estoque", "disponivel": True}])
    payload = inspector.availability_for_cnpj("12.345.678/0001-90")
    assert payload["cnpj"] == "12345678000190"
    assert payload["items"][0]["dataset_id"] == "mov_estoque"


def test_inspect_dataset_retorna_preview(monkeypatch, tmp_path: Path):
    arquivo = tmp_path / "mov_estoque.parquet"
    arquivo.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(inspector, "encontrar_dataset", lambda cnpj, dataset_id: _Localizado(arquivo, metadata={"fonte": "teste"}, dataset_id="mov_estoque"))
    monkeypatch.setattr(inspector, "probe_parquet", lambda path: {"status": "materializado", "rows": 2})

    class _DF:
        columns = ["id", "valor"]
        def head(self, n):
            return self
        def to_dicts(self):
            return [{"id": 1, "valor": 10.0}, {"id": 2, "valor": 20.0}]

    monkeypatch.setattr(inspector, "read_materialized_frame", lambda path: _DF())

    payload = inspector.inspect_dataset("12345678000190", "mov_estoque", limit=5)
    assert payload["dataset_id"] == "mov_estoque"
    assert payload["probe"]["status"] == "materializado"
    assert len(payload["preview"]) == 2
    assert payload["metadata"]["fonte"] == "teste"
