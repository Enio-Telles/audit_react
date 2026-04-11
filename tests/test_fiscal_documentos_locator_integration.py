from pathlib import Path

from backend.routers import fiscal_documentos as documentos


def test_find_nfe_prioriza_localizador(monkeypatch, tmp_path: Path):
    esperado = tmp_path / "canonico" / "nfe.parquet"
    esperado.parent.mkdir(parents=True, exist_ok=True)
    esperado.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(documentos, "locate_dataset", lambda cnpj, dataset_id, *fallbacks: esperado)

    resolved = documentos._find_nfe("12345678901234")
    assert resolved == esperado
