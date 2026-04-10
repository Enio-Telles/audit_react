from pathlib import Path

from backend.routers import fiscal_fiscalizacao as fiscalizacao



def test_dados_cadastrais_path_prioriza_localizador(monkeypatch, tmp_path: Path):
    esperado = tmp_path / "canonico" / "dados_cadastrais.parquet"
    esperado.parent.mkdir(parents=True, exist_ok=True)
    esperado.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(fiscalizacao, "locate_dataset", lambda cnpj, dataset_id, *fallbacks: esperado)

    resolved = fiscalizacao._dados_cadastrais_path("12345678901234")
    assert resolved == esperado
