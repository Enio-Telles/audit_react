from pathlib import Path

from backend.routers import fiscal_dataset_locator as locator


class _Localizado:
    def __init__(self, caminho: Path):
        self.caminho = caminho


def test_locate_dataset_prioriza_catalogo(monkeypatch, tmp_path: Path):
    esperado = tmp_path / "catalogo" / "dados.parquet"
    esperado.parent.mkdir(parents=True, exist_ok=True)
    esperado.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(locator, "encontrar_dataset", lambda cnpj, dataset_id: _Localizado(esperado))

    resolved = locator.locate_dataset("123", "mov_estoque", tmp_path / "fallback.parquet")
    assert resolved == esperado


def test_locate_dataset_usa_fallback_quando_catalogo_nao_acha(monkeypatch, tmp_path: Path):
    fallback = tmp_path / "fallback.parquet"
    fallback.write_text("ok", encoding="utf-8")

    monkeypatch.setattr(locator, "encontrar_dataset", lambda cnpj, dataset_id: None)

    resolved = locator.locate_dataset("123", "mov_estoque", fallback)
    assert resolved == fallback
