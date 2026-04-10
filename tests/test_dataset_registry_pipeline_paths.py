from pathlib import Path

from utilitarios import dataset_registry as registry


def test_obter_caminho_canonico_tb_documentos(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(registry, "CNPJ_ROOT", tmp_path)
    caminho = registry.obter_caminho("12345678901234", "tb_documentos")
    assert caminho == tmp_path / "12345678901234" / "analises" / "produtos" / "tb_documentos_12345678901234.parquet"


def test_obter_caminho_canonico_aba_mensal(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(registry, "CNPJ_ROOT", tmp_path)
    caminho = registry.obter_caminho("12345678901234", "aba_mensal")
    assert caminho == tmp_path / "12345678901234" / "analises" / "produtos" / "aba_mensal_12345678901234.parquet"


def test_obter_caminho_canonico_malhas(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(registry, "CNPJ_ROOT", tmp_path)
    caminho = registry.obter_caminho("12345678901234", "malhas")
    assert caminho == tmp_path / "12345678901234" / "fisconforme" / "malhas.parquet"
