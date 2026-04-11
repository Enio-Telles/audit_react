from pathlib import Path

import polars as pl

import transformacao.bloco_h as bloco_h


def test_materializar_bloco_h_registra_dataset_canonico(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(bloco_h, "CNPJ_ROOT", tmp_path)
    cnpj = "12345678901234"
    pasta = tmp_path / cnpj / "analises" / "produtos"
    pasta.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"item": [1]}).write_parquet(pasta / f"bloco_h_{cnpj}.parquet")

    capturado = {}

    def fake_registrar(cnpj: str | None, dataset_id: str, dataframe: pl.DataFrame, *, metadata=None):
        capturado["cnpj"] = cnpj
        capturado["dataset_id"] = dataset_id
        capturado["rows"] = dataframe.height
        return tmp_path / "out"

    monkeypatch.setattr(bloco_h, "registrar_dataset", fake_registrar)

    assert bloco_h.materializar_bloco_h(cnpj) is True
    assert capturado["cnpj"] == cnpj
    assert capturado["dataset_id"] == "bloco_h"
    assert capturado["rows"] == 1
