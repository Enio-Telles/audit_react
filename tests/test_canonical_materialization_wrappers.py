from pathlib import Path

import polars as pl

import transformacao.c170_xml as c170_wrapper
import transformacao.produtos_final_v2 as produtos_wrapper


CNPJ = "12345678901234"


def test_c170_wrapper_materializa_dataset_canonico(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(c170_wrapper, "CNPJ_ROOT", tmp_path)
    capturado = {}

    def fake_impl(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
        pasta = pasta_cnpj or (tmp_path / cnpj)
        destino = pasta / "arquivos_parquet"
        destino.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"a": [1]}).write_parquet(destino / f"c170_xml_{cnpj}.parquet")
        return True

    def fake_registrar(cnpj: str | None, dataset_id: str, dataframe: pl.DataFrame, *, metadata=None):
        capturado["cnpj"] = cnpj
        capturado["dataset_id"] = dataset_id
        capturado["rows"] = dataframe.height
        return tmp_path / "out"

    monkeypatch.setattr(c170_wrapper, "_gerar_c170_xml", fake_impl)
    monkeypatch.setattr(c170_wrapper, "registrar_dataset", fake_registrar)

    assert c170_wrapper.gerar_c170_xml(CNPJ) is True
    assert capturado["cnpj"] == CNPJ
    assert capturado["dataset_id"] == "c170_xml"
    assert capturado["rows"] == 1


def test_produtos_wrapper_materializa_produtos_agrupados_e_final(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(produtos_wrapper, "CNPJ_ROOT", tmp_path)
    capturados: list[str] = []

    def fake_impl(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
        pasta = pasta_cnpj or (tmp_path / cnpj)
        destino = pasta / "analises" / "produtos"
        destino.mkdir(parents=True, exist_ok=True)
        pl.DataFrame({"id_agrupado": ["x"]}).write_parquet(destino / f"produtos_agrupados_{cnpj}.parquet")
        pl.DataFrame({"id_agrupado": ["x"], "descricao_final": ["ABC"]}).write_parquet(destino / f"produtos_final_{cnpj}.parquet")
        return True

    def fake_registrar(cnpj: str | None, dataset_id: str, dataframe: pl.DataFrame, *, metadata=None):
        capturados.append(dataset_id)
        return tmp_path / dataset_id

    monkeypatch.setattr(produtos_wrapper, "_gerar_produtos_final", fake_impl)
    monkeypatch.setattr(produtos_wrapper, "registrar_dataset", fake_registrar)

    assert produtos_wrapper.gerar_produtos_final(CNPJ) is True
    assert "produtos_agrupados" in capturados
    assert "produtos_final" in capturados
