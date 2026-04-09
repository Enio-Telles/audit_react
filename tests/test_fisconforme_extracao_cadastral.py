from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.fisconforme import extracao_cadastral
from interface_grafica.fisconforme.extracao_cadastral import (
    converter_linha_oracle_em_dicionario,
    exportar_cache_completo,
    obter_estatisticas_cache,
)


def test_converter_linha_oracle_em_dicionario_preserva_valores_unicos():
    resultado = converter_linha_oracle_em_dicionario(
        ["RAZAO_SOCIAL", "IE", "ATIVO"],
        ("Empresa Teste", 12345, None),
    )

    assert resultado == {
        "RAZAO_SOCIAL": "Empresa Teste",
        "IE": 12345,
        "ATIVO": None,
    }


def test_exportar_cache_completo_copia_parquet_existente(tmp_path, monkeypatch):
    caminho_cache = tmp_path / "dados_cadastrais.parquet"
    pl.DataFrame({"CNPJ": ["123"], "RAZAO_SOCIAL": ["Empresa Teste"]}).write_parquet(caminho_cache)
    monkeypatch.setattr(extracao_cadastral, "PARQUET_CADASTRAIS", caminho_cache)
    monkeypatch.setattr(extracao_cadastral, "PARQUET_DIR", tmp_path)

    caminho_exportado = tmp_path / "saida" / "exportado.parquet"
    resultado = exportar_cache_completo(caminho_exportado)

    assert resultado == caminho_exportado
    assert caminho_exportado.exists()
    assert pl.read_parquet(caminho_exportado).to_dicts() == [
        {"CNPJ": "123", "RAZAO_SOCIAL": "Empresa Teste"}
    ]


def test_obter_estatisticas_cache_usa_contagem_e_schema_do_scan(tmp_path, monkeypatch):
    caminho_cache = tmp_path / "dados_cadastrais.parquet"
    pl.DataFrame(
        {"CNPJ": ["123", "456"], "RAZAO_SOCIAL": ["Empresa A", "Empresa B"]}
    ).write_parquet(caminho_cache)
    monkeypatch.setattr(extracao_cadastral, "PARQUET_CADASTRAIS", caminho_cache)

    estatisticas = obter_estatisticas_cache()

    assert estatisticas["total"] == 2
    assert estatisticas["colunas"] == ["CNPJ", "RAZAO_SOCIAL"]
