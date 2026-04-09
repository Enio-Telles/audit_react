from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.sql_service import SqlService


def test_construir_dataframe_resultado_preserva_colunas_com_tipo_estavel():
    registros = [
        {"codigo": 1, "nome": "A"},
        {"codigo": 2, "nome": "B"},
    ]

    dataframe = SqlService.construir_dataframe_resultado(registros)

    assert dataframe.schema["codigo"] == pl.Int64
    assert dataframe.to_dicts() == registros


def test_construir_dataframe_resultado_normaliza_para_texto_coluna_com_tipo_misto():
    registros = [
        {"codigo": 1, "nome": "A", "ie": 123},
        {"codigo": "2", "nome": "B", "ie": "0000000123"},
        {"codigo": None, "nome": "C", "ie": None},
    ]

    dataframe = SqlService.construir_dataframe_resultado(registros)

    assert dataframe.schema["codigo"] == pl.String
    assert dataframe.schema["ie"] == pl.String
    assert dataframe.to_dicts() == [
        {"codigo": "1", "nome": "A", "ie": "123"},
        {"codigo": "2", "nome": "B", "ie": "0000000123"},
        {"codigo": None, "nome": "C", "ie": None},
    ]
