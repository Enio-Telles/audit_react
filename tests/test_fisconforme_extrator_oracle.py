from pathlib import Path
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.fisconforme.extrator_oracle import _montar_dataframe_lote


def test_montar_dataframe_lote_normaliza_coluna_mista_sem_forcar_texto():
    dataframe = _montar_dataframe_lote(
        lote=[
            (1, "Empresa A"),
            ("2", "Empresa B"),
            (None, "Empresa C"),
        ],
        colunas=["codigo", "nome"],
    )

    assert dataframe.schema["codigo"] == pl.String
    assert dataframe.to_dicts() == [
        {"codigo": "1", "nome": "Empresa A"},
        {"codigo": "2", "nome": "Empresa B"},
        {"codigo": None, "nome": "Empresa C"},
    ]


def test_montar_dataframe_lote_forca_texto_quando_solicitado():
    dataframe = _montar_dataframe_lote(
        lote=[
            (1, "Empresa A"),
            (2, None),
        ],
        colunas=["codigo", "nome"],
        forcar_texto=True,
    )

    assert dataframe.schema["codigo"] == pl.String
    assert dataframe.to_dicts() == [
        {"codigo": "1", "nome": "Empresa A"},
        {"codigo": "2", "nome": None},
    ]
