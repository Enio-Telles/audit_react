from __future__ import annotations

from pathlib import Path
import sys

import openpyxl
import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from utilitarios.exportar_excel_adaptado import exportar_excel


def test_exportar_excel_aplica_texto_para_identificadores_e_data_para_iso(tmp_path):
    df = pl.DataFrame(
        {
            "cnpj": [12345678000190],
            "chave_acesso": ["00123456789012345678901234567890123456789012"],
            "data_emissao": ["2026-04-09"],
            "valor_total": [1234.5],
        }
    )

    arquivo = exportar_excel(
        df=df,
        nome_base="dados_cadastrais_teste",
        diretorio_saida=tmp_path,
        nome_aba="Dados",
    )

    assert arquivo is not None

    workbook = openpyxl.load_workbook(arquivo)
    worksheet = workbook["Dados"]

    assert worksheet["A2"].value == "12345678000190"
    assert worksheet["A2"].data_type == "s"
    assert worksheet["B2"].value == "00123456789012345678901234567890123456789012"
    assert worksheet["B2"].data_type == "s"
    assert worksheet["C2"].number_format == "dd/mm/yyyy"
    assert worksheet["C2"].value is not None
    assert worksheet["D2"].number_format == "#,##0.00"
    assert worksheet["D2"].value == 1234.5
