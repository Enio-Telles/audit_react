from datetime import date
from pathlib import Path
import sys

import polars as pl
import pytest

sys.path.insert(0, str(Path("src/transformacao").resolve()))

from calculos_anuais import calcular_aba_anual_dataframe


def test_calcular_aba_anual_zera_apenas_icms_saidas_quando_st_ativo_no_ano():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_1", "id_1", "id_1", "id_1"],
            "co_sefin_agr": ["1001"] * 4,
            "descr_padrao": ["Produto ST"] * 4,
            "unid_ref": ["UN"] * 4,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "2 - SAIDAS", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 3, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 3, 1), date(2024, 6, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3, 4],
            "q_conv": [10.0, 5.0, 4.0, 8.0],
            "preco_item": [0.0, 50.0, 48.0, 0.0],
            "Vl_item": [0.0, 50.0, 48.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0, 0.0],
            "saldo_estoque_anual": [10.0, 15.0, 11.0, 11.0],
            "it_pc_interna": [17.0, 17.0, 17.0, 17.0],
            "dev_simples": [False, False, False, False],
            "excluir_estoque": [False, False, False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "co_sefin_agr": ["1001"],
            "ano": [2024],
            "ST": ["['S' de 01/01/2024 até 31/12/2024]"],
            "__tem_st_ano__": [True],
            "__aliq_ref__": [17.0],
        }
    )

    result = calcular_aba_anual_dataframe(df, df_aux_st)
    row = result.row(0, named=True)

    assert row["ST"] == "['S' de 01/01/2024 até 31/12/2024]"
    assert row["unid_ref"] == "UN"
    assert row["pme"] == 10.0
    assert row["pms"] == 12.0
    assert row["saidas_desacob"] == 3.0
    assert row["ICMS_saidas_desac"] == 0.0
    assert row["ICMS_estoque_desac"] == pytest.approx(22.44, rel=1e-4)


def test_calcular_aba_anual_usa_fallback_pme_mva_quando_nao_ha_pms():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_2", "id_2", "id_2"],
            "co_sefin_agr": ["2002"] * 3,
            "descr_padrao": ["Produto sem PMS"] * 3,
            "unid_ref": ["CX"] * 3,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [0.0, 10.0, 7.0],
            "preco_item": [0.0, 100.0, 0.0],
            "Vl_item": [0.0, 100.0, 0.0],
            "entr_desac_anual": [0.0, 0.0, 0.0],
            "saldo_estoque_anual": [0.0, 10.0, 10.0],
            "it_pc_interna": [18.0, 18.0, 18.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "co_sefin_agr": ["2002"],
            "ano": [2024],
            "ST": ["['N' de 01/01/2024 até 31/12/2024]"],
            "__tem_st_ano__": [False],
            "__aliq_ref__": [18.0],
        }
    )

    result = calcular_aba_anual_dataframe(df, df_aux_st)
    row = result.row(0, named=True)

    assert row["unid_ref"] == "CX"
    assert row["pme"] == 10.0
    assert row["pms"] == 0.0
    assert row["saidas_desacob"] == 3.0
    assert row["estoque_final_desacob"] == 10.0
    assert row["ICMS_saidas_desac"] == pytest.approx(7.02, rel=1e-4)
    assert row["ICMS_estoque_desac"] == pytest.approx(23.4, rel=1e-4)


def test_calcular_aba_anual_inclui_entradas_desacob_em_saidas_calculadas():
    df = pl.DataFrame(
        {
            "id_agrupado": ["id_3", "id_3", "id_3"],
            "co_sefin_agr": ["3003"] * 3,
            "descr_padrao": ["Produto com entrada desacob"] * 3,
            "unid_ref": ["UN"] * 3,
            "Tipo_operacao": ["0 - ESTOQUE INICIAL", "1 - ENTRADA", "3 - ESTOQUE FINAL"],
            "Dt_doc": [date(2024, 1, 1), date(2024, 5, 1), date(2024, 12, 31)],
            "Dt_e_s": [date(2024, 1, 1), date(2024, 5, 1), date(2024, 12, 31)],
            "ordem_operacoes": [1, 2, 3],
            "q_conv": [10.0, 5.0, 8.0],
            "preco_item": [0.0, 50.0, 0.0],
            "Vl_item": [0.0, 50.0, 0.0],
            "entr_desac_anual": [0.0, 2.0, 0.0],
            "saldo_estoque_anual": [10.0, 15.0, 15.0],
            "it_pc_interna": [17.0, 17.0, 17.0],
            "dev_simples": [False, False, False],
            "excluir_estoque": [False, False, False],
        }
    )
    df_aux_st = pl.DataFrame(
        {
            "co_sefin_agr": ["3003"],
            "ano": [2024],
            "ST": [""],
            "__tem_st_ano__": [False],
            "__aliq_ref__": [17.0],
        }
    )

    result = calcular_aba_anual_dataframe(df, df_aux_st)
    row = result.row(0, named=True)

    assert row["entradas_desacob"] == 2.0
    assert row["saidas_calculadas"] == 9.0
