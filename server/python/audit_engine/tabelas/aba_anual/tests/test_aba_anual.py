"""Testes isolados da tabela aba_anual do audit_engine, garantindo blindagem tributária para o ST."""

from pathlib import Path
from unittest.mock import patch
import pytest
import polars as pl
from datetime import datetime

from audit_engine.tabelas.aba_anual.gerador import gerar_aba_anual
from audit_engine.contratos.base import ContratoTabela, ColunaSchema, TipoColuna

# Contrato genérico
CONTRATO_MOCK = ContratoTabela(
    nome="aba_anual",
    descricao="Mock",
    modulo="teste",
    funcao="gerar_aba_anual",
    dependencias=["mov_estoque"],
    saida="aba_anual.parquet",
    colunas=[
        ColunaSchema("id_agrupado", TipoColuna.STRING, "ID"),
        ColunaSchema("descricao", TipoColuna.STRING, "Desc"),
        ColunaSchema("ano", TipoColuna.INT, "Ano"),
        ColunaSchema("estoque_inicial", TipoColuna.FLOAT, ""),
        ColunaSchema("entradas", TipoColuna.FLOAT, ""),
        ColunaSchema("saidas", TipoColuna.FLOAT, ""),
        ColunaSchema("estoque_final", TipoColuna.FLOAT, ""),
        ColunaSchema("saldo_final", TipoColuna.FLOAT, ""),
        ColunaSchema("entradas_desacob", TipoColuna.FLOAT, ""),
        ColunaSchema("saidas_desacob", TipoColuna.FLOAT, ""),
        ColunaSchema("estoque_final_desacob", TipoColuna.FLOAT, ""),
        ColunaSchema("pme", TipoColuna.FLOAT, ""),
        ColunaSchema("pms", TipoColuna.FLOAT, ""),
        ColunaSchema("ICMS_saidas_desac", TipoColuna.FLOAT, "", obrigatoria=False),
        ColunaSchema("ICMS_estoque_desac", TipoColuna.FLOAT, "", obrigatoria=False),
        ColunaSchema("tem_st_ano", TipoColuna.BOOL, "", obrigatoria=False),
    ]
)

@pytest.fixture
def base_mov_estoque() -> pl.DataFrame:
    """Base com omissão de saídas e valores para pme/pms."""
    return pl.DataFrame({
        "id_agrupado": ["PROD_ST", "PROD_ST", "PROD_NO_ST", "PROD_NO_ST"],
        "data": ["2026-01-10", "2026-02-15", "2026-03-10", "2026-04-15"],
        "ano": [2026, 2026, 2026, 2026],
        "tipo": ["1 - ENTRADA", "2 - SAIDAS", "1 - ENTRADA", "2 - SAIDAS"],
        "q_conv": [100.0, 50.0, 200.0, 100.0],
        "valor_total": [1000.0, 600.0, 2000.0, 1200.0],
        "saldo_estoque_anual": [100.0, 50.0, 200.0, 100.0],
        # Forçar uma omissão de saída para o cálculo de ICMS
        "__qtd_decl_final_audit__": [0.0, 0.0, 0.0, 0.0],
        "entr_desac_anual": [0.0, 0.0, 0.0, 0.0],
        "excluir_estoque": [False, False, False, False],
        "descricao": ["Prod ST", "Prod ST", "Prod No ST", "Prod No ST"]
    })

@pytest.fixture
def mock_matriz_st() -> pl.DataFrame:
    """Matriz PROD_ST como ST em 2026."""
    return pl.DataFrame({
        "it_co_sefin": ["PROD_ST", "PROD_TARDIO"],
        "it_in_st": ["S", "S"],
        "it_da_inicio": [datetime(2025, 1, 1), datetime(2028, 1, 1)],
        "it_da_final": [datetime(2027, 12, 31), datetime(2028, 12, 31)]
    })

def run_gerador_with_mocks(base_mov, matriz_st_mock, tmp_path):
    dir_parquets = tmp_path / "parquets"
    dir_parquets.mkdir()
    base_mov.write_parquet(dir_parquets / "mov_estoque.parquet")
    arquivo_saida = tmp_path / "saida.parquet"

    original_read_parquet = pl.read_parquet
    
    # Patch exists e read_parquet
    with patch("audit_engine.tabelas.aba_anual.gerador.Path.exists") as mock_exists, \
         patch("audit_engine.tabelas.aba_anual.gerador.pl.read_parquet") as mock_read:
        
        def exists_side_effect(self):
            return "mov_estoque.parquet" in str(self) or "sitafe_produto_sefin_aux.parquet" in str(self)

        def read_side_effect(path, **kwargs):
            if "sitafe_produto_sefin_aux.parquet" in str(path):
                return matriz_st_mock
            return original_read_parquet(path, **kwargs)

        mock_exists.side_effect = exists_side_effect
        mock_read.side_effect = read_side_effect
        
        gerar_aba_anual(tmp_path, dir_parquets, arquivo_saida, CONTRATO_MOCK)
    
    return original_read_parquet(arquivo_saida)

def test_abatimento_real_st_zerando_imposto(base_mov_estoque, mock_matriz_st, tmp_path):
    """Cenário 1: Abatimento ST zera imposto."""
    df_gerado = run_gerador_with_mocks(base_mov_estoque, mock_matriz_st, tmp_path)
    
    df_prod_st = df_gerado.filter(pl.col("id_agrupado") == "PROD_ST")
    assert not df_prod_st.is_empty(), "PROD_ST não encontrado no resultado."
    assert df_prod_st["tem_st_ano"][0] is True, "Flag 'tem_st_ano' deveria ser True para PROD_ST."
    assert df_prod_st["ICMS_saidas_desac"][0] == 0.0, "ICMS para PROD_ST deveria ser zerado."

def test_sem_abatimento_calcula_imposto(base_mov_estoque, mock_matriz_st, tmp_path):
    """Cenário 2: Não-ST calcula imposto."""
    df_gerado = run_gerador_with_mocks(base_mov_estoque, mock_matriz_st, tmp_path)

    df_prod_no_st = df_gerado.filter(pl.col("id_agrupado") == "PROD_NO_ST")
    assert not df_prod_no_st.is_empty(), "PROD_NO_ST não encontrado no resultado."
    assert df_prod_no_st["tem_st_ano"][0] is False, "Flag 'tem_st_ano' deveria ser False para PROD_NO_ST."
    # Forçar saldo final < apurado para gerar saidas_desacob
    assert df_prod_no_st["saidas_desacob"][0] > 0, "Saídas desacobertadas não calculadas para PROD_NO_ST."
    assert df_prod_no_st["ICMS_saidas_desac"][0] > 0.0, "ICMS para PROD_NO_ST não deveria ser zerado."

def test_mock_fallback_sem_matriz(base_mov_estoque, tmp_path):
    """Cenário 3: Fallback sem matriz ST."""
    dir_parquets = tmp_path / "parquets"
    dir_parquets.mkdir()
    base_mov_estoque.write_parquet(dir_parquets / "mov_estoque.parquet")
    arquivo_saida = tmp_path / "saida.parquet"

    original_read_parquet = pl.read_parquet
    with patch("audit_engine.tabelas.aba_anual.gerador.Path.exists") as mock_exists:
        # Apenas mov_estoque existe
        mock_exists.side_effect = lambda self: "mov_estoque.parquet" in str(self)
        
        gerar_aba_anual(tmp_path, dir_parquets, arquivo_saida, CONTRATO_MOCK)
        
        df_gerado = original_read_parquet(arquivo_saida)

    assert all(v is False for v in df_gerado["tem_st_ano"].to_list()), "Todas as flags 'tem_st_ano' deveriam ser False no modo fallback."
    assert all(v > 0.0 for v in df_gerado.filter(pl.col("saidas_desacob") > 0)["ICMS_saidas_desac"]), "ICMS deveria ser calculado para todos os produtos com saídas desacobertadas."
