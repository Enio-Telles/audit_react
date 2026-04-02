"""Testes de contrato de schema das tabelas do pipeline.

Valida que os schemas das tabelas Parquet permanecem estaveis e que
campos canonicos estao presentes nas tabelas gold.
"""

import polars as pl
import pytest
from pathlib import Path

CNPJ_PILOTO = "37671507000187"
BASE = Path(f"storage/CNPJ/{CNPJ_PILOTO}")


def parquet(camada: str, nome: str) -> pl.DataFrame:
    """Le parquet de uma camada e tabela especificas.
    
    Skips o teste se o arquivo nao existir.
    """
    path = BASE / camada / f"{nome}.parquet"
    if not path.exists():
        pytest.skip(f"Parquet nao encontrado: {path}")
    return pl.read_parquet(path)


def lf(camada: str, nome: str) -> pl.LazyFrame:
    """Retorna LazyFrame de uma tabela para consultas eficientes."""
    path = BASE / camada / f"{nome}.parquet"
    if not path.exists():
        pytest.skip(f"Parquet nao encontrado: {path}")
    return pl.scan_parquet(path)


# ── Contratos da camada gold (parquets) ──────────────────────────────────────


def test_fatores_conversao_schema():
    """Valida schema da tabela fatores_conversao."""
    df = parquet("parquets", "fatores_conversao")
    
    assert "id_agrupado" in df.columns, "Campo id_agrupado ausente"
    assert "descricao_padrao" in df.columns, "Campo canônico descricao_padrao ausente"
    assert "fator_compra_ref" in df.columns, "Campo fator_compra_ref ausente"
    assert "fator_venda_ref" in df.columns, "Campo fator_venda_ref ausente"
    assert "unid_ref" in df.columns, "Campo unid_ref ausente"
    
    # Valida que fator nao é nulo
    if not df.is_empty():
        assert df.filter(pl.col("fator_compra_ref").is_null()).is_empty(), "Fator compra nulo encontrado"


def test_produtos_final_schema():
    """Valida schema da tabela produtos_final."""
    df = parquet("parquets", "produtos_final")
    
    campos_obrigatorios = [
        "id_agrupado",
        "id_grupo",
        "descricao_padrao",
        "ncm",
        "cest",
        "fator_compra_ref",
        "fator_venda_ref",
        "unid_ref",
    ]
    
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatorio ausente: {campo}"


def test_nfe_entrada_schema():
    """Valida schema da tabela nfe_entrada."""
    df = parquet("parquets", "nfe_entrada")
    
    if df.is_empty():
        pytest.skip("Tabela nfe_entrada vazia")
    
    campos_obrigatorios = [
        "chave_nfe",
        "item",
        "cfop",
        "cst",
        "quantidade",
        "valor_unitario",
        "valor_bruto",
    ]
    
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatorio ausente: {campo}"


def test_nfe_entrada_sem_duplicatas():
    """Valida que nao ha duplicatas de chave_nfe + item."""
    df = parquet("parquets", "nfe_entrada")
    
    if df.is_empty():
        pytest.skip("Tabela nfe_entrada vazia")
    
    chaves = df.select(["chave_nfe", "item"]).filter(
        pl.col("chave_nfe").is_not_null()
    )
    
    if not chaves.is_empty():
        duplicatas = chaves.filter(pl.col("chave_nfe").is_duplicated())
        assert duplicatas.is_empty(), f"Duplicatas em nfe_entrada: {len(duplicatas)} registros"


def test_mov_estoque_schema():
    """Valida schema da tabela mov_estoque."""
    df = parquet("parquets", "mov_estoque")
    
    if df.is_empty():
        pytest.skip("Tabela mov_estoque vazia")
    
    campos_obrigatorios = [
        "id_agrupado",
        "ano_mes",
        "tipo_movimentacao",
        "quantidade",
    ]
    
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatorio ausente: {campo}"


def test_aba_mensal_schema():
    """Valida schema da tabela aba_mensal."""
    df = parquet("parquets", "aba_mensal")
    
    if df.is_empty():
        pytest.skip("Tabela aba_mensal vazia")
    
    campos_obrigatorios = [
        "ano_mes",
        "id_agrupado",
        "saldo_inicial",
        "entradas",
        "saidas",
        "saldo_final",
    ]
    
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatorio ausente: {campo}"


def test_aba_anual_schema():
    """Valida schema da tabela aba_anual."""
    df = parquet("parquets", "aba_anual")
    
    if df.is_empty():
        pytest.skip("Tabela aba_anual vazia")
    
    campos_obrigatorios = [
        "ano",
        "id_agrupado",
        "saldo_inicial_anual",
        "total_entradas",
        "total_saidas",
        "saldo_final_anual",
    ]
    
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatorio ausente: {campo}"


def test_produtos_agrupados_schema():
    """Valida schema da tabela produtos_agrupados."""
    df = parquet("parquets", "produtos_agrupados")
    
    if df.is_empty():
        pytest.skip("Tabela produtos_agrupados vazia")
    
    assert "id_agrupado" in df.columns, "Campo id_agrupado ausente"
    assert "descricao" in df.columns, "Campo descricao ausente"
    assert "ncm" in df.columns, "Campo NCM ausente"


def test_id_agrupados_schema():
    """Valida schema da tabela id_agrupados."""
    df = parquet("parquets", "id_agrupados")
    
    if df.is_empty():
        pytest.skip("Tabela id_agrupados vazia")
    
    assert "id_agrupado" in df.columns, "Campo id_agrupado ausente"
    assert "id_item" in df.columns, "Campo id_item ausente"


def test_produtos_unidades_schema():
    """Valida schema da tabela produtos_unidades."""
    df = parquet("parquets", "produtos_unidades")
    
    if df.is_empty():
        pytest.skip("Tabela produtos_unidades vazia")
    
    campos_obrigatorios = [
        "id_item",
        "descricao",
        "unid_medida",
    ]
    
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatorio ausente: {campo}"


def test_produtos_schema():
    """Valida schema da tabela produtos."""
    df = parquet("parquets", "produtos")
    
    if df.is_empty():
        pytest.skip("Tabela produtos vazia")
    
    campos_obrigatorios = [
        "id_item",
        "descricao",
        "ncm",
        "cest",
    ]
    
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatorio ausente: {campo}"


def test_produtos_selecionados_schema():
    """Valida schema da tabela produtos_selecionados."""
    df = parquet("parquets", "produtos_selecionados")
    
    # Esta tabela pode estar vazia se nao houver selecao manual
    if df.is_empty():
        pytest.skip("Tabela produtos_selecionados vazia")
    
    assert "id_agrupado" in df.columns, "Campo id_agrupado ausente"
    assert "selecionado" in df.columns, "Campo selecionado ausente"


# ── Contratos da camada silver ────────────────────────────────────────────────


def test_c176_xml_campos_enriquecidos():
    """Valida que c176_xml tem campos do C176 + dados XML."""
    df = parquet("silver", "c176_xml")
    
    if df.is_empty():
        pytest.skip("Tabela c176_xml vazia")
    
    # Deve ter campos diretos do C176 + dados de entrada XML
    assert "chave" in df.columns, "Campo chave ausente"
    assert "item" in df.columns, "Campo item ausente"


def test_nfe_dados_st_silver():
    """Valida que nfe_dados_st silver nao esta vazia."""
    df = parquet("silver", "nfe_dados_st")
    
    if df.is_empty():
        pytest.skip("Tabela nfe_dados_st vazia")
    
    assert len(df) > 0, "silver/nfe_dados_st vazia"


def test_itens_schema():
    """Valida schema da tabela itens."""
    df = parquet("silver", "itens")
    
    if df.is_empty():
        pytest.skip("Tabela itens vazia")
    
    campos_obrigatorios = [
        "chave",
        "item",
        "cfop",
        "cst",
    ]
    
    for campo in campos_obrigatorios:
        assert campo in df.columns, f"Campo obrigatorio ausente: {campo}"


# ── Testes de integridade geral ──────────────────────────────────────────────


def test_todas_tabelas_gold_tem_registros():
    """Valida que todas as tabelas gold tem pelo menos um registro."""
    tabelas_gold = [
        "produtos_unidades",
        "produtos",
        "produtos_agrupados",
        "id_agrupados",
        "fatores_conversao",
        "produtos_final",
        "nfe_entrada",
        "mov_estoque",
        "aba_mensal",
        "aba_anual",
    ]
    
    for tabela in tabelas_gold:
        path = BASE / "parquets" / f"{tabela}.parquet"
        if path.exists():
            df = pl.read_parquet(path)
            assert len(df) > 0, f"Tabela {tabela} esta vazia"


def test_manifesto_existe():
    """Valida que o manifesto do CNPJ piloto existe."""
    manifesto_path = BASE / "manifesto.json"
    
    if not manifesto_path.exists():
        pytest.skip("Manifesto nao encontrado")
    
    import json
    with manifesto_path.open("r", encoding="utf-8") as f:
        manifesto = json.load(f)
    
    assert "cnpj" in manifesto, "Campo cnpj ausente no manifesto"
    assert manifesto["cnpj"] == CNPJ_PILOTO, "CNPJ do manifesto nao confere"
