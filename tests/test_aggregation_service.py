from pathlib import Path
import sys

import polars as pl


sys.path.insert(0, str(Path("src").resolve()))

import interface_grafica.services.aggregation_service as aggregation_service_module
from interface_grafica.services.aggregation_service import ServicoAgregacao


def test_recalcular_valores_totais_persiste_medias_sem_duplicar_descricao(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(aggregation_service_module, "registrar_evento_performance", lambda *args, **kwargs: None)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descr_padrao": ["Produto A", "Produto B"],
            "lista_chave_produto": [["desc_1", "desc_2"], ["desc_3"]],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_descricao": ["desc_1", "desc_2", "desc_3"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO A", "PRODUTO B"],
        }
    ).write_parquet(pasta_prod / f"descricao_produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "descricao": ["Produto A", "Produto B"],
            "compras": [100.0, 0.0],
            "qtd_compras": [10.0, 0.0],
            "vendas": [60.0, 0.0],
            "qtd_vendas": [3.0, 0.0],
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")

    servico = ServicoAgregacao()

    assert servico.recalcular_valores_totais(cnpj, reprocessar_referencias=False)

    df = pl.read_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet").sort("id_agrupado")
    row_a = df.row(0, named=True)
    row_b = df.row(1, named=True)

    assert row_a["id_agrupado"] == "AGR_1"
    assert row_a["total_compras"] == 100.0
    assert row_a["qtd_compras_total"] == 10.0
    assert row_a["preco_medio_compra"] == 10.0
    assert row_a["total_vendas"] == 60.0
    assert row_a["qtd_vendas_total"] == 3.0
    assert row_a["preco_medio_venda"] == 20.0
    assert row_a["total_entradas"] == 100.0
    assert row_a["total_saidas"] == 60.0
    assert row_a["total_movimentacao"] == 160.0

    assert row_b["id_agrupado"] == "AGR_2"
    assert row_b["total_compras"] == 0.0
    assert row_b["qtd_compras_total"] == 0.0
    assert row_b["preco_medio_compra"] is None
    assert row_b["total_vendas"] == 0.0
    assert row_b["qtd_vendas_total"] == 0.0
    assert row_b["preco_medio_venda"] is None
    assert row_b["total_entradas"] == 0.0
    assert row_b["total_saidas"] == 0.0
    assert row_b["total_movimentacao"] == 0.0


def test_agregar_linhas_recalcula_padroes_com_colunas_item_unidades(monkeypatch, tmp_path: Path):
    cnpj = "12345678000190"
    pasta_prod = tmp_path / cnpj / "analises" / "produtos"
    pasta_prod.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(aggregation_service_module, "CNPJ_ROOT", tmp_path)
    monkeypatch.setattr(aggregation_service_module, "registrar_evento_performance", lambda *args, **kwargs: None)

    pl.DataFrame(
        {
            "id_agrupado": ["AGR_1", "AGR_2"],
            "descr_padrao": ["Produto A", "Produto A"],
            "ncm_padrao": ["1000", "1000"],
            "cest_padrao": ["2000", "2000"],
            "gtin_padrao": ["789", "789"],
            "lista_co_sefin": [["2290"], ["2290"]],
            "co_sefin_padrao": ["2290", "2290"],
            "lista_unidades": [["UN"], ["CX"]],
            "co_sefin_divergentes": [False, False],
            "fontes": [["c170"], ["nfe"]],
            "lista_chave_produto": [["desc_1"], ["desc_2"]],
        }
    ).write_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")

    pl.DataFrame(
        {
            "id_descricao": ["desc_1", "desc_2"],
            "descricao_normalizada": ["PRODUTO A", "PRODUTO A"],
            "descricao": ["Produto A", "Produto A"],
            "lista_desc_compl": [["Garrafa 1L"], ["Caixa 12x1L"]],
            "lista_ncm": [["1000"], ["1000"]],
            "lista_cest": [["2000"], ["2000"]],
            "lista_gtin": [["789"], ["789"]],
            "fontes": [["c170"], ["nfe"]],
        }
    ).write_parquet(pasta_prod / f"descricao_produtos_{cnpj}.parquet")

    pl.DataFrame(
        {
            "descricao": ["Produto A", "Produto A"],
            "ncm": ["1000", "1000"],
            "cest": ["2000", "2000"],
            "gtin": ["789", "789"],
            "co_sefin_item": ["2290", "2290"],
            "fontes": [["c170"], ["nfe"]],
        }
    ).write_parquet(pasta_prod / f"item_unidades_{cnpj}.parquet")

    servico = ServicoAgregacao()
    monkeypatch.setattr(servico, "recalcular_valores_totais", lambda *args, **kwargs: True)
    monkeypatch.setattr(servico, "recalcular_produtos_final", lambda *args, **kwargs: True)
    monkeypatch.setattr(servico, "recalcular_referencias_produtos", lambda *args, **kwargs: True)

    resultado = servico.agregar_linhas(cnpj, ["AGR_1", "AGR_2"])

    assert resultado["success"] is True

    df = pl.read_parquet(pasta_prod / f"produtos_agrupados_{cnpj}.parquet")
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["descr_padrao"] == "Produto A"
    assert row["ncm_padrao"] == "1000"
    assert row["cest_padrao"] == "2000"
    assert row["gtin_padrao"] == "789"
    assert row["lista_ncm"] == ["1000"]
    assert row["lista_cest"] == ["2000"]
    assert row["lista_gtin"] == ["789"]
    assert row["lista_descricoes"] == ["Caixa 12x1L", "Garrafa 1L", "Produto A"]
