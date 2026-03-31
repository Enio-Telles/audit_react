from __future__ import annotations

from pathlib import Path

import polars as pl

import audit_engine  # noqa: F401
from audit_engine.contratos.base import CONTRATOS
from audit_engine.tabelas.aba_anual.gerador import gerar_aba_anual
from audit_engine.tabelas.aba_mensal.gerador import gerar_aba_mensal


def _escrever_parquet(caminho: Path, dados: list[dict]) -> None:
    caminho.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(dados).write_parquet(caminho, compression="zstd")


def test_aba_mensal_consolida_movimentacoes_e_classifica_omissao(tmp_path: Path):
    cnpj = "12345678000190"
    diretorio_cnpj = tmp_path / cnpj
    diretorio_parquets = diretorio_cnpj / "parquets"
    diretorio_parquets.mkdir(parents=True, exist_ok=True)

    _escrever_parquet(
        diretorio_parquets / "mov_estoque.parquet",
        [
            {
                "id_agrupado": "AGR_00001",
                "descricao": "Produto A",
                "tipo": "ENTRADA",
                "data": "2026-01-05",
                "quantidade": 10.0,
                "valor_unitario": 5.0,
                "valor_total": 50.0,
                "cfop": "1102",
                "origem": "efd",
                "saldo": 10.0,
                "custo_medio": 5.0,
            },
            {
                "id_agrupado": "AGR_00001",
                "descricao": "Produto A",
                "tipo": "SAIDA",
                "data": "2026-01-10",
                "quantidade": 3.0,
                "valor_unitario": 5.0,
                "valor_total": 15.0,
                "cfop": "5102",
                "origem": "nfe",
                "saldo": 7.0,
                "custo_medio": 5.0,
            },
            {
                "id_agrupado": "AGR_00001",
                "descricao": "Produto A",
                "tipo": "SAIDA",
                "data": "2026-01-31",
                "quantidade": 9.0,
                "valor_unitario": 5.0,
                "valor_total": 45.0,
                "cfop": "5102",
                "origem": "nfe",
                "saldo": -2.0,
                "custo_medio": 5.0,
            },
        ],
    )

    total_registros = gerar_aba_mensal(
        diretorio_cnpj,
        diretorio_parquets,
        diretorio_parquets / CONTRATOS["aba_mensal"].saida,
        CONTRATOS["aba_mensal"],
    )

    assert total_registros == 1

    df_resultado = pl.read_parquet(diretorio_parquets / "aba_mensal.parquet")
    linha = df_resultado.to_dicts()[0]

    assert linha["id_agrupado"] == "AGR_00001"
    assert linha["mes"] == "2026-01"
    assert linha["saldo_inicial"] == 10.0
    assert linha["entradas"] == 10.0
    assert linha["saidas"] == 12.0
    assert linha["saldo_final"] == -2.0
    assert linha["custo_medio"] == 5.0
    assert linha["valor_estoque"] == -10.0
    assert linha["qtd_movimentos"] == 3
    assert linha["omissao"] is True


def test_aba_anual_soma_meses_e_preserva_fechamento_final(tmp_path: Path):
    cnpj = "12345678000190"
    diretorio_cnpj = tmp_path / cnpj
    diretorio_parquets = diretorio_cnpj / "parquets"
    diretorio_parquets.mkdir(parents=True, exist_ok=True)

    _escrever_parquet(
        diretorio_parquets / "aba_mensal.parquet",
        [
            {
                "id_agrupado": "AGR_00001",
                "descricao": "Produto A",
                "mes": "2026-01",
                "saldo_inicial": 10.0,
                "entradas": 10.0,
                "saidas": 12.0,
                "saldo_final": -2.0,
                "custo_medio": 5.0,
                "valor_estoque": -10.0,
                "qtd_movimentos": 3,
                "omissao": True,
            },
            {
                "id_agrupado": "AGR_00001",
                "descricao": "Produto A",
                "mes": "2026-02",
                "saldo_inicial": -2.0,
                "entradas": 4.0,
                "saidas": 1.0,
                "saldo_final": 1.0,
                "custo_medio": 5.0,
                "valor_estoque": 5.0,
                "qtd_movimentos": 2,
                "omissao": False,
            },
        ],
    )

    total_registros = gerar_aba_anual(
        diretorio_cnpj,
        diretorio_parquets,
        diretorio_parquets / CONTRATOS["aba_anual"].saida,
        CONTRATOS["aba_anual"],
    )

    assert total_registros == 1

    df_resultado = pl.read_parquet(diretorio_parquets / "aba_anual.parquet")
    linha = df_resultado.to_dicts()[0]

    assert linha["id_agrupado"] == "AGR_00001"
    assert linha["ano"] == "2026"
    assert linha["saldo_inicial_ano"] == 10.0
    assert linha["total_entradas"] == 14.0
    assert linha["total_saidas"] == 13.0
    assert linha["saldo_final_ano"] == 1.0
    assert linha["custo_medio_anual"] == 5.0
    assert linha["valor_estoque_final"] == 5.0
    assert linha["meses_com_omissao"] == 1
    assert linha["total_omissao"] == 0.0
