from __future__ import annotations

from pathlib import Path

import polars as pl

import audit_engine  # noqa: F401
from audit_engine.contratos.base import CONTRATOS
from audit_engine.pipeline.orquestrador import OrquestradorPipeline


def test_pipeline_gera_todas_tabelas_com_schema_quando_sem_dados(tmp_path: Path):
    cnpj = "12345678000190"
    diretorio_cnpj = tmp_path / cnpj

    for pasta in ["extraidos", "parquets", "edicoes", "exportacoes"]:
        (diretorio_cnpj / pasta).mkdir(parents=True, exist_ok=True)

    resultado = OrquestradorPipeline(diretorio_cnpj, cnpj).executar_pipeline_completo()

    assert resultado.status in {"concluido", "concluido_com_erros"}

    for nome_tabela, contrato in CONTRATOS.items():
        caminho_parquet = diretorio_cnpj / "parquets" / contrato.saida
        assert caminho_parquet.exists(), f"Parquet ausente: {nome_tabela}"

        dataframe = pl.read_parquet(caminho_parquet)
        assert dataframe.columns == [coluna.nome for coluna in contrato.colunas], (
            f"Schema divergente em {nome_tabela}"
        )

    tabelas_silver_esperadas = {
        "tb_documentos",
        "item_unidades",
        "itens",
        "descricao_produtos",
        "fontes_produtos",
        "c170_xml",
        "c176_xml",
        "nfe_dados_st",
        "e111_ajustes",
    }
    arquivos_silver = {arquivo.stem for arquivo in (diretorio_cnpj / "silver").glob("*.parquet")}
    assert tabelas_silver_esperadas.issubset(arquivos_silver)

    manifesto = OrquestradorPipeline(diretorio_cnpj, cnpj).gerar_manifesto()
    assert manifesto["camadas"]["silver"]["total_tabelas"] >= len(tabelas_silver_esperadas)
