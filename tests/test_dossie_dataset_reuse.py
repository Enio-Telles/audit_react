from pathlib import Path
import json
import sys

import polars as pl

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services import dossie_dataset_reuse as dataset_reuse
from utilitarios import dataset_registry as registry


def test_salvar_dataset_compartilhado_persiste_metadata(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dataset_reuse, "CNPJ_ROOT", cnpj_root_teste)

    dataframe = pl.DataFrame({"coluna": [1, 2]})
    metadata = dataset_reuse.criar_metadata_dataset_compartilhado(
        cnpj="12345678000190",
        sql_id="dados_cadastrais.sql",
        parametros={"CNPJ": "12345678000190"},
        versao_consulta="v1",
        dependencias=["dados_cadastrais.sql"],
    )

    caminho = dataset_reuse.salvar_dataset_compartilhado(
        "12345678000190",
        "dados_cadastrais.sql",
        dataframe,
        metadata=metadata,
    )

    assert caminho is not None
    assert caminho.exists()

    caminho_metadata = dataset_reuse.obter_caminho_metadata_dataset_compartilhado(
        "12345678000190",
        "dados_cadastrais.sql",
    )
    assert caminho_metadata.exists()

    metadata_salva = json.loads(caminho_metadata.read_text(encoding="utf-8"))
    assert metadata_salva["sql_id"] == "dados_cadastrais.sql"
    assert metadata_salva["versao_consulta"] == "v1"
    assert metadata_salva["parametros"]["CNPJ"] == "12345678000190"


def test_carregar_dataset_reutilizavel_retorna_metadata_e_lazyframe(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dataset_reuse, "CNPJ_ROOT", cnpj_root_teste)

    caminho_dataset = (
        cnpj_root_teste
        / "12345678000190"
        / "arquivos_parquet"
        / "shared_sql"
        / "dados_cadastrais_12345678000190.parquet"
    )
    caminho_dataset.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"nome": ["Empresa Teste"]}).write_parquet(caminho_dataset)

    caminho_metadata = caminho_dataset.with_suffix(".metadata.json")
    caminho_metadata.write_text(
        json.dumps({"sql_id": "dados_cadastrais.sql", "origem": "teste"}, ensure_ascii=True),
        encoding="utf-8",
    )

    lazyframe = dataset_reuse.carregar_lazyframe_reutilizavel("12345678000190", "dados_cadastrais.sql")
    assert lazyframe is not None
    dataframe_lazy, caminho_origem = lazyframe
    assert caminho_origem == caminho_dataset
    assert dataframe_lazy.collect().shape == (1, 1)

    dataset = dataset_reuse.carregar_dataset_reutilizavel("12345678000190", "dados_cadastrais.sql")
    assert dataset is not None
    assert dataset.caminho_origem == caminho_dataset
    assert dataset.metadata == {"sql_id": "dados_cadastrais.sql", "origem": "teste"}
    assert dataset.dataframe.to_dicts() == [{"nome": "Empresa Teste"}]


def test_carregar_dataset_reutilizavel_prioriza_metadata_do_caminho_real_reutilizado(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dataset_reuse, "CNPJ_ROOT", cnpj_root_teste)
    monkeypatch.setattr(registry, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    caminho_dataset = cnpj_root_teste / cnpj / "arquivos_parquet" / "nfe_agr_12345678000190.parquet"
    caminho_dataset.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"nome": ["Legado"]}).write_parquet(caminho_dataset)
    caminho_dataset.with_suffix(".metadata.json").write_text(
        json.dumps({"sql_id": "NFe.sql", "origem": "legado_real"}, ensure_ascii=True),
        encoding="utf-8",
    )

    dataset = dataset_reuse.carregar_dataset_reutilizavel(cnpj, "NFe.sql")

    assert dataset is not None
    assert dataset.metadata == {"sql_id": "NFe.sql", "origem": "legado_real"}


def test_listar_caminhos_reutilizaveis_reconhece_parquets_analiticos_de_estoque_e_ressarcimento(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dataset_reuse, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    base_produtos = cnpj_root_teste / cnpj / "analises" / "produtos"
    base_ressarcimento = cnpj_root_teste / cnpj / "analises" / "ressarcimento_st"
    base_produtos.mkdir(parents=True, exist_ok=True)
    base_ressarcimento.mkdir(parents=True, exist_ok=True)

    caminho_mov = base_produtos / f"mov_estoque_{cnpj}.parquet"
    caminho_mensal = base_produtos / f"aba_mensal_{cnpj}.parquet"
    caminho_anual = base_produtos / f"aba_anual_{cnpj}.parquet"
    caminho_ressarc = base_ressarcimento / f"ressarcimento_st_item_{cnpj}.parquet"

    pl.DataFrame({"coluna": [1]}).write_parquet(caminho_mov)
    pl.DataFrame({"coluna": [2]}).write_parquet(caminho_mensal)
    pl.DataFrame({"coluna": [3]}).write_parquet(caminho_anual)
    pl.DataFrame({"coluna": [4]}).write_parquet(caminho_ressarc)

    assert dataset_reuse.carregar_lazyframe_reutilizavel(cnpj, "mov_estoque.sql") is not None
    assert dataset_reuse.carregar_lazyframe_reutilizavel(cnpj, "aba_mensal.sql") is not None
    assert dataset_reuse.carregar_lazyframe_reutilizavel(cnpj, "aba_anual.sql") is not None
    assert dataset_reuse.carregar_lazyframe_reutilizavel(cnpj, "ressarcimento_st_item.sql") is not None

    caminhos_mov = dataset_reuse.listar_caminhos_reutilizaveis(cnpj, "mov_estoque.sql")
    caminhos_mensal = dataset_reuse.listar_caminhos_reutilizaveis(cnpj, "aba_mensal.sql")
    caminhos_anual = dataset_reuse.listar_caminhos_reutilizaveis(cnpj, "aba_anual.sql")
    caminhos_ressarc = dataset_reuse.listar_caminhos_reutilizaveis(cnpj, "ressarcimento_st_item.sql")

    assert caminho_mov in caminhos_mov
    assert caminho_mensal in caminhos_mensal
    assert caminho_anual in caminhos_anual
    assert caminho_ressarc in caminhos_ressarc


def test_listar_caminhos_reutilizaveis_prioriza_shared_sql_sobre_legacy_para_nfe(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dataset_reuse, "CNPJ_ROOT", cnpj_root_teste)
    monkeypatch.setattr(registry, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    base = cnpj_root_teste / cnpj / "arquivos_parquet"
    shared = base / "shared_sql" / f"nfe_base_{cnpj}.parquet"
    legacy = base / f"nfe_agr_{cnpj}.parquet"
    shared.parent.mkdir(parents=True, exist_ok=True)
    legacy.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"coluna": [1]}).write_parquet(shared)
    pl.DataFrame({"coluna": [2]}).write_parquet(legacy)

    caminhos = dataset_reuse.listar_caminhos_reutilizaveis(cnpj, "NFe.sql")

    assert caminhos[0] == shared
    assert legacy in caminhos


def test_carregar_dataset_reutilizavel_repassa_parametro_para_composicao_fronteira(monkeypatch):
    chamadas: list[dict] = []

    monkeypatch.setattr(dataset_reuse, "carregar_lazyframe_reutilizavel", lambda *args, **kwargs: None)

    def atualizar_fronteira_fake(cnpj: str, data_limite_processamento=None):
        chamadas.append(
            {
                "cnpj": cnpj,
                "data_limite_processamento": data_limite_processamento,
            }
        )
        return None

    monkeypatch.setattr(dataset_reuse, "atualizar_composicao_fronteira", atualizar_fronteira_fake)

    resultado = dataset_reuse.carregar_dataset_reutilizavel(
        "12345678000190",
        "fronteira.sql",
        parametros={"data_limite_processamento": "31/03/2024"},
    )

    assert resultado is None
    assert chamadas == [
        {
            "cnpj": "12345678000190",
            "data_limite_processamento": "31/03/2024",
        }
    ]


def test_carregar_dataset_reutilizavel_ignora_parquet_corrompido_e_cai_para_legacy(tmp_path, monkeypatch):
    cnpj_root_teste = tmp_path / "CNPJ"
    monkeypatch.setattr(dataset_reuse, "CNPJ_ROOT", cnpj_root_teste)
    monkeypatch.setattr(registry, "CNPJ_ROOT", cnpj_root_teste)

    cnpj = "12345678000190"
    base = cnpj_root_teste / cnpj / "arquivos_parquet"
    caminho_corrompido = base / "shared_sql" / f"dossie_vistorias_{cnpj}.parquet"
    caminho_legacy = base / f"dados_cadastrais_{cnpj}.parquet"
    caminho_corrompido.parent.mkdir(parents=True, exist_ok=True)
    caminho_legacy.parent.mkdir(parents=True, exist_ok=True)

    caminho_corrompido.write_bytes(b"")
    pl.DataFrame({"nome": ["Empresa Legada"]}).write_parquet(caminho_legacy)

    monkeypatch.setattr(
        dataset_reuse,
        "listar_caminhos_reutilizaveis",
        lambda *_args, **_kwargs: [caminho_corrompido, caminho_legacy],
    )

    dataset = dataset_reuse.carregar_dataset_reutilizavel(cnpj, "dossie_vistorias.sql")

    assert dataset is not None
    assert dataset.caminho_origem == caminho_legacy
    assert dataset.dataframe.to_dicts() == [{"nome": "Empresa Legada"}]
