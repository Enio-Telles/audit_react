"""Testes para o registro centralizado de datasets (dataset_registry)."""
from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from utilitarios import dataset_registry as reg


CNPJ_TESTE = "37671507000187"


# ---------------------------------------------------------------------------
# Catálogo
# ---------------------------------------------------------------------------


class TestCatalogo:
    def test_catalogo_nao_vazio(self):
        assert len(reg.CATALOGO) > 0

    def test_ids_unicos(self):
        ids = [d.dataset_id for d in reg.CATALOGO]
        assert len(ids) == len(set(ids)), f"IDs duplicados: {[x for x in ids if ids.count(x) > 1]}"

    def test_sql_ids_unicos(self):
        sql_ids = [d.sql_id for d in reg.CATALOGO if d.sql_id]
        assert len(sql_ids) == len(set(sql_ids)), f"SQL IDs duplicados"

    def test_obter_definicao_existente(self):
        d = reg.obter_definicao("nfe_base")
        assert d is not None
        assert d.sql_id == "NFe.sql"
        assert d.tipo == "por_cnpj"

    def test_obter_definicao_inexistente(self):
        assert reg.obter_definicao("inexistente_xyz") is None

    def test_obter_definicao_por_sql(self):
        d = reg.obter_definicao_por_sql("NFe.sql")
        assert d is not None
        assert d.dataset_id == "nfe_base"

    def test_obter_definicao_por_sql_case_insensitive(self):
        d = reg.obter_definicao_por_sql("nfe.sql")
        assert d is not None
        assert d.dataset_id == "nfe_base"

    def test_listar_datasets(self):
        todos = reg.listar_datasets()
        assert len(todos) == len(reg.CATALOGO)
        # deve estar ordenado
        ids = [d.dataset_id for d in todos]
        assert ids == sorted(ids)

    def test_listar_datasets_por_tabela(self):
        nfe_consumers = reg.listar_datasets_por_tabela("BI.FATO_NFE_DETALHE")
        assert any(d.dataset_id == "nfe_base" for d in nfe_consumers)

    def test_resolver_dataset_por_sql_id(self):
        assert reg.resolver_dataset_por_sql_id("NFe.sql") == "nfe_base"
        assert reg.resolver_dataset_por_sql_id("c170.sql") == "efd_c170"
        assert reg.resolver_dataset_por_sql_id("inexistente.sql") is None

    def test_resolver_dataset_por_sql_id_aceita_basename_quando_catalogo_usa_subpasta(self):
        assert reg.resolver_dataset_por_sql_id("sitafe_nfe_calculo_item.sql") == "sitafe_calculo_item"


# ---------------------------------------------------------------------------
# Caminhos
# ---------------------------------------------------------------------------


class TestCaminhos:
    def test_caminho_canonico_por_cnpj(self):
        caminho = reg.obter_caminho(CNPJ_TESTE, "nfe_base")
        assert "shared_sql" in str(caminho)
        assert CNPJ_TESTE in caminho.name
        assert caminho.suffix == ".parquet"

    def test_caminho_dimensao_global(self):
        caminho = reg.obter_caminho(None, "dim_localidade")
        assert "dimensoes" in str(caminho)
        assert caminho.suffix == ".parquet"

    def test_caminho_por_cnpj_sem_cnpj_raises(self):
        with pytest.raises(ValueError, match="CNPJ obrigatório"):
            reg.obter_caminho(None, "nfe_base")

    def test_caminho_dataset_inexistente_raises(self):
        with pytest.raises(ValueError, match="Dataset desconhecido"):
            reg.obter_caminho(CNPJ_TESTE, "nao_existe_xyz")

    def test_listar_caminhos_com_fallback_nfe(self):
        caminhos = reg.listar_caminhos_com_fallback(CNPJ_TESTE, "nfe_base")
        assert len(caminhos) >= 2  # ao menos canônico + 1 legado
        # primeiro deve ser o canônico
        assert "shared_sql" in str(caminhos[0])
        assert caminhos[1].name == f"NFe_{CNPJ_TESTE}.parquet"
        # sem duplicatas
        assert len(caminhos) == len(set(str(c).lower() for c in caminhos))

    def test_listar_caminhos_com_fallback_cadastral(self):
        caminhos = reg.listar_caminhos_com_fallback(CNPJ_TESTE, "cadastral")
        assert len(caminhos) >= 2
        assert "shared_sql" in str(caminhos[0])

    def test_listar_caminhos_com_fallback_sitafe_calculo_item_inclui_legado_fora_de_arquivos_parquet(self):
        caminhos = reg.listar_caminhos_com_fallback(CNPJ_TESTE, "sitafe_calculo_item")
        assert any(str(caminho).endswith(f"shared_sql\\sitafe_nfe_calculo_item_{CNPJ_TESTE}.parquet") for caminho in caminhos)

    def test_listar_caminhos_dataset_desconhecido(self):
        assert reg.listar_caminhos_com_fallback(CNPJ_TESTE, "xyz_nao_existe") == []


# ---------------------------------------------------------------------------
# Busca e carregamento
# ---------------------------------------------------------------------------


class TestBuscaCarregamento:
    def test_encontrar_dataset_nao_existente(self, tmp_path):
        # com um CNPJ que certamente não tem dados
        assert reg.encontrar_dataset("00000000000000", "nfe_base") is None

    def test_carregar_lazyframe_nao_existente(self):
        assert reg.carregar_lazyframe("00000000000000", "nfe_base") is None

    def test_carregar_dataframe_nao_existente(self):
        assert reg.carregar_dataframe("00000000000000", "nfe_base") is None


# ---------------------------------------------------------------------------
# Registro e persistência
# ---------------------------------------------------------------------------


class TestRegistro:
    def test_registrar_e_encontrar(self, tmp_path, monkeypatch):
        # Redirecionar CNPJ_ROOT para tmp_path
        monkeypatch.setattr(reg, "CNPJ_ROOT", tmp_path)

        cnpj = "99999999000199"
        df = pl.DataFrame({"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]})
        metadata = reg.criar_metadata(
            cnpj=cnpj,
            dataset_id="nfe_base",
            sql_id="NFe.sql",
            linhas=3,
        )

        caminho = reg.registrar_dataset(cnpj, "nfe_base", df, metadata=metadata)
        assert caminho is not None
        assert caminho.exists()
        assert caminho.suffix == ".parquet"

        # Metadata sidecar
        meta_path = caminho.with_suffix(".metadata.json")
        assert meta_path.exists()

        # Encontrar
        localizado = reg.encontrar_dataset(cnpj, "nfe_base")
        assert localizado is not None
        assert localizado.reutilizado is True
        assert localizado.caminho == caminho
        assert localizado.metadata is not None
        assert localizado.metadata["dataset_id"] == "nfe_base"

    def test_registrar_dimensao_global(self, tmp_path, monkeypatch):
        monkeypatch.setattr(reg, "REFERENCIAS_ROOT", tmp_path)

        df = pl.DataFrame({"cod": ["001", "002"], "desc": ["Ativo", "Inativo"]})
        caminho = reg.registrar_dataset(None, "dim_situacao", df)
        assert caminho is not None
        assert caminho.exists()

    def test_carregar_lazyframe_apos_registro(self, tmp_path, monkeypatch):
        monkeypatch.setattr(reg, "CNPJ_ROOT", tmp_path)

        cnpj = "88888888000188"
        df = pl.DataFrame({"valor": [10, 20]})
        reg.registrar_dataset(cnpj, "efd_c100", df)

        resultado = reg.carregar_lazyframe(cnpj, "efd_c100")
        assert resultado is not None
        lf, caminho = resultado
        assert lf.collect().height == 2


# ---------------------------------------------------------------------------
# Diagnóstico
# ---------------------------------------------------------------------------


class TestDiagnostico:
    def test_diagnosticar_disponibilidade(self):
        diag = reg.diagnosticar_disponibilidade("00000000000000")
        assert len(diag) == len(reg.CATALOGO)
        for item in diag:
            assert "dataset_id" in item
            assert "disponivel" in item
            assert isinstance(item["disponivel"], bool)

    def test_diagnosticar_com_dados_existentes(self, tmp_path, monkeypatch):
        monkeypatch.setattr(reg, "CNPJ_ROOT", tmp_path)

        cnpj = "77777777000177"
        df = pl.DataFrame({"a": [1]})
        reg.registrar_dataset(cnpj, "cadastral", df)

        diag = reg.diagnosticar_disponibilidade(cnpj)
        cadastral = next(d for d in diag if d["dataset_id"] == "cadastral")
        assert cadastral["disponivel"] is True


# ---------------------------------------------------------------------------
# Cobertura de datasets conhecidos
# ---------------------------------------------------------------------------


class TestCobertura:
    """Garante que datasets críticos estão no catálogo."""

    @pytest.mark.parametrize(
        "dataset_id",
        [
            "nfe_base",
            "nfce_base",
            "efd_c100",
            "efd_c170",
            "efd_c176",
            "efd_0200",
            "efd_0190",
            "efd_0000",
            "efd_bloco_h",
            "efd_e111",
            "cadastral",
            "dim_localidade",
            "dim_regime",
            "dim_situacao",
        ],
    )
    def test_dataset_critico_cadastrado(self, dataset_id):
        assert reg.obter_definicao(dataset_id) is not None

    @pytest.mark.parametrize(
        "sql_id",
        [
            "NFe.sql",
            "NFCe.sql",
            "c100.sql",
            "c170.sql",
            "c176.sql",
            "reg_0200.sql",
            "reg_0190.sql",
            "reg_0000.sql",
            "bloco_h.sql",
            "E111.sql",
            "dados_cadastrais.sql",
        ],
    )
    def test_sql_critica_mapeada(self, sql_id):
        assert reg.obter_definicao_por_sql(sql_id) is not None
