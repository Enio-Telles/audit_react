from pathlib import Path

from utilitarios import dataset_registry as registry


def test_normaliza_aliases_basicos():
    assert registry.normalizar_dataset_id("cadastral") == "dados_cadastrais"
    assert registry.normalizar_dataset_id("movimentacao_estoque") == "mov_estoque"
    assert registry.normalizar_dataset_id("efd_bloco_h") == "bloco_h"


def test_lista_aliases_do_dataset_canonico():
    aliases = registry.listar_aliases_dataset("dados_cadastrais")
    assert "cadastral" in aliases
    assert "cadastro_fisconforme" in aliases


def test_listar_caminhos_com_fallback_inclui_variantes_delta_e_parquet():
    caminhos = registry.listar_caminhos_com_fallback("12345678901234", "dados_cadastrais")
    textos = {str(path) for path in caminhos}
    assert any(texto.endswith("dados_cadastrais.parquet") for texto in textos)
    assert any(texto.endswith("dados_cadastrais") for texto in textos)


def test_encontrar_dataset_resolve_alias_para_delta_dir(monkeypatch, tmp_path: Path):
    monkeypatch.setattr(registry, "CNPJ_ROOT", tmp_path)
    delta_dir = tmp_path / "12345678901234" / "fisconforme" / "dados_cadastrais"
    delta_dir.mkdir(parents=True)

    localizado = registry.encontrar_dataset("12345678901234", "cadastral")
    assert localizado is not None
    assert localizado.dataset_id == "dados_cadastrais"
    assert localizado.caminho == delta_dir


def test_catalogo_resumido_tem_aliases_e_materializados():
    resumo = registry.catalogo_resumido()
    assert resumo["total_datasets"] >= 10
    assert "dados_cadastrais" in resumo["materialized_datasets"]
    assert resumo["aliases"]["cadastral"] == "dados_cadastrais"
