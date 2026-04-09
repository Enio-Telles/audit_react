from pathlib import Path
import sys

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.dossie_cache_keys import criar_chave_cache_secao
from interface_grafica.services.dossie_cache_keys import gerar_chave_cache_dossie
from interface_grafica.services.dossie_cache_keys import gerar_nome_arquivo_cache_dossie
from interface_grafica.services.dossie_cache_keys import normalizar_parametros_dossie
from interface_grafica.services.dossie_cache_keys import serializar_parametros_dossie


def test_normalizar_parametros_remove_vazios_e_ordena_chaves():
    parametros = {
        "fim": "2025-12-31",
        "inicio": "2025-01-01",
        "vazio": "",
        "nulo": None,
        "itens": ["b", "a", "a"],
    }

    normalizado = normalizar_parametros_dossie(parametros)

    assert normalizado == {
        "fim": "2025-12-31",
        "inicio": "2025-01-01",
        "itens": ["a", "a", "b"],
    }


def test_serializacao_estavel_para_parametros_equivalentes():
    p1 = {"b": [2, 1], "a": "x"}
    p2 = {"a": "x", "b": [1, 2]}

    assert serializar_parametros_dossie(p1) == serializar_parametros_dossie(p2)


def test_chave_igual_para_parametros_equivalentes():
    p1 = {"inicio": "2025-01-01", "fim": "2025-12-31", "uf": ["TO", "GO"]}
    p2 = {"uf": ["GO", "TO"], "fim": "2025-12-31", "inicio": "2025-01-01"}

    chave_1 = gerar_chave_cache_dossie("12345678000190", "nfe-saida", p1, versao_consulta="v2")
    chave_2 = gerar_chave_cache_dossie("12345678000190", "nfe-saida", p2, versao_consulta="v2")

    assert chave_1 == chave_2


def test_chave_muda_quando_secao_ou_versao_mudam():
    base = gerar_chave_cache_dossie("12345678000190", "nfe-saida", {"ano": 2025}, versao_consulta="v1")
    outra_secao = gerar_chave_cache_dossie("12345678000190", "nfce-saida", {"ano": 2025}, versao_consulta="v1")
    outra_versao = gerar_chave_cache_dossie("12345678000190", "nfe-saida", {"ano": 2025}, versao_consulta="v2")

    assert base != outra_secao
    assert base != outra_versao


def test_nome_arquivo_cache_retem_contexto_basico():
    nome = gerar_nome_arquivo_cache_dossie(
        cnpj="12345678000190",
        secao="NFe Saida",
        parametros={"ano": 2025},
        versao_consulta="v1",
    )

    assert nome.startswith("dossie_12345678000190_nfe_saida_")
    assert nome.endswith(".parquet")


def test_alias_legado_de_secao_gera_mesma_chave_do_nome_canonico():
    parametros = {"ano": 2025, "uf": ["GO", "TO"]}

    chave_canonica = gerar_chave_cache_dossie(
        cnpj="12345678000190",
        secao="nfe-saida",
        parametros=parametros,
        versao_consulta="v3",
    )
    chave_legada = criar_chave_cache_secao(
        secao="nfe-saida",
        cnpj="12345678000190",
        parametros=parametros,
        versao_consulta="v3",
    )

    assert chave_legada == chave_canonica
