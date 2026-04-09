from pathlib import Path
import sys

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.dossie_catalog import listar_secoes_dossie
from interface_grafica.services.dossie_catalog import listar_sql_prioritarias
from interface_grafica.services.dossie_catalog import obter_secao_dossie


def test_listar_secoes_dossie_retorna_ordem_basica():
    secoes = listar_secoes_dossie()

    ids = [secao.id for secao in secoes]
    assert ids[:3] == ["cadastro", "documentos_fiscais", "arrecadacao"]
    assert "contato" in ids
    assert "estoque" in ids
    assert "ressarcimento_st" in ids


def test_obter_secao_dossie_localiza_por_id():
    secao = obter_secao_dossie("documentos_fiscais")

    assert secao is not None
    assert secao.titulo == "Documentos fiscais"
    assert secao.tipo_fonte == "sql_catalog"


def test_secao_arrecadacao_agora_e_cache_catalog():
    secao = obter_secao_dossie("arrecadacao")

    assert secao is not None
    assert secao.tipo_fonte == "cache_catalog"
    assert listar_sql_prioritarias("arrecadacao") == []


def test_listar_sql_prioritarias_reaproveita_catalogo_existente():
    sql_ids = listar_sql_prioritarias("documentos_fiscais")

    assert sql_ids == ["NFe.sql", "NFCe.sql"]


def test_secao_contato_reaproveita_multiplas_fontes():
    sql_ids = listar_sql_prioritarias("contato")

    assert sql_ids == [
        "dados_cadastrais.sql",
        "dossie_filiais_raiz.sql",
        "dossie_contador.sql",
        "dossie_historico_fac.sql",
        "dossie_rascunho_fac_contador.sql",
        "dossie_req_inscricao_contador.sql",
        "dossie_historico_socios.sql",
        "NFe.sql",
        "NFCe.sql",
    ]


def test_secao_inexistente_retorna_vazio_ou_none():
    assert obter_secao_dossie("inexistente") is None
    assert listar_sql_prioritarias("inexistente") == []


def test_secao_estoque_e_cache_only_sem_sql_prioritaria():
    secao = obter_secao_dossie("estoque")

    assert secao is not None
    assert secao.tipo_fonte == "cache_catalog"
    assert listar_sql_prioritarias("estoque") == []
