from pathlib import Path
import sys

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.dossie_catalog import listar_secoes_dossie
from interface_grafica.services.dossie_catalog import listar_sql_prioritarias
from interface_grafica.services.dossie_catalog import obter_secao_dossie


def test_listar_secoes_dossie_retorna_ordem_basica():
    secoes = listar_secoes_dossie()

    assert [secao.id for secao in secoes] == [
        "cadastro",
        "documentos_fiscais",
        "arrecadacao",
    ]


def test_obter_secao_dossie_localiza_por_id():
    secao = obter_secao_dossie("documentos_fiscais")

    assert secao is not None
    assert secao.titulo == "Documentos fiscais"
    assert secao.tipo_fonte == "sql_catalog"


def test_listar_sql_prioritarias_reaproveita_catalogo_existente():
    sql_ids = listar_sql_prioritarias("documentos_fiscais")

    assert sql_ids == ["NFe.sql", "NFCe.sql"]


def test_secao_inexistente_retorna_vazio_ou_none():
    assert obter_secao_dossie("inexistente") is None
    assert listar_sql_prioritarias("inexistente") == []
