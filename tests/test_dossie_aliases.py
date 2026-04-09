from pathlib import Path
import sys

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.dossie_aliases import listar_aliases_por_secao
from interface_grafica.services.dossie_aliases import listar_sql_ids_por_secao


def test_listar_aliases_por_secao_ordena_por_prioridade():
    aliases = listar_aliases_por_secao("documentos_fiscais")

    assert [alias.sql_id for alias in aliases] == ["NFe.sql", "NFCe.sql"]


def test_listar_sql_ids_por_secao_retorna_vazio_quando_inexistente():
    assert listar_sql_ids_por_secao("secao_inexistente") == []


def test_alias_de_cadastro_prioriza_sql_existente():
    assert listar_sql_ids_por_secao("cadastro") == ["dados_cadastrais.sql"]


def test_alias_de_contato_usa_fontes_multiplas_em_ordem():
    assert listar_sql_ids_por_secao("contato") == [
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
