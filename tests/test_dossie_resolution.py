from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path("src").resolve()))

from interface_grafica.services.dossie_resolution import DossieResolucaoErro
from interface_grafica.services.dossie_resolution import resolver_secao_dossie


def test_resolver_secao_dossie_reaproveita_aliases_prioritarios():
    resolvido = resolver_secao_dossie(
        cnpj="12345678000190",
        secao_id="documentos_fiscais",
        parametros={"ano": 2025},
        versao_consulta="v1",
    )

    assert resolvido.secao_id == "documentos_fiscais"
    assert resolvido.sql_ids == ("NFe.sql", "NFCe.sql")
    assert resolvido.cache_key
    assert resolvido.cache_file_name.endswith(".parquet")


def test_resolver_secao_dossie_para_cadastro_usa_sql_existente():
    resolvido = resolver_secao_dossie(
        cnpj="12345678000190",
        secao_id="cadastro",
        parametros=None,
        versao_consulta="v1",
    )

    assert resolvido.sql_ids == ("dados_cadastrais.sql",)


def test_resolver_secao_dossie_falha_para_secao_inexistente():
    with pytest.raises(DossieResolucaoErro):
        resolver_secao_dossie(
            cnpj="12345678000190",
            secao_id="nao_existe",
            parametros=None,
            versao_consulta="v1",
        )
