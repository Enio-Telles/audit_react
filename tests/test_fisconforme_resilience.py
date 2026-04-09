from pathlib import Path
import sys

sys.path.insert(0, str(Path("src").resolve()))
sys.path.insert(0, str(Path("backend").resolve()))

from backend.routers.fisconforme import _converter_linhas_oracle_em_registros
from interface_grafica.fisconforme.extracao import converter_linhas_oracle_em_registros


def test_router_fisconforme_converte_coluna_mista_sem_quebrar_contrato():
    colunas = ["IE", "RAZAO_SOCIAL"]
    linhas = [
        (123, "Empresa A"),
        ("000123", "Empresa B"),
        (None, "Empresa C"),
    ]

    registros = _converter_linhas_oracle_em_registros(colunas, linhas)

    assert registros == [
        {"IE": "123", "RAZAO_SOCIAL": "Empresa A"},
        {"IE": "000123", "RAZAO_SOCIAL": "Empresa B"},
        {"IE": "", "RAZAO_SOCIAL": "Empresa C"},
    ]


def test_extracao_fisconforme_converte_coluna_mista_sem_perder_registros():
    colunas = ["ie", "razao_social"]
    linhas = [
        (123, "Empresa A"),
        ("000123", "Empresa B"),
        (None, "Empresa C"),
    ]

    registros = converter_linhas_oracle_em_registros(colunas, linhas)

    assert registros == [
        {"ie": "123", "razao_social": "Empresa A"},
        {"ie": "000123", "razao_social": "Empresa B"},
        {"ie": None, "razao_social": "Empresa C"},
    ]
