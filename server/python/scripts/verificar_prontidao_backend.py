"""Verifica se o ambiente Python do backend esta pronto para testes e E2E local."""

from __future__ import annotations

import importlib
import json
import sys


MODULOS_OBRIGATORIOS = {
    "fastapi": "API e TestClient",
    "polars": "pipeline analitico",
    "pypdf": "manipulacao de PDF nos relatorios e testes",
    "docx": "geracao de DOCX via python-docx",
    "oracledb": "cliente Oracle do backend",
    "multipart": "suporte a upload/form-data do FastAPI",
    "pytest": "suite automatizada",
}

MODULOS_OPCIONAIS = {
    "weasyprint": "renderizacao HTML -> PDF em runtime",
    "reportlab": "fallback textual de PDF em runtime",
}


def verificar_modulos(modulos: dict[str, str]) -> list[dict[str, str | bool]]:
    """Executa importacao real dos modulos declarados e registra o resultado."""
    resultados: list[dict[str, str | bool]] = []
    for nome_modulo, finalidade in modulos.items():
        try:
            importlib.import_module(nome_modulo)
            resultados.append(
                {
                    "modulo": nome_modulo,
                    "finalidade": finalidade,
                    "instalado": True,
                    "mensagem": "",
                }
            )
        except Exception as erro:  # noqa: BLE001
            resultados.append(
                {
                    "modulo": nome_modulo,
                    "finalidade": finalidade,
                    "instalado": False,
                    "mensagem": f"{type(erro).__name__}: {erro}",
                }
            )
    return resultados


def main() -> None:
    obrigatorios = verificar_modulos(MODULOS_OBRIGATORIOS)
    opcionais = verificar_modulos(MODULOS_OPCIONAIS)
    pronto_core = all(item["instalado"] for item in obrigatorios)

    payload = {
        "python": sys.executable,
        "pronto_core": pronto_core,
        "obrigatorios": obrigatorios,
        "opcionais": opcionais,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    if not pronto_core:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
