"""Executa fluxo E2E local contra a API FastAPI (sem depender de servidor HTTP externo)."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

RAIZ_BACKEND = Path(__file__).resolve().parents[1]
if str(RAIZ_BACKEND) not in sys.path:
    sys.path.insert(0, str(RAIZ_BACKEND))

from api import app


def _carregar_bytes_se_existir(caminho: Path) -> bytes | None:
    """Lê um arquivo binário somente quando ele existir."""
    if not caminho.exists():
        return None
    return caminho.read_bytes()


def _restaurar_arquivo_binario(caminho: Path, conteudo: bytes | None) -> None:
    """Restaura um arquivo ao estado anterior ou remove quando não existia."""
    if conteudo is None:
        if caminho.exists():
            caminho.unlink()
        return

    caminho.parent.mkdir(parents=True, exist_ok=True)
    caminho.write_bytes(conteudo)


def _extrair_ids_publicos_produtos(dados_produtos: list[dict]) -> list[str]:
    """Extrai os IDs publicos de produtos priorizando o contrato atual do backend."""
    ids_produtos: list[str] = []
    for item in dados_produtos:
        identificador = item.get("id_produto")
        if identificador in (None, ""):
            identificador = item.get("id_item")
        if identificador not in (None, ""):
            ids_produtos.append(str(identificador))
    return ids_produtos


def _registrar_consulta_camadas(cliente: TestClient, cnpj: str) -> dict:
    """Consulta as camadas operacionais para validar listagem, leitura e manifesto."""
    relatorio_camadas: dict[str, dict] = {}
    for camada in ("extraidos", "silver", "parquets"):
        resposta_lista = cliente.get(f"/api/tabelas/{cnpj}?camada={camada}")
        resposta_lista.raise_for_status()
        payload_lista = resposta_lista.json()
        relatorio_camadas[camada] = {
            "status_code": resposta_lista.status_code,
            "total_tabelas": len(payload_lista.get("tabelas", [])),
        }

        tabelas = payload_lista.get("tabelas", [])
        if tabelas:
            nome_tabela = tabelas[0]["nome"]
            resposta_leitura = cliente.get(f"/api/tabelas/{cnpj}/{nome_tabela}?camada={camada}&pagina=1&por_pagina=5")
            resposta_leitura.raise_for_status()
            payload_leitura = resposta_leitura.json()
            relatorio_camadas[camada]["tabela_exemplo"] = nome_tabela
            relatorio_camadas[camada]["total_registros_exemplo"] = payload_leitura.get("total_registros", 0)

    resposta_manifesto = cliente.get(f"/api/storage/{cnpj}/manifesto")
    resposta_manifesto.raise_for_status()
    relatorio_camadas["manifesto"] = resposta_manifesto.json().get("manifesto", {})
    return relatorio_camadas


def executar_fluxo(cnpj: str, executar_extracao: bool, manter_alteracoes: bool = False) -> dict:
    cliente = TestClient(app)

    relatorio: dict = {"cnpj": cnpj}
    diretorio_cnpj = (RAIZ_BACKEND.parents[1] / "storage" / "CNPJ" / cnpj).resolve()
    caminho_agregacao = diretorio_cnpj / "edicoes" / "agregacao.json"
    caminho_fatores = diretorio_cnpj / "edicoes" / "fatores.json"
    diretorio_exportacoes = diretorio_cnpj / "exportacoes"

    estado_original: dict[str, Any] = {
        "agregacao": _carregar_bytes_se_existir(caminho_agregacao),
        "fatores": _carregar_bytes_se_existir(caminho_fatores),
        "exportacoes": {arquivo.name for arquivo in diretorio_exportacoes.glob("*")} if diretorio_exportacoes.exists() else set(),
    }

    try:
        resposta_pipeline = cliente.post(
            "/api/pipeline/executar",
            json={
                "cnpj": cnpj,
                "consultas": ["nfe", "nfce", "c170", "bloco_h", "reg0200", "reg0220"] if executar_extracao else [],
                "executar_extracao": executar_extracao,
            },
        )
        resposta_pipeline.raise_for_status()
        dados_pipeline = resposta_pipeline.json()
        relatorio["pipeline"] = {
            "status": dados_pipeline.get("status"),
            "tabelas_geradas": len(dados_pipeline.get("tabelas_geradas", [])),
            "erros": dados_pipeline.get("erros", []),
        }

        resposta_produtos = cliente.get(f"/api/tabelas/{cnpj}/produtos?pagina=1&por_pagina=5")
        resposta_produtos.raise_for_status()
        dados_produtos = resposta_produtos.json()
        relatorio["produtos"] = {
            "total_registros": dados_produtos.get("total_registros", 0),
        }

        ids_produtos = _extrair_ids_publicos_produtos(dados_produtos.get("dados", []))
        relatorio["camadas"] = _registrar_consulta_camadas(cliente, cnpj)

        if len(ids_produtos) >= 2:
            resposta_agregar = cliente.post(
                f"/api/agregacao/agregar?cnpj={cnpj}",
                json={
                    "ids_produtos": ids_produtos[:2],
                    "descricao_padrao": "E2E_GRUPO_TESTE",
                },
            )
            relatorio["agregacao"] = {
                "status_code": resposta_agregar.status_code,
                "status": resposta_agregar.json().get("status") if resposta_agregar.headers.get("content-type", "").startswith("application/json") else None,
            }

        resposta_fatores = cliente.get(f"/api/tabelas/{cnpj}/fatores_conversao?pagina=1&por_pagina=1")
        if resposta_fatores.status_code == 200:
            dados_fatores = resposta_fatores.json().get("dados", [])
            if dados_fatores:
                id_agrupado = dados_fatores[0].get("id_agrupado")
                if id_agrupado:
                    resposta_fator = cliente.put(
                        f"/api/conversao/fator?cnpj={cnpj}",
                        json={
                            "id_agrupado": id_agrupado,
                            "fator": 1.0,
                        },
                    )
                    relatorio["conversao"] = {
                        "status_code": resposta_fator.status_code,
                        "status": resposta_fator.json().get("status") if resposta_fator.headers.get("content-type", "").startswith("application/json") else None,
                    }

                    resposta_recalculo = cliente.post(f"/api/conversao/recalcular?cnpj={cnpj}")
                    relatorio["recalculo"] = {
                        "status_code": resposta_recalculo.status_code,
                        "status": resposta_recalculo.json().get("status") if resposta_recalculo.headers.get("content-type", "").startswith("application/json") else None,
                    }

        resposta_exportar = cliente.get(f"/api/exportar/{cnpj}/produtos?formato=csv")
        relatorio["exportacao"] = {
            "status_code": resposta_exportar.status_code,
            "content_type": resposta_exportar.headers.get("content-type"),
        }

        resposta_status = cliente.get(f"/api/pipeline/status/{cnpj}")
        if resposta_status.status_code == 200:
            dados_status = resposta_status.json()
            relatorio["integridade"] = {
                "completo": dados_status.get("completo"),
                "tabelas_ok": sum(1 for valor in dados_status.get("tabelas", {}).values() if valor),
            }

        for nome_tabela in ("fatores_conversao", "ajustes_e111", "st_itens", "mov_estoque", "aba_mensal", "aba_anual"):
            resposta_tabela = cliente.get(f"/api/tabelas/{cnpj}/{nome_tabela}?pagina=1&por_pagina=5")
            resposta_tabela.raise_for_status()
            payload_tabela = resposta_tabela.json()
            relatorio[nome_tabela] = {
                "total_registros": payload_tabela.get("total_registros", 0),
                "colunas": payload_tabela.get("colunas", []),
            }

        return relatorio
    finally:
        if not manter_alteracoes:
            _restaurar_arquivo_binario(caminho_agregacao, estado_original["agregacao"])
            _restaurar_arquivo_binario(caminho_fatores, estado_original["fatores"])

            if diretorio_exportacoes.exists():
                arquivos_atuais = {arquivo.name for arquivo in diretorio_exportacoes.glob("*")}
                for nome_arquivo in arquivos_atuais - estado_original["exportacoes"]:
                    (diretorio_exportacoes / nome_arquivo).unlink()

            resposta_restauracao = cliente.post(
                "/api/pipeline/executar",
                json={
                    "cnpj": cnpj,
                    "consultas": [],
                    "executar_extracao": False,
                },
            )
            resposta_restauracao.raise_for_status()


def main() -> None:
    parser = argparse.ArgumentParser(description="Executa fluxo E2E local do audit_react")
    parser.add_argument("cnpj", help="CNPJ alvo (com ou sem pontuacao)")
    parser.add_argument(
        "--executar-extracao",
        action="store_true",
        help="Executa etapa Oracle antes do pipeline (requer conexao Oracle valida)",
    )
    parser.add_argument(
        "--manter-alteracoes",
        action="store_true",
        help="Mantem edicoes e exportacoes geradas durante a validacao E2E no CNPJ alvo",
    )
    parser.add_argument("--saida", default="", help="Arquivo JSON opcional para salvar o relatorio")
    args = parser.parse_args()

    relatorio = executar_fluxo(
        args.cnpj,
        executar_extracao=args.executar_extracao,
        manter_alteracoes=args.manter_alteracoes,
    )

    if args.saida:
        caminho_saida = Path(args.saida)
        caminho_saida.parent.mkdir(parents=True, exist_ok=True)
        caminho_saida.write_text(json.dumps(relatorio, indent=2, ensure_ascii=False), encoding="utf-8")

    print(json.dumps(relatorio, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
