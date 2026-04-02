"""Gera manifesto operacional por CNPJ."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from .camadas_cnpj import CAMADAS_CNPJ_PADRAO, garantir_estrutura_camadas_cnpj

VERSAO_REGRA_CORE_FISCAL = "core-fiscal-cnpj-parquet-polars-v2-st"


def _coletar_metadados_parquet(caminho: Path) -> dict[str, Any]:
    """Coleta metadados minimos de um parquet sem alterar seu conteudo."""
    # ⚡ BOLT: Otimização de performance. Evita carregar o Parquet na memória inteiramente.
    # Usa lazy evaluation (read_parquet_schema e scan_parquet) para obter dados sem ler as linhas.
    schema = pl.read_parquet_schema(caminho)
    estatisticas = caminho.stat()

    return {
        "nome": caminho.stem,
        "arquivo": caminho.name,
        "caminho": str(caminho),
        "registros": pl.scan_parquet(caminho).select(pl.len()).collect().item(),
        "colunas": list(schema.names()),
        "schema": {coluna: str(tipo) for coluna, tipo in schema.items()},
        "tamanho_bytes": estatisticas.st_size,
        "atualizado_em": datetime.fromtimestamp(estatisticas.st_mtime, tz=timezone.utc).isoformat(),
    }


def gerar_manifesto_cnpj(
    diretorio_cnpj: Path,
    *,
    cnpj: str,
    versao_regra: str = VERSAO_REGRA_CORE_FISCAL,
) -> dict[str, Any]:
    """Gera manifesto operacional consolidando as tabelas por camada."""
    garantir_estrutura_camadas_cnpj(diretorio_cnpj)

    camadas: dict[str, Any] = {}
    total_tabelas = 0
    total_registros = 0

    for camada in CAMADAS_CNPJ_PADRAO:
        diretorio_camada = diretorio_cnpj / camada
        tabelas: list[dict[str, Any]] = []

        for arquivo in sorted(diretorio_camada.glob("*.parquet")):
            try:
                metadados = _coletar_metadados_parquet(arquivo)
            except Exception as erro:  # noqa: BLE001
                estatisticas = arquivo.stat()
                metadados = {
                    "nome": arquivo.stem,
                    "arquivo": arquivo.name,
                    "caminho": str(arquivo),
                    "registros": 0,
                    "colunas": [],
                    "schema": {},
                    "tamanho_bytes": estatisticas.st_size,
                    "atualizado_em": datetime.fromtimestamp(
                        estatisticas.st_mtime,
                        tz=timezone.utc,
                    ).isoformat(),
                    "erro": str(erro),
                }

            tabelas.append(metadados)
            total_tabelas += 1
            total_registros += int(metadados.get("registros", 0))

        camadas[camada] = {
            "diretorio": str(diretorio_camada),
            "total_tabelas": len(tabelas),
            "total_registros": sum(int(item.get("registros", 0)) for item in tabelas),
            "tabelas": tabelas,
        }

    manifesto = {
        "cnpj": cnpj,
        "gerado_em": datetime.now(tz=timezone.utc).isoformat(),
        "versao_regra": versao_regra,
        "camadas": camadas,
        "total_tabelas": total_tabelas,
        "total_registros": total_registros,
    }

    return manifesto
