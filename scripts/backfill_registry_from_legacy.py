"""
Backfill seguro do registry canonico a partir de Parquets legados ja existentes.

Uso:
    python scripts/backfill_registry_from_legacy.py <CNPJ> [<CNPJ> ...]

Objetivo:
- copiar datasets legados para o caminho canonico do registry;
- preservar o legado como fallback;
- registrar metadata auditavel informando a origem do backfill.
"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _PROJECT_ROOT / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

import polars as pl

from utilitarios.dataset_registry import CATALOGO
from utilitarios.dataset_registry import criar_metadata
from utilitarios.dataset_registry import listar_caminhos_com_fallback
from utilitarios.dataset_registry import obter_caminho
from utilitarios.dataset_registry import registrar_dataset


def _normalizar_cnpj(cnpj: str) -> str:
    return "".join(caractere for caractere in str(cnpj) if caractere.isdigit())


def backfill_cnpj(cnpj: str) -> list[dict[str, str]]:
    """Replica datasets legados no caminho canonico quando o canonico ainda nao existe."""

    cnpj_limpo = _normalizar_cnpj(cnpj)
    resultados: list[dict[str, str]] = []

    for definicao in CATALOGO:
        if definicao.tipo == "dimensao_global":
            continue

        caminhos = listar_caminhos_com_fallback(cnpj_limpo, definicao.dataset_id)
        if not caminhos:
            resultados.append(
                {"dataset_id": definicao.dataset_id, "status": "sem_caminho", "detalhe": "dataset sem resolucao"}
            )
            continue

        caminho_canonico = caminhos[0]
        if caminho_canonico.exists():
            resultados.append(
                {"dataset_id": definicao.dataset_id, "status": "ja_canonico", "detalhe": str(caminho_canonico)}
            )
            continue

        caminho_legado = next((caminho for caminho in caminhos[1:] if caminho.exists()), None)
        if caminho_legado is None:
            resultados.append(
                {"dataset_id": definicao.dataset_id, "status": "sem_legado", "detalhe": "nenhum parquet legado encontrado"}
            )
            continue

        dataframe = pl.read_parquet(caminho_legado)
        metadata = criar_metadata(
            cnpj=cnpj_limpo,
            dataset_id=definicao.dataset_id,
            sql_id=definicao.sql_id,
            parametros={
                "modo": "backfill_legado",
                "arquivo_legado_origem": str(caminho_legado),
                "tabela_origem": list(definicao.tabelas_oracle),
            },
            linhas=dataframe.height,
        )
        caminho_registrado = registrar_dataset(cnpj_limpo, definicao.dataset_id, dataframe, metadata=metadata)
        resultados.append(
            {
                "dataset_id": definicao.dataset_id,
                "status": "backfill_ok" if caminho_registrado else "erro_backfill",
                "detalhe": str(caminho_legado),
            }
        )

    return resultados


def main() -> None:
    if len(sys.argv) < 2:
        print("Uso: python scripts/backfill_registry_from_legacy.py <CNPJ> [<CNPJ> ...]")
        raise SystemExit(1)

    for argumento in sys.argv[1:]:
        cnpj = _normalizar_cnpj(argumento)
        print(f"\n=== Backfill do CNPJ {cnpj} ===")
        for resultado in backfill_cnpj(cnpj):
            print(f"{resultado['dataset_id']}: {resultado['status']} :: {resultado['detalhe']}")


if __name__ == "__main__":
    main()
