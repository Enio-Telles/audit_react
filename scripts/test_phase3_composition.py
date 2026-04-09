"""
Smoke test manual da Fase 3 de composicoes Polars.

Uso:
    python scripts/test_phase3_composition.py <CNPJ> [data_limite_processamento]

Exemplo:
    python scripts/test_phase3_composition.py 37671507000187 31/03/2024
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _PROJECT_ROOT / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from interface_grafica.services.dossie_dataset_reuse import carregar_dataset_reutilizavel
from transformacao.composicao_dif_icms import atualizar_composicao_dif_icms
from transformacao.composicao_enderecos import atualizar_composicao_enderecos
from transformacao.composicao_fronteira import atualizar_composicao_fronteira
from utilitarios.dataset_registry import encontrar_dataset

logging.basicConfig(level=logging.INFO)


def _imprimir_status_base(cnpj: str, dataset_id: str, rotulo: str) -> None:
    dataset = encontrar_dataset(cnpj, dataset_id)
    print(f"{rotulo}: {'OK' if dataset else 'MISSING'}")


def _testar_composicao_dif_icms(cnpj: str) -> None:
    print("\n--- Testando composicao direta: dif_ICMS_NFe_EFD.sql ---")
    caminho = atualizar_composicao_dif_icms(cnpj)
    if caminho and caminho.exists():
        print(f"Sucesso: {caminho}")
    else:
        print("Falha ao gerar composicao de diferenca de ICMS.")

    print("\n--- Testando reuso: dif_ICMS_NFe_EFD.sql ---")
    dataset = carregar_dataset_reutilizavel(cnpj, "dif_ICMS_NFe_EFD.sql")
    if dataset and dataset.reutilizado:
        print(f"Sucesso no reuso. Linhas: {len(dataset.dataframe)}")
    else:
        print("Falha no reuso da composicao de diferenca de ICMS.")


def _testar_composicao_fronteira(cnpj: str, data_limite_processamento: str | None) -> None:
    print("\n--- Testando composicao direta: fronteira.sql ---")
    caminho = atualizar_composicao_fronteira(cnpj, data_limite_processamento=data_limite_processamento)
    if caminho and caminho.exists():
        print(f"Sucesso: {caminho}")
    else:
        print("Falha ao gerar composicao de fronteira.")

    print("\n--- Testando reuso: fronteira.sql ---")
    dataset = carregar_dataset_reutilizavel(
        cnpj,
        "fronteira.sql",
        parametros={"data_limite_processamento": data_limite_processamento} if data_limite_processamento else None,
    )
    if dataset and dataset.reutilizado:
        print(f"Sucesso no reuso. Linhas: {len(dataset.dataframe)}")
    else:
        print("Falha no reuso da composicao de fronteira.")


def _testar_enderecos(cnpj: str) -> None:
    print("\n--- Testando avaliacao conservadora: dossie_enderecos.sql ---")
    caminho = atualizar_composicao_enderecos(cnpj)
    if caminho and caminho.exists():
        print(f"Composicao ativada: {caminho}")
    else:
        print(
            "Composicao nao ativada. "
            "Resultado esperado quando o dataset cadastral compartilhado ainda nao "
            "expõe os campos detalhados do endereco oficial."
        )


def main() -> None:
    if len(sys.argv) < 2:
        print("Uso: python scripts/test_phase3_composition.py <CNPJ> [data_limite_processamento]")
        raise SystemExit(1)

    cnpj = "".join(caractere for caractere in sys.argv[1] if caractere.isdigit())
    data_limite_processamento = sys.argv[2].strip() if len(sys.argv) > 2 else None

    print(f"--- Verificando CNPJ: {cnpj} ---")
    if data_limite_processamento:
        print(f"Data limite processamento: {data_limite_processamento}")

    _imprimir_status_base(cnpj, "nfe_base", "Base NFe")
    _imprimir_status_base(cnpj, "nfce_base", "Base NFCe")
    _imprimir_status_base(cnpj, "efd_c100", "Base EFD C100")
    _imprimir_status_base(cnpj, "sitafe_calculo_item", "Base SITAFE calculo item")
    _imprimir_status_base(cnpj, "cadastral", "Base cadastral")

    _testar_composicao_dif_icms(cnpj)
    _testar_composicao_fronteira(cnpj, data_limite_processamento)
    _testar_enderecos(cnpj)


if __name__ == "__main__":
    main()
