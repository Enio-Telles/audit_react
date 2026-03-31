"""Utilitarios para organizacao das camadas por CNPJ."""

from __future__ import annotations

from pathlib import Path
from typing import Iterable


CAMADAS_CNPJ_PADRAO = ("extraidos", "silver", "parquets", "edicoes", "exportacoes")


def garantir_estrutura_camadas_cnpj(
    diretorio_cnpj: Path,
    camadas: Iterable[str] = CAMADAS_CNPJ_PADRAO,
) -> dict[str, Path]:
    """Garante a estrutura padrao de diretorios operacionais por CNPJ."""
    diretorios: dict[str, Path] = {}

    for camada in camadas:
        diretorio_camada = diretorio_cnpj / camada
        diretorio_camada.mkdir(parents=True, exist_ok=True)
        diretorios[camada] = diretorio_camada

    return diretorios


def obter_diretorio_camada(diretorio_cnpj: Path, camada: str) -> Path:
    """Resolve uma camada valida de armazenamento do CNPJ."""
    if camada not in CAMADAS_CNPJ_PADRAO:
        raise ValueError(f"Camada invalida: {camada}")

    return diretorio_cnpj / camada
