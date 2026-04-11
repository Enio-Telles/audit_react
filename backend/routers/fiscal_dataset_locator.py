from __future__ import annotations

from pathlib import Path

from utilitarios.dataset_registry import encontrar_dataset

from .fiscal_storage import resolve_materialized_path


def locate_dataset(cnpj: str, dataset_id: str, *fallbacks: Path) -> Path:
    localizado = encontrar_dataset(cnpj, dataset_id)
    if localizado is not None:
        return localizado.caminho

    for fallback in fallbacks:
        resolved = resolve_materialized_path(fallback)
        if resolved.exists():
            return resolved

    if fallbacks:
        return resolve_materialized_path(fallbacks[0])
    raise ValueError(f"Nenhum caminho disponível para dataset={dataset_id!r} cnpj={cnpj!r}")
