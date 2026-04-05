from __future__ import annotations

import re
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from interface_grafica.services.registry_service import RegistryService
from interface_grafica.services.parquet_service import ParquetService
from interface_grafica.config import CNPJ_ROOT

router = APIRouter()
registry = RegistryService()


class CNPJAdd(BaseModel):
    cnpj: str


def _sanitize(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


@router.get("")
def list_cnpjs():
    records = registry.list_records()
    svc = ParquetService(CNPJ_ROOT)
    discovered = svc.list_cnpjs()
    seen = {r.cnpj for r in records}
    for d in discovered:
        if d not in seen:
            registry.upsert(d)
            seen.add(d)
    return [
        {"cnpj": r.cnpj, "added_at": r.added_at, "last_run_at": r.last_run_at}
        for r in registry.list_records()
    ]


@router.post("")
def add_cnpj(body: CNPJAdd):
    cnpj = _sanitize(body.cnpj)
    if len(cnpj) < 11:
        raise HTTPException(400, "CNPJ inválido")
    record = registry.upsert(cnpj)
    return {"cnpj": record.cnpj, "added_at": record.added_at, "last_run_at": record.last_run_at}


@router.delete("/{cnpj}")
def remove_cnpj(cnpj: str):
    cnpj = _sanitize(cnpj)
    # Remove from registry only - does not delete data files
    raw = registry._load_raw()
    new_raw = [r for r in raw if r["cnpj"] != cnpj]
    registry._save_raw(new_raw)
    return {"removed": cnpj}


@router.get("/{cnpj}/files")
def list_files(cnpj: str):
    cnpj = _sanitize(cnpj)
    svc = ParquetService(CNPJ_ROOT)
    files = svc.list_parquet_files(cnpj)
    return [
        {"name": p.name, "path": str(p), "size": p.stat().st_size if p.exists() else 0}
        for p in files
    ]


@router.get("/{cnpj}/schema")
def get_schema(cnpj: str, path: str):
    p = Path(path)
    if not p.exists():
        raise HTTPException(404, "Arquivo não encontrado")
    svc = ParquetService(CNPJ_ROOT)
    cols = svc.get_schema(p)
    return {"columns": cols}
