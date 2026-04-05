from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from fastapi import APIRouter, BackgroundTasks, HTTPException
from pydantic import BaseModel

from interface_grafica.config import CNPJ_ROOT
from interface_grafica.services.registry_service import RegistryService

router = APIRouter()
registry = RegistryService()

_pipeline_status: dict[str, dict] = {}


class PipelineRequest(BaseModel):
    cnpj: str
    tabelas: list[str] = []
    data_limite: str | None = None


def _sanitize(cnpj: str) -> str:
    return re.sub(r"\D", "", cnpj or "")


def _run_pipeline(cnpj: str, tabelas: list[str], data_limite: str | None) -> None:
    _pipeline_status[cnpj] = {"status": "running", "progresso": [], "erros": []}
    try:
        from interface_grafica.services.pipeline_funcoes_service import ServicoPipelineCompleto
        from interface_grafica.services.sql_service import SqlService
        svc = ServicoPipelineCompleto()
        result = svc.executar_completo(
            cnpj=cnpj,
            consultas=[],
            tabelas=tabelas,
            data_limite=data_limite,
            progresso=lambda msg: _pipeline_status[cnpj]["progresso"].append(msg),
        )
        if result.ok:
            registry.upsert(cnpj, ran_now=True)
            _pipeline_status[cnpj]["status"] = "done"
        else:
            _pipeline_status[cnpj]["status"] = "error"
            _pipeline_status[cnpj]["erros"] = result.erros
    except Exception as exc:
        _pipeline_status[cnpj]["status"] = "error"
        _pipeline_status[cnpj]["erros"] = [str(exc)]


@router.post("/run")
def run_pipeline(req: PipelineRequest, background_tasks: BackgroundTasks):
    cnpj = _sanitize(req.cnpj)
    if not cnpj:
        raise HTTPException(400, "CNPJ inválido")
    if _pipeline_status.get(cnpj, {}).get("status") == "running":
        raise HTTPException(409, "Pipeline já em execução para este CNPJ")
    _pipeline_status[cnpj] = {"status": "queued", "progresso": [], "erros": []}
    background_tasks.add_task(_run_pipeline, cnpj, req.tabelas, req.data_limite)
    return {"cnpj": cnpj, "status": "queued"}


@router.get("/status/{cnpj}")
def get_status(cnpj: str):
    cnpj = _sanitize(cnpj)
    return _pipeline_status.get(cnpj, {"status": "idle", "progresso": [], "erros": []})
