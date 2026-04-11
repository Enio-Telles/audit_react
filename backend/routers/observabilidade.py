from __future__ import annotations

import socket
from urllib.parse import urlparse

from fastapi import APIRouter
from pydantic import BaseModel

from observabilidade.openlineage import LineageDataset, get_openlineage_emitter
from utilitarios.dataset_registry import catalogo_resumido
from utilitarios.delta_lake import get_delta_runtime_config
from utilitarios.schema_registry import SchemaRegistry
from utilitarios.sql_cache import get_sql_catalog_cache

from .fiscal_catalog_inspector import availability_for_cnpj, catalog_status, inspect_dataset

router = APIRouter()


class OpenLineageTestRequest(BaseModel):
    cnpj: str = "00000000000000"
    job_name: str = "audit_react.smoke"


def _socket_check(url: str) -> dict:
    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    try:
        with socket.create_connection((host, port), timeout=2):
            return {"url": url, "host": host, "port": port, "reachable": True}
    except OSError as exc:
        return {
            "url": url,
            "host": host,
            "port": port,
            "reachable": False,
            "error": str(exc),
        }


@router.get("/status")
def status() -> dict:
    emitter = get_openlineage_emitter("audit_react.api")
    schema_registry = SchemaRegistry()
    return {
        "sql_cache": get_sql_catalog_cache().stats(),
        "openlineage": emitter.status(),
        "delta": get_delta_runtime_config(),
        "dataset_catalog": catalogo_resumido(),
        "schema_registry": schema_registry.summary(),
    }


@router.get("/dataset-catalog")
def dataset_catalog_status() -> dict:
    return catalog_status()


@router.get("/dataset-catalog/{cnpj}")
def dataset_catalog_for_cnpj(cnpj: str) -> dict:
    return availability_for_cnpj(cnpj)


@router.get("/dataset-catalog/{cnpj}/{dataset_id}")
def dataset_catalog_dataset(cnpj: str, dataset_id: str, limit: int = 20) -> dict:
    return inspect_dataset(cnpj, dataset_id, limit=limit)


@router.get("/schema-registry")
def schema_registry_status() -> dict:
    registry = SchemaRegistry()
    return registry.summary()


@router.get("/stack-smoke")
def stack_smoke() -> dict:
    return {
        "prometheus": _socket_check("http://localhost:9090"),
        "grafana": _socket_check("http://localhost:3001"),
        "marquez_api": _socket_check("http://localhost:5000"),
        "marquez_ui": _socket_check("http://localhost:3002"),
        "api": _socket_check("http://localhost:8000"),
    }


@router.post("/openlineage/test")
def openlineage_test(req: OpenLineageTestRequest) -> dict:
    emitter = get_openlineage_emitter(req.job_name)
    if not emitter.enabled:
        return {
            "ok": False,
            "message": "OPENLINEAGE_URL nao configurado.",
            "status": emitter.status(),
        }

    output = LineageDataset(namespace="debug", name=f"{req.cnpj}/smoke")
    emitter.start_run(cnpj=req.cnpj)
    emitter.emit_step_complete("smoke", outputs=[output], duration_seconds=0.0)
    emitter.complete_run(outputs=[output], success=True)
    return {
        "ok": True,
        "message": "evento sintetico emitido",
        "status": emitter.status(),
        "output": output.as_dict(),
    }
