from __future__ import annotations

import socket
from urllib.parse import urlparse

from fastapi import APIRouter
from pydantic import BaseModel

from observabilidade.openlineage import LineageDataset, get_openlineage_emitter
from utilitarios.delta_lake import get_delta_runtime_config
from utilitarios.sql_cache import get_sql_catalog_cache

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
    return {
        "sql_cache": get_sql_catalog_cache().stats(),
        "openlineage": emitter.status(),
        "delta": get_delta_runtime_config(),
    }


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
