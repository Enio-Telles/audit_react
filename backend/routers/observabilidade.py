from __future__ import annotations

from fastapi import APIRouter

from observabilidade.openlineage import get_openlineage_emitter
from utilitarios.sql_cache import get_sql_catalog_cache

router = APIRouter()


@router.get("/status")
def status() -> dict:
    emitter = get_openlineage_emitter("audit_react.api")
    return {
        "sql_cache": get_sql_catalog_cache().stats(),
        "openlineage": emitter.status(),
    }
