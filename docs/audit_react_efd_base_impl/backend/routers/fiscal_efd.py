"""
Router canônico do domínio EFD.

Endpoints:
- GET /api/fiscal/efd/records
- GET /api/fiscal/efd/dictionary/{record}
- GET /api/fiscal/efd/manifest/{record}
- GET /api/fiscal/efd/dataset/{record}
- GET /api/fiscal/efd/tree/documents
- GET /api/fiscal/efd/compare/{record}
- GET /api/fiscal/efd/row-provenance/{record}

A implementação delega as regras de exploração ao EfdService.
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from backend.services.efd_service import EfdService

router = APIRouter(prefix="/api/fiscal/efd", tags=["fiscal-efd"])
service = EfdService()


@router.get("/records")
def list_records() -> list[dict[str, Any]]:
    return service.list_records()


@router.get("/dictionary/{record}")
def get_dictionary(record: str) -> dict[str, Any]:
    try:
        return {"record": record, "fields": service.get_dictionary(record)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/manifest/{record}")
def get_manifest(record: str, cnpj: Optional[str] = Query(default=None)) -> dict[str, Any]:
    try:
        return service.get_manifest(record=record, cnpj=cnpj)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/dataset/{record}")
def read_dataset(
    record: str,
    cnpj: Optional[str] = Query(default=None),
    periodo: Optional[str] = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=1, le=5000),
    prefer_layer: Optional[str] = Query(default=None),
    columns: Optional[str] = Query(default=None, description="Lista de colunas separadas por vírgula."),
    filters: Optional[str] = Query(default=None, description="Filtros campo=valor separados por ';'."),
) -> dict[str, Any]:
    parsed_columns = [c.strip() for c in columns.split(",")] if columns else None
    parsed_filters: dict[str, str] = {}
    if filters:
        for item in filters.split(";"):
            if not item.strip():
                continue
            if "=" not in item:
                raise HTTPException(status_code=400, detail=f"Filtro inválido: {item}")
            key, value = item.split("=", 1)
            parsed_filters[key.strip()] = value.strip()

    try:
        return service.read_record(
            record=record,
            cnpj=cnpj,
            periodo=periodo,
            filters=parsed_filters or None,
            columns=parsed_columns,
            page=page,
            page_size=page_size,
            prefer_layer=prefer_layer,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/tree/documents")
def get_document_tree(
    cnpj: str = Query(...),
    periodo: Optional[str] = Query(default=None),
    chave_documento: Optional[str] = Query(default=None),
    limit_docs: int = Query(default=50, ge=1, le=500),
) -> dict[str, Any]:
    try:
        return service.build_document_tree(
            cnpj=cnpj,
            periodo=periodo,
            chave_documento=chave_documento,
            limit_docs=limit_docs,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/compare/{record}")
def compare_periods(
    record: str,
    cnpj: str = Query(...),
    periodo_a: str = Query(...),
    periodo_b: str = Query(...),
    limit: int = Query(default=200, ge=1, le=5000),
    key_field: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    try:
        return service.compare_periods(
            record=record,
            cnpj=cnpj,
            periodo_a=periodo_a,
            periodo_b=periodo_b,
            limit=limit,
            key_field=key_field,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/row-provenance/{record}")
def row_provenance(
    record: str,
    row_identifier: str = Query(...),
    cnpj: Optional[str] = Query(default=None),
    key_field: Optional[str] = Query(default=None),
    prefer_layer: Optional[str] = Query(default=None),
) -> dict[str, Any]:
    try:
        return service.row_provenance(
            record=record,
            cnpj=cnpj,
            row_identifier=row_identifier,
            key_field=key_field,
            prefer_layer=prefer_layer,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
