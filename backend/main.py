from __future__ import annotations

import sys
from pathlib import Path

# Ensure src/ is on sys.path so existing services can be imported
SRC_DIR = Path(__file__).resolve().parents[1] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import cnpj, parquet, pipeline, estoque, aggregation, sql_query, fisconforme, oracle, ressarcimento, dossie

app = FastAPI(
    title="Fiscal Parquet API",
    description="REST API para o Fiscal Parquet",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def add_security_headers(request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "frame-ancestors 'none'"
    return response

app.include_router(cnpj.router, prefix="/api/cnpj", tags=["cnpj"])
app.include_router(parquet.router, prefix="/api/parquet", tags=["parquet"])
app.include_router(pipeline.router, prefix="/api/pipeline", tags=["pipeline"])
app.include_router(estoque.router, prefix="/api/estoque", tags=["estoque"])
app.include_router(ressarcimento.router, prefix="/api/ressarcimento", tags=["ressarcimento"])
app.include_router(aggregation.router, prefix="/api/aggregation", tags=["aggregation"])
app.include_router(sql_query.router, prefix="/api/sql", tags=["sql"])
app.include_router(fisconforme.router, prefix="/api/fisconforme", tags=["fisconforme"])
app.include_router(oracle.router, prefix="/api/oracle", tags=["oracle"])
app.include_router(dossie.router, prefix="/api/dossie", tags=["dossie"])


@app.get("/api/health")
def health():
    return {"status": "ok"}
