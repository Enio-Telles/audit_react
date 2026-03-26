"""
audit_engine — Motor de Auditoria Fiscal
Módulo principal que registra todos os geradores e contratos.
"""

# Importar contratos (registra automaticamente)
from .contratos import base  # noqa: F401

# Importar módulos (registra geradores via decorator)
from . import tabelas  # noqa: F401

# Importar orquestrador
from .pipeline.orquestrador import OrquestradorPipeline  # noqa: F401

__version__ = "1.0.0"
