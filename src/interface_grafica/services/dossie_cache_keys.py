"""
Compatibilidade temporaria para a antiga localizacao das chaves de cache do dossie.
"""
from __future__ import annotations

from importlib import import_module as _import_module
import sys as _sys

_sys.modules[__name__] = _import_module("utilitarios.dossie_cache_keys")
