"""
Compatibilidade temporaria para a antiga localizacao do builder do dossie.
"""
from __future__ import annotations

from importlib import import_module as _import_module
import sys as _sys

_sys.modules[__name__] = _import_module("utilitarios.dossie_section_builder")
