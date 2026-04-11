"""
Compatibilidade temporaria para a antiga localizacao do servico de agregacao.

O modulo canonico agora vive em `utilitarios.servico_agregacao`.
"""
from __future__ import annotations

from utilitarios.servico_agregacao import ServicoAgregacao

__all__ = ["ServicoAgregacao"]
