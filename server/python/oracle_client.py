"""Cliente de conexão Oracle usando configurações vindas do .env."""

from __future__ import annotations

from typing import Any

import oracledb

from configuracao_backend import obter_configuracao_oracle


def criar_conexao_oracle(indice: int = 0) -> Any:
    """Cria conexão Oracle com base nas variáveis do .env."""
    configuracao = obter_configuracao_oracle(indice=indice)
    return oracledb.connect(
        user=configuracao.usuario,
        password=configuracao.senha,
        dsn=configuracao.dsn,
    )

