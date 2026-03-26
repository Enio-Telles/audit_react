"""Configuração central do backend baseada em variáveis de ambiente."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv


@dataclass(frozen=True)
class ConfiguracaoOracle:
    """Representa uma configuração de conexão Oracle."""

    host: str
    porta: int
    servico: str
    usuario: str
    senha: str

    @property
    def dsn(self) -> str:
        """Monta o DSN Oracle no formato host:porta/servico."""
        return f"{self.host}:{self.porta}/{self.servico}"


@dataclass(frozen=True)
class ConfiguracaoBackend:
    """Representa configurações principais do backend."""

    diretorio_base_cnpj: str
    nivel_log: str


def _carregar_arquivo_env() -> None:
    """Carrega variáveis de ambiente a partir do .env da raiz do projeto."""
    arquivo_env = Path(__file__).resolve().parents[2] / ".env"
    if arquivo_env.exists():
        load_dotenv(arquivo_env, override=False)


def _obter_valor_obrigatorio(chave: str) -> str:
    """Obtém variável obrigatória do ambiente e falha com mensagem clara."""
    valor = os.getenv(chave)
    if valor is None or valor.strip() == "":
        raise ValueError(f"Variável obrigatória não configurada: {chave}")
    return valor


def _obter_porta_oracle(sufixo: str) -> int:
    """Obtém porta Oracle como inteiro validado."""
    valor = _obter_valor_obrigatorio(f"ORACLE_PORT{sufixo}")
    try:
        return int(valor)
    except ValueError as erro:
        raise ValueError(
            f"Valor inválido para ORACLE_PORT{sufixo}: '{valor}'. Use número inteiro."
        ) from erro


def _obter_usuario_oracle(sufixo: str) -> str:
    """Obtém usuário Oracle priorizando DB_USER e aceitando ORACLE_USER como fallback."""
    usuario = os.getenv(f"DB_USER{sufixo}") or os.getenv(f"ORACLE_USER{sufixo}")
    if usuario is None or usuario.strip() == "":
        raise ValueError(
            f"Variável obrigatória não configurada: DB_USER{sufixo} (ou ORACLE_USER{sufixo})"
        )
    return usuario


def _obter_senha_oracle(sufixo: str) -> str:
    """Obtém senha Oracle priorizando DB_PASSWORD e aceitando ORACLE_PASSWORD como fallback."""
    senha = os.getenv(f"DB_PASSWORD{sufixo}") or os.getenv(f"ORACLE_PASSWORD{sufixo}")
    if senha is None or senha.strip() == "":
        raise ValueError(
            "Variável obrigatória não configurada: "
            f"DB_PASSWORD{sufixo} (ou ORACLE_PASSWORD{sufixo})"
        )
    return senha


def obter_configuracao_oracle(indice: int = 0) -> ConfiguracaoOracle:
    """Lê configuração Oracle do .env para conexão com banco fiscal."""
    _carregar_arquivo_env()
    sufixo = "" if indice == 0 else f"_{indice}"

    return ConfiguracaoOracle(
        host=_obter_valor_obrigatorio(f"ORACLE_HOST{sufixo}"),
        porta=_obter_porta_oracle(sufixo),
        servico=_obter_valor_obrigatorio(f"ORACLE_SERVICE{sufixo}"),
        usuario=_obter_usuario_oracle(sufixo),
        senha=_obter_senha_oracle(sufixo),
    )


def obter_configuracao_backend() -> ConfiguracaoBackend:
    """Lê configurações de backend do .env."""
    _carregar_arquivo_env()

    diretorio_base_cnpj = os.getenv("STORAGE_BASE_DIR", "/storage/CNPJ")
    nivel_log = os.getenv("LOG_LEVEL", "INFO").upper()

    return ConfiguracaoBackend(
        diretorio_base_cnpj=diretorio_base_cnpj,
        nivel_log=nivel_log,
    )

