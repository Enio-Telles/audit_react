"""Configuracao central do backend baseada em variaveis de ambiente."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


@dataclass(frozen=True)
class ConfiguracaoOracle:
    """Representa uma configuracao de conexao Oracle."""

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
    """Representa configuracoes principais do backend."""

    diretorio_base_cnpj: str
    nivel_log: str
    diretorio_consultas_sql: str


@dataclass(frozen=True)
class ResumoConfiguracaoOracle:
    """Representa dados nao sensiveis de uma conexao Oracle configurada."""

    indice: int
    host: str
    porta: int | None
    servico: str
    configurada: bool
    erro: str | None = None


def _carregar_arquivo_env() -> None:
    """Carrega variaveis de ambiente a partir do .env da raiz do projeto."""
    arquivo_env = Path(__file__).resolve().parents[2] / ".env"
    if arquivo_env.exists():
        load_dotenv(arquivo_env, override=False)


def _obter_valor_obrigatorio(chave: str) -> str:
    """Obtem variavel obrigatoria do ambiente e falha com mensagem clara."""
    valor = os.getenv(chave)
    if valor is None or valor.strip() == "":
        raise ValueError(f"Variavel obrigatoria nao configurada: {chave}")
    return valor


def _obter_valor_opcional(chave: str) -> str | None:
    """Obtem variavel opcional do ambiente removendo espacos laterais."""
    valor = os.getenv(chave)
    if valor is None:
        return None

    valor_normalizado = valor.strip()
    return valor_normalizado or None


def _obter_porta_oracle(sufixo: str) -> int:
    """Obtem porta Oracle como inteiro validado."""
    valor = _obter_valor_obrigatorio(f"ORACLE_PORT{sufixo}")
    try:
        return int(valor)
    except ValueError as erro:
        raise ValueError(
            f"Valor invalido para ORACLE_PORT{sufixo}: '{valor}'. Use numero inteiro."
        ) from erro


def _obter_usuario_oracle(sufixo: str) -> str:
    """Obtem usuario Oracle priorizando DB_USER e aceitando ORACLE_USER como fallback."""
    usuario = os.getenv(f"DB_USER{sufixo}") or os.getenv(f"ORACLE_USER{sufixo}")
    if usuario is None or usuario.strip() == "":
        raise ValueError(
            f"Variavel obrigatoria nao configurada: DB_USER{sufixo} (ou ORACLE_USER{sufixo})"
        )
    return usuario


def _obter_senha_oracle(sufixo: str) -> str:
    """Obtem senha Oracle priorizando DB_PASSWORD e aceitando ORACLE_PASSWORD como fallback."""
    senha = os.getenv(f"DB_PASSWORD{sufixo}") or os.getenv(f"ORACLE_PASSWORD{sufixo}")
    if senha is None or senha.strip() == "":
        raise ValueError(
            "Variavel obrigatoria nao configurada: "
            f"DB_PASSWORD{sufixo} (ou ORACLE_PASSWORD{sufixo})"
        )
    return senha


def _montar_sufixo_oracle(indice: int) -> str:
    """Converte indice numerico no sufixo esperado das variaveis Oracle."""
    return "" if indice == 0 else f"_{indice}"


def _listar_chaves_oracle(sufixo: str) -> list[str]:
    """Lista chaves de ambiente relacionadas a uma configuracao Oracle."""
    return [
        f"ORACLE_HOST{sufixo}",
        f"ORACLE_PORT{sufixo}",
        f"ORACLE_SERVICE{sufixo}",
        f"DB_USER{sufixo}",
        f"DB_PASSWORD{sufixo}",
        f"ORACLE_USER{sufixo}",
        f"ORACLE_PASSWORD{sufixo}",
    ]


def existe_variavel_oracle_configurada(indice: int = 0) -> bool:
    """Indica se ha qualquer variavel Oracle declarada para o indice informado."""
    _carregar_arquivo_env()
    sufixo = _montar_sufixo_oracle(indice)
    return any(_obter_valor_opcional(chave) for chave in _listar_chaves_oracle(sufixo))


def listar_resumos_configuracoes_oracle(
    indices_extras: list[int] | None = None,
    max_indices: int = 10,
) -> list[dict[str, Any]]:
    """Lista configuracoes Oracle disponiveis sem expor credenciais."""
    _carregar_arquivo_env()

    indices_detectados = {
        indice
        for indice in range(max_indices)
        if existe_variavel_oracle_configurada(indice)
    }

    for indice_extra in indices_extras or []:
        if 0 <= indice_extra < max_indices:
            indices_detectados.add(indice_extra)

    if not indices_detectados:
        indices_detectados.add(0)

    resumos: list[dict[str, Any]] = []
    for indice in sorted(indices_detectados):
        sufixo = _montar_sufixo_oracle(indice)

        try:
            configuracao = obter_configuracao_oracle(indice=indice)
            resumo = ResumoConfiguracaoOracle(
                indice=indice,
                host=configuracao.host,
                porta=configuracao.porta,
                servico=configuracao.servico,
                configurada=True,
                erro=None,
            )
        except Exception as erro:  # noqa: BLE001
            porta_bruta = _obter_valor_opcional(f"ORACLE_PORT{sufixo}")
            try:
                porta = int(porta_bruta) if porta_bruta is not None else None
            except ValueError:
                porta = None

            resumo = ResumoConfiguracaoOracle(
                indice=indice,
                host=_obter_valor_opcional(f"ORACLE_HOST{sufixo}") or "",
                porta=porta,
                servico=_obter_valor_opcional(f"ORACLE_SERVICE{sufixo}") or "",
                configurada=False,
                erro=str(erro),
            )

        resumos.append(
            {
                "indice": resumo.indice,
                "host": resumo.host,
                "porta": resumo.porta,
                "servico": resumo.servico,
                "configurada": resumo.configurada,
                "erro": resumo.erro,
            }
        )

    return resumos


def obter_configuracao_oracle(indice: int = 0) -> ConfiguracaoOracle:
    """Le configuracao Oracle do .env para conexao com banco fiscal."""
    _carregar_arquivo_env()
    sufixo = _montar_sufixo_oracle(indice)

    return ConfiguracaoOracle(
        host=_obter_valor_obrigatorio(f"ORACLE_HOST{sufixo}"),
        porta=_obter_porta_oracle(sufixo),
        servico=_obter_valor_obrigatorio(f"ORACLE_SERVICE{sufixo}"),
        usuario=_obter_usuario_oracle(sufixo),
        senha=_obter_senha_oracle(sufixo),
    )


def obter_configuracao_backend() -> ConfiguracaoBackend:
    """Le configuracoes de backend do .env."""
    _carregar_arquivo_env()

    diretorio_base_cnpj = os.getenv("STORAGE_BASE_DIR", "/storage/CNPJ")
    nivel_log = os.getenv("LOG_LEVEL", "INFO").upper()
    diretorio_consultas_padrao = (Path(__file__).resolve().parent / "consultas").as_posix()
    diretorio_consultas_sql = os.getenv("SQL_QUERIES_DIR", diretorio_consultas_padrao)

    return ConfiguracaoBackend(
        diretorio_base_cnpj=diretorio_base_cnpj,
        nivel_log=nivel_log,
        diretorio_consultas_sql=diretorio_consultas_sql,
    )
