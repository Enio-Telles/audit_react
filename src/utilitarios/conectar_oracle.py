"""
Modulo de conexao Oracle.

Resolve credenciais dinamicamente a partir de candidatos de `.env`, com
fallback explicito para ambientes externos reutilizados pelo projeto.
"""
from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import oracledb
from dotenv import dotenv_values, load_dotenv
from rich import print as rprint

from utilitarios.project_paths import ENV_PATH

logger = logging.getLogger(__name__)

DEFAULT_HOST = "exa01-scan.sefin.ro.gov.br"
DEFAULT_PORT = 1521
DEFAULT_SERVICE = "sefindw"

EXTERNAL_ENV_CANDIDATES = (
    Path(r"C:\sist_react_01\sql\.env"),
    Path(r"C:\sist_react_01\.env"),
)


def listar_caminhos_env_oracle() -> list[Path]:
    """Retorna candidatos de env em ordem de preferencia."""

    explicit = os.getenv("AUDIT_REACT_ORACLE_ENV_PATH", "").strip()
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    candidates.extend(
        [
            ENV_PATH,
            Path.cwd() / ".env",
        ]
    )
    candidates.extend(EXTERNAL_ENV_CANDIDATES)

    resolved: list[Path] = []
    seen: set[str] = set()
    for item in candidates:
        key = str(item).lower()
        if key in seen:
            continue
        seen.add(key)
        resolved.append(item)
    return resolved


def _ler_env(caminho: Path) -> dict[str, str]:
    for encoding in ("utf-8", "latin-1"):
        try:
            valores = dotenv_values(caminho, encoding=encoding)
            return {str(k): str(v) for k, v in valores.items() if k and v is not None}
        except Exception:
            continue
    return {}


def _selecionar_env_oracle() -> tuple[Path | None, dict[str, str]]:
    chaves_relevantes = {"ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "DB_USER", "DB_PASSWORD"}
    for caminho in listar_caminhos_env_oracle():
        if not caminho.exists():
            continue
        valores = _ler_env(caminho)
        if any((valores.get(chave, "") or "").strip() for chave in chaves_relevantes):
            return caminho, valores
    return None, {}


def carregar_env_oracle() -> Path | None:
    """Carrega o primeiro `.env` Oracle valido encontrado."""

    caminho, valores = _selecionar_env_oracle()
    if caminho is None:
        return None

    for chave, valor in valores.items():
        if chave in {"ORACLE_HOST", "ORACLE_PORT", "ORACLE_SERVICE", "DB_USER", "DB_PASSWORD"}:
            os.environ[chave] = valor

    # Mantem comportamento conhecido do dotenv para outros consumers.
    for encoding in ("utf-8", "latin-1"):
        try:
            load_dotenv(dotenv_path=caminho, encoding=encoding, override=True)
            break
        except Exception:
            continue
    return caminho


def obter_configuracao_oracle() -> dict[str, Any]:
    """Resolve a configuracao Oracle atual, com fallback externo."""

    origem = carregar_env_oracle()

    host = os.getenv("ORACLE_HOST", DEFAULT_HOST).strip() or DEFAULT_HOST
    service = os.getenv("ORACLE_SERVICE", DEFAULT_SERVICE).strip() or DEFAULT_SERVICE

    try:
        port = int((os.getenv("ORACLE_PORT", str(DEFAULT_PORT)) or str(DEFAULT_PORT)).strip())
    except Exception:
        logger.warning("ORACLE_PORT invalido; usando padrao %s", DEFAULT_PORT)
        port = DEFAULT_PORT

    return {
        "host": host,
        "port": port,
        "service": service,
        "user": os.getenv("DB_USER", "").strip(),
        "password": os.getenv("DB_PASSWORD", "").strip(),
        "env_path": origem,
        "env_candidates": [str(item) for item in listar_caminhos_env_oracle()],
    }


def conectar(cpf_usuario=None, senha=None):
    """
    Funcao legada para compatibilidade.
    Recomendado usar `obter_conexao_oracle` como context manager.
    """

    cfg = obter_configuracao_oracle()
    usuario = str(cpf_usuario or cfg["user"] or "").strip()
    senha_final = str(senha or cfg["password"] or "").strip()

    if not usuario or not senha_final:
        origem = str(cfg["env_path"]) if cfg["env_path"] else "nenhum arquivo encontrado"
        rprint(
            "[red]Erro:[/red] Credenciais (DB_USER/DB_PASSWORD) nao encontradas. "
            f"Origem resolvida: {origem}"
        )
        return None

    try:
        dsn = oracledb.makedsn(cfg["host"], cfg["port"], service_name=cfg["service"])
        conexao = oracledb.connect(user=usuario, password=senha_final, dsn=dsn)

        with conexao.cursor() as cursor:
            cursor.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'")

        return conexao
    except Exception as exc:
        logger.error("Detalhes internos do erro de conexao Oracle", exc_info=exc)
        rprint(
            "[red]Erro de conexao Oracle.[/red] "
            f"Host={cfg['host']} service={cfg['service']} "
            f"env={cfg['env_path'] or 'nao localizado'}"
        )
        return None


@contextmanager
def obter_conexao_oracle(user=None, password=None):
    """Context manager seguro para conexoes Oracle."""

    conn = conectar(user, password)
    if conn is None:
        raise ConnectionError("Nao foi possivel estabelecer conexao com o Oracle.")
    try:
        yield conn
    finally:
        try:
            conn.close()
        except Exception as exc:
            logger.debug("Falha ao encerrar conexao Oracle", exc_info=exc)


_cfg_inicial = obter_configuracao_oracle()
HOST = _cfg_inicial["host"]
PORTA = _cfg_inicial["port"]
SERVICO = _cfg_inicial["service"]


if __name__ == "__main__":
    try:
        with obter_conexao_oracle() as conn:
            rprint("[green]Conexao via Context Manager estabelecida com sucesso![/green]")
            conn.close()
    except Exception as exc:
        logger.error("Detalhes internos da falha no teste de conexao", exc_info=exc)
        rprint("[red]Falha no teste de conexao. Verifique os logs para mais detalhes.[/red]")
