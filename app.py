"""Inicializador central do projeto audit_react.

Este arquivo concentra os fluxos de inicialização local para evitar que o
projeto dependa de múltiplos comandos soltos no terminal.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import platform
import shutil
import signal
import subprocess
import sys
import threading
from pathlib import Path
from typing import Iterable, Sequence


DIRETORIO_RAIZ = Path(__file__).resolve().parent
DIRETORIO_API = DIRETORIO_RAIZ / "server" / "python"


def criar_parser() -> argparse.ArgumentParser:
    """Cria o parser de linha de comando do inicializador."""
    parser = argparse.ArgumentParser(
        description="Centraliza a inicialização local do frontend e da API.",
    )
    subparsers = parser.add_subparsers(dest="comando", required=True)

    parser_dev = subparsers.add_parser(
        "dev",
        help="Inicia frontend Vite e API FastAPI em paralelo.",
    )
    parser_dev.add_argument(
        "--host-api",
        default="0.0.0.0",
        help="Host da API FastAPI.",
    )
    parser_dev.add_argument(
        "--porta-api",
        type=int,
        default=8000,
        help="Porta da API FastAPI.",
    )

    parser_api = subparsers.add_parser(
        "api",
        help="Inicia somente a API FastAPI.",
    )
    parser_api.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host da API FastAPI.",
    )
    parser_api.add_argument(
        "--porta",
        type=int,
        default=8000,
        help="Porta da API FastAPI.",
    )

    subparsers.add_parser(
        "web",
        help="Inicia somente o frontend Vite.",
    )
    subparsers.add_parser(
        "build",
        help="Executa o build do frontend/gateway Node.",
    )

    return parser


def localizar_executavel(nome: str) -> str:
    """Localiza um executável necessário no PATH."""
    executavel = shutil.which(nome)
    if executavel:
        return executavel

    if platform.system().lower().startswith("win"):
        executavel_cmd = shutil.which(f"{nome}.cmd")
        if executavel_cmd:
            return executavel_cmd

    raise FileNotFoundError(nome)


def montar_mensagem_ausencia_pnpm() -> str:
    """Monta instruções guiadas quando o pnpm não está disponível."""
    return "\n".join(
        [
            "Dependência ausente: 'pnpm' não foi encontrada no PATH.",
            "",
            "Como corrigir:",
            "1. Instale o Node.js: https://nodejs.org/",
            "2. Habilite o pnpm com Corepack: corepack enable",
            "3. Ative a versão do projeto: corepack prepare pnpm@10.4.1 --activate",
            "4. Instale as dependências do frontend: pnpm install",
            "",
            "Se preferir, também funciona instalar globalmente:",
            "npm install -g pnpm",
            "",
            f"Comando tentado a partir de: {DIRETORIO_RAIZ}",
        ]
    )


def montar_mensagem_ausencia_uvicorn() -> str:
    """Monta instruções guiadas quando o uvicorn não está instalado."""
    return "\n".join(
        [
            "Dependência ausente: o módulo Python 'uvicorn' não está disponível neste ambiente.",
            "",
            "Como corrigir no ambiente virtual atual:",
            f"1. Ative o ambiente Python desejado em: {sys.executable}",
            "2. Instale as dependências do backend:",
            "   pip install -r server/python/requirements.txt",
            "",
            "Se quiser instalar só o necessário para subir a API:",
            "pip install fastapi uvicorn",
            "",
            "Depois disso, execute novamente: python app.py",
        ]
    )


def montar_mensagem_frontend_nao_preparado() -> str:
    """Monta instruções quando as dependências do frontend ainda não foram instaladas."""
    return "\n".join(
        [
            "Frontend não está pronto para iniciar: dependências locais ausentes.",
            "",
            "Como corrigir:",
            "1. No diretório do projeto, execute: pnpm install",
            "2. Após instalar, rode novamente: python app.py",
            "",
            f"Diretório esperado do frontend: {DIRETORIO_RAIZ}",
        ]
    )


def garantir_pnpm_disponivel() -> str:
    """Valida a disponibilidade do pnpm e retorna o executável."""
    try:
        return localizar_executavel("pnpm")
    except FileNotFoundError as erro:
        raise SystemExit(montar_mensagem_ausencia_pnpm()) from erro


def garantir_uvicorn_disponivel() -> None:
    """Valida a disponibilidade do uvicorn no Python atual."""
    if importlib.util.find_spec("uvicorn") is None:
        raise SystemExit(montar_mensagem_ausencia_uvicorn())


def garantir_frontend_preparado(pnpm: str) -> None:
    """Valida se as dependências locais do frontend já foram instaladas."""
    diretorio_node_modules = DIRETORIO_RAIZ / "node_modules"
    if not diretorio_node_modules.exists():
        raise SystemExit(montar_mensagem_frontend_nao_preparado())

    resultado = subprocess.run(
        [pnpm, "exec", "vite", "--version"],
        cwd=DIRETORIO_RAIZ,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if resultado.returncode != 0:
        raise SystemExit(montar_mensagem_frontend_nao_preparado())


def montar_ambiente_api() -> dict[str, str]:
    """Monta ambiente da API garantindo que o pacote Python seja importável."""
    ambiente = os.environ.copy()
    pythonpath_atual = ambiente.get("PYTHONPATH", "")
    caminho_api = str(DIRETORIO_API)

    if pythonpath_atual:
        ambiente["PYTHONPATH"] = os.pathsep.join([caminho_api, pythonpath_atual])
    else:
        ambiente["PYTHONPATH"] = caminho_api

    return ambiente


def transmitir_saida(processo: subprocess.Popen[str], prefixo: str) -> None:
    """Replica a saída do subprocesso com prefixo para facilitar rastreabilidade."""
    assert processo.stdout is not None
    for linha in processo.stdout:
        print(f"[{prefixo}] {linha}", end="")


def iniciar_subprocesso(
    comando: Sequence[str],
    diretorio: Path,
    prefixo: str,
    ambiente: dict[str, str] | None = None,
) -> subprocess.Popen[str]:
    """Inicia subprocesso e prepara leitura de stdout/stderr de forma unificada."""
    processo = subprocess.Popen(
        comando,
        cwd=diretorio,
        env=ambiente,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    thread_saida = threading.Thread(
        target=transmitir_saida,
        args=(processo, prefixo),
        daemon=True,
    )
    thread_saida.start()
    return processo


def encerrar_processos(processos: Iterable[subprocess.Popen[str]]) -> None:
    """Encerra subprocessos filhos de maneira previsível."""
    for processo in processos:
        if processo.poll() is None:
            processo.terminate()

    for processo in processos:
        if processo.poll() is None:
            try:
                processo.wait(timeout=5)
            except subprocess.TimeoutExpired:
                processo.kill()


def aguardar_processos(processos: list[subprocess.Popen[str]]) -> int:
    """Aguarda subprocessos e encerra os demais quando um deles finalizar."""
    try:
        while True:
            for processo in processos:
                codigo = processo.poll()
                if codigo is not None:
                    encerrar_processos(processos)
                    return codigo
    except KeyboardInterrupt:
        print("\nEncerrando serviços...")
        encerrar_processos(processos)
        return 130


def executar_api(host: str, porta: int) -> int:
    """Inicia somente a API FastAPI."""
    garantir_uvicorn_disponivel()
    comando = [
        sys.executable,
        "-m",
        "uvicorn",
        "api:app",
        "--reload",
        "--host",
        host,
        "--port",
        str(porta),
    ]
    processo = iniciar_subprocesso(
        comando=comando,
        diretorio=DIRETORIO_API,
        prefixo="api",
        ambiente=montar_ambiente_api(),
    )
    return aguardar_processos([processo])


def executar_web() -> int:
    """Inicia somente o frontend Vite."""
    pnpm = garantir_pnpm_disponivel()
    garantir_frontend_preparado(pnpm)
    processo = iniciar_subprocesso(
        comando=[pnpm, "dev"],
        diretorio=DIRETORIO_RAIZ,
        prefixo="web",
    )
    return aguardar_processos([processo])


def executar_dev(host_api: str, porta_api: int) -> int:
    """Inicia frontend e API em paralelo para desenvolvimento local."""
    garantir_uvicorn_disponivel()
    pnpm = garantir_pnpm_disponivel()
    garantir_frontend_preparado(pnpm)

    processo_api = iniciar_subprocesso(
        comando=[
            sys.executable,
            "-m",
            "uvicorn",
            "api:app",
            "--reload",
            "--host",
            host_api,
            "--port",
            str(porta_api),
        ],
        diretorio=DIRETORIO_API,
        prefixo="api",
        ambiente=montar_ambiente_api(),
    )
    processo_web = iniciar_subprocesso(
        comando=[pnpm, "dev"],
        diretorio=DIRETORIO_RAIZ,
        prefixo="web",
    )

    print("Frontend e API iniciados. Pressione Ctrl+C para encerrar os dois.")
    return aguardar_processos([processo_api, processo_web])


def executar_build() -> int:
    """Executa o build principal do projeto Node."""
    pnpm = garantir_pnpm_disponivel()
    garantir_frontend_preparado(pnpm)
    resultado = subprocess.run(
        [pnpm, "build"],
        cwd=DIRETORIO_RAIZ,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return resultado.returncode


def main() -> int:
    """Ponto de entrada do inicializador."""
    signal.signal(signal.SIGINT, signal.default_int_handler)
    parser = criar_parser()
    argumentos_entrada = sys.argv[1:]

    if not argumentos_entrada:
        argumentos_entrada = ["dev"]

    argumentos = parser.parse_args(argumentos_entrada)

    if argumentos.comando == "dev":
        return executar_dev(
            host_api=argumentos.host_api,
            porta_api=argumentos.porta_api,
        )

    if argumentos.comando == "api":
        return executar_api(host=argumentos.host, porta=argumentos.porta)

    if argumentos.comando == "web":
        return executar_web()

    if argumentos.comando == "build":
        return executar_build()

    parser.error(f"Comando não suportado: {argumentos.comando}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
