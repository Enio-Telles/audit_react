"""Prepara o ambiente Python do backend para testes e execucao local."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


RAIZ_BACKEND = Path(__file__).resolve().parents[1]
ARQUIVO_REQUIREMENTS = RAIZ_BACKEND / "requirements.txt"

PACOTES_CORE = [
    "fastapi>=0.104.0",
    "uvicorn>=0.24.0",
    "polars>=0.20.0",
    "pydantic>=2.5.0",
    "openpyxl>=3.1.0",
    "oracledb>=2.0.0",
    "python-dotenv>=1.0.1",
    "python-multipart>=0.0.9",
    "pypdf>=5.1.0",
    "python-docx>=1.1.2",
    "pytest>=8.3.0",
]


def instalar_pacotes(pacotes: list[str]) -> None:
    """Instala um conjunto de pacotes usando o mesmo interpretador em uso."""
    comando = [sys.executable, "-m", "pip", "install", *pacotes]
    subprocess.run(comando, cwd=RAIZ_BACKEND, check=True)


def instalar_requirements_completo() -> None:
    """Instala todas as dependencias declaradas em requirements.txt."""
    comando = [sys.executable, "-m", "pip", "install", "-r", str(ARQUIVO_REQUIREMENTS)]
    subprocess.run(comando, cwd=RAIZ_BACKEND, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepara o ambiente Python do backend do audit_react")
    parser.add_argument(
        "--escopo",
        choices=("core", "completo"),
        default="core",
        help="`core` instala o minimo para API, testes e E2E local; `completo` instala todo o requirements.txt",
    )
    args = parser.parse_args()

    if args.escopo == "completo":
        instalar_requirements_completo()
    else:
        instalar_pacotes(PACOTES_CORE)

    print(f"Ambiente backend preparado com escopo: {args.escopo}")


if __name__ == "__main__":
    main()
