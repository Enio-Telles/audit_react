from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from interface_grafica.services.dossie_convergencia_report import gerar_painel_prioridades_contato


def montar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Gera um painel markdown ranqueando a prioridade operacional da secao contato por CNPJ."
    )
    parser.add_argument(
        "--comparacao-json",
        required=True,
        help="Arquivo JSON de comparacao gerado por verificar_convergencia_extracoes.py.",
    )
    parser.add_argument(
        "--raiz-cnpj",
        default=r"c:\Sistema_react\dados\CNPJ",
        help="Diretorio raiz dos CNPJs com os artefatos do Dossie.",
    )
    parser.add_argument(
        "--saida",
        default=r"c:\Sistema_react\output\verificacao_convergencia\painel_prioridades_contato.md",
        help="Arquivo markdown de saida do painel de prioridades.",
    )
    return parser


def main() -> int:
    parser = montar_parser()
    args = parser.parse_args()

    conteudo = gerar_painel_prioridades_contato(
        comparacao_json=Path(args.comparacao_json),
        raiz_cnpj=Path(args.raiz_cnpj),
        caminho_saida=Path(args.saida),
    )
    print(conteudo)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
