from __future__ import annotations

import argparse
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import polars as pl


@dataclass
class InventarioParquet:
    caminho_relativo: str
    nome_logico: str
    tamanho_bytes: int
    linhas: int
    colunas: int
    assinatura_schema: str
    schema: list[tuple[str, str]]


def normalizar_nome_logico(caminho_arquivo: Path, cnpj: str) -> str:
    nome_sem_cnpj = re.sub(rf"_{re.escape(cnpj)}$", "", caminho_arquivo.stem, flags=re.IGNORECASE)
    return nome_sem_cnpj.lower()


def ler_schema(caminho_arquivo: Path) -> list[tuple[str, str]]:
    schema = pl.read_parquet_schema(caminho_arquivo)
    return [(nome_coluna, str(tipo_dado)) for nome_coluna, tipo_dado in schema.items()]


def contar_linhas(caminho_arquivo: Path) -> int:
    return int(pl.scan_parquet(caminho_arquivo).select(pl.len()).collect().item())


def gerar_assinatura_schema(schema: list[tuple[str, str]]) -> str:
    conteudo = json.dumps(schema, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha1(conteudo.encode("utf-8")).hexdigest()


def inventariar_cnpj(pasta_cnpj: Path, cnpj: str) -> dict[str, object]:
    arquivos = sorted(pasta_cnpj.rglob("*.parquet"))
    inventario: list[InventarioParquet] = []

    for caminho_arquivo in arquivos:
        schema = ler_schema(caminho_arquivo)
        item = InventarioParquet(
            caminho_relativo=caminho_arquivo.relative_to(pasta_cnpj).as_posix(),
            nome_logico=normalizar_nome_logico(caminho_arquivo, cnpj),
            tamanho_bytes=caminho_arquivo.stat().st_size,
            linhas=contar_linhas(caminho_arquivo),
            colunas=len(schema),
            assinatura_schema=gerar_assinatura_schema(schema),
            schema=schema,
        )
        inventario.append(item)

    contagem_por_topo: dict[str, int] = {}
    contagem_nomes_logicos: dict[str, int] = {}

    for item in inventario:
        topo = item.caminho_relativo.split("/", 1)[0]
        contagem_por_topo[topo] = contagem_por_topo.get(topo, 0) + 1
        contagem_nomes_logicos[item.nome_logico] = contagem_nomes_logicos.get(item.nome_logico, 0) + 1

    duplicacoes_logicas = {
        nome_logico: quantidade
        for nome_logico, quantidade in sorted(contagem_nomes_logicos.items())
        if quantidade > 1
    }

    return {
        "cnpj": cnpj,
        "total_parquets": len(inventario),
        "contagem_por_topo": contagem_por_topo,
        "duplicacoes_logicas": duplicacoes_logicas,
        "arquivos": [asdict(item) for item in inventario],
    }


def carregar_baseline(caminho_baseline: Path) -> dict[str, object]:
    return json.loads(caminho_baseline.read_text(encoding="utf-8"))


def indexar_arquivos_por_caminho(arquivos: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    return {str(item["caminho_relativo"]): item for item in arquivos}


def comparar_baselines(
    baseline_antes: dict[str, object], baseline_depois: dict[str, object]
) -> dict[str, object]:
    comparacao: dict[str, object] = {"cnpjs": {}}

    for item_antes in baseline_antes["cnpjs"]:
        cnpj = item_antes["cnpj"]
        item_depois = next((item for item in baseline_depois["cnpjs"] if item["cnpj"] == cnpj), None)
        if item_depois is None:
            comparacao["cnpjs"][cnpj] = {"erro": "CNPJ ausente no baseline depois"}
            continue

        arquivos_antes = indexar_arquivos_por_caminho(item_antes["arquivos"])
        arquivos_depois = indexar_arquivos_por_caminho(item_depois["arquivos"])

        caminhos_antes = set(arquivos_antes)
        caminhos_depois = set(arquivos_depois)

        somente_antes = sorted(caminhos_antes - caminhos_depois)
        somente_depois = sorted(caminhos_depois - caminhos_antes)

        alterados: list[dict[str, object]] = []
        for caminho_relativo in sorted(caminhos_antes & caminhos_depois):
            antes = arquivos_antes[caminho_relativo]
            depois = arquivos_depois[caminho_relativo]
            divergencias: dict[str, object] = {}

            if antes["linhas"] != depois["linhas"]:
                divergencias["linhas"] = {"antes": antes["linhas"], "depois": depois["linhas"]}
            if antes["colunas"] != depois["colunas"]:
                divergencias["colunas"] = {"antes": antes["colunas"], "depois": depois["colunas"]}
            if antes["assinatura_schema"] != depois["assinatura_schema"]:
                divergencias["schema"] = {
                    "antes": antes["assinatura_schema"],
                    "depois": depois["assinatura_schema"],
                }

            if divergencias:
                alterados.append({"caminho_relativo": caminho_relativo, "divergencias": divergencias})

        comparacao["cnpjs"][cnpj] = {
            "somente_antes": somente_antes,
            "somente_depois": somente_depois,
            "alterados": alterados,
            "convergencia_total": not somente_antes and not somente_depois and not alterados,
        }

    return comparacao


def montar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inventaria e compara extracoes Parquet por CNPJ para verificar convergencia antes/depois."
    )
    parser.add_argument("cnpjs", nargs="+", help="Lista de CNPJs a verificar.")
    parser.add_argument(
        "--raiz-cnpj",
        default=r"c:\Sistema_react\dados\CNPJ",
        help="Diretorio raiz onde os CNPJs estao armazenados.",
    )
    parser.add_argument(
        "--salvar-baseline",
        help="Caminho de saida para salvar o baseline atual em JSON.",
    )
    parser.add_argument(
        "--comparar-com",
        help="Caminho de um baseline JSON anterior para comparar com o estado atual.",
    )
    return parser


def main() -> int:
    parser = montar_parser()
    argumentos = parser.parse_args()

    raiz_cnpj = Path(argumentos.raiz_cnpj)
    baselines_cnpjs = []

    for cnpj in argumentos.cnpjs:
        pasta_cnpj = raiz_cnpj / cnpj
        if not pasta_cnpj.exists():
            raise FileNotFoundError(f"Pasta do CNPJ nao encontrada: {pasta_cnpj}")
        baselines_cnpjs.append(inventariar_cnpj(pasta_cnpj, cnpj))

    baseline_atual = {
        "gerado_em": datetime.now().isoformat(),
        "raiz_cnpj": str(raiz_cnpj),
        "cnpjs": baselines_cnpjs,
    }

    print(json.dumps(baseline_atual, ensure_ascii=False, indent=2))

    if argumentos.salvar_baseline:
        caminho_saida = Path(argumentos.salvar_baseline)
        caminho_saida.parent.mkdir(parents=True, exist_ok=True)
        caminho_saida.write_text(json.dumps(baseline_atual, ensure_ascii=False, indent=2), encoding="utf-8")

    if argumentos.comparar_com:
        baseline_antes = carregar_baseline(Path(argumentos.comparar_com))
        comparacao = comparar_baselines(baseline_antes, baseline_atual)
        print("\n=== COMPARACAO ===")
        print(json.dumps(comparacao, ensure_ascii=False, indent=2))

        if argumentos.salvar_baseline:
            caminho_comparacao = Path(argumentos.salvar_baseline).with_name(
                Path(argumentos.salvar_baseline).stem + "_comparacao.json"
            )
            caminho_comparacao.write_text(json.dumps(comparacao, ensure_ascii=False, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
