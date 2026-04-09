"""
Verificacao de convergencia entre extracoes canonicas e legadas.

Uso:
    python scripts/verificar_convergencia_otimizacao.py <CNPJ>

Saida:
    Relatorio com status por dataset.
"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _PROJECT_ROOT / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

import polars as pl
from rich import print as rprint
from rich.table import Table

from utilitarios.dataset_registry import CATALOGO
from utilitarios.dataset_registry import listar_caminhos_com_fallback
from utilitarios.dataset_registry import obter_definicao


def _normalizar_para_comparacao(dataframe: pl.DataFrame) -> pl.DataFrame:
    """Normaliza colunas e ordenacao para comparacao deterministica."""

    colunas_ordenadas = sorted(dataframe.columns)
    return dataframe.select(colunas_ordenadas).sort(colunas_ordenadas)


def _hash_dataframe(dataframe: pl.DataFrame) -> str:
    """Gera hash estavel do conteudo."""

    dataframe_normalizado = _normalizar_para_comparacao(dataframe)
    dataframe_texto = dataframe_normalizado.select(
        [
            pl.col(coluna).cast(pl.Utf8, strict=False).fill_null("__NULL__").alias(coluna)
            for coluna in dataframe_normalizado.columns
        ]
    )
    return str(dataframe_texto.hash_rows().sum())


def comparar_dataset(cnpj: str, dataset_id: str) -> dict:
    """Compara o dataset canonico do registry com o primeiro legado disponivel."""

    definicao = obter_definicao(dataset_id)
    if definicao is None or definicao.tipo == "dimensao_global":
        return {"dataset_id": dataset_id, "status": "IGNORADO", "motivo": "Dimensao global ou desconhecido"}

    caminhos = listar_caminhos_com_fallback(cnpj, dataset_id)
    if not caminhos:
        return {"dataset_id": dataset_id, "status": "IGNORADO", "motivo": "Sem caminhos definidos"}

    caminho_canonico = caminhos[0]
    caminho_legado = next((caminho for caminho in caminhos[1:] if caminho.exists()), None)

    if not caminho_canonico.exists() and caminho_legado is None:
        return {
            "dataset_id": dataset_id,
            "status": "AUSENTE",
            "motivo": "Nenhum Parquet encontrado (nem canonico nem legado)",
        }

    if not caminho_canonico.exists():
        return {
            "dataset_id": dataset_id,
            "status": "PENDENTE",
            "motivo": f"Apenas legado existe: {caminho_legado.name}",
            "caminho_legado": str(caminho_legado),
        }

    if caminho_legado is None:
        return {
            "dataset_id": dataset_id,
            "status": "APENAS_CANONICO",
            "motivo": f"Apenas canonico existe: {caminho_canonico.name}",
            "caminho_canonico": str(caminho_canonico),
        }

    try:
        dataframe_canonico = pl.read_parquet(caminho_canonico)
        dataframe_legado = pl.read_parquet(caminho_legado)
    except Exception as exc:
        return {"dataset_id": dataset_id, "status": "ERRO", "motivo": f"Erro ao ler Parquets: {exc}"}

    resultado = {
        "dataset_id": dataset_id,
        "caminho_canonico": str(caminho_canonico),
        "caminho_legado": str(caminho_legado),
        "linhas_canonico": dataframe_canonico.height,
        "linhas_legado": dataframe_legado.height,
        "colunas_canonico": len(dataframe_canonico.columns),
        "colunas_legado": len(dataframe_legado.columns),
    }

    colunas_canonico = set(dataframe_canonico.columns)
    colunas_legado = set(dataframe_legado.columns)
    colunas_extras_canonico = colunas_canonico - colunas_legado
    colunas_extras_legado = colunas_legado - colunas_canonico
    colunas_comuns = colunas_canonico & colunas_legado

    if colunas_extras_canonico:
        resultado["colunas_extras_canonico"] = sorted(colunas_extras_canonico)
    if colunas_extras_legado:
        resultado["colunas_extras_legado"] = sorted(colunas_extras_legado)

    if not colunas_comuns:
        resultado["status"] = "DIVERGE"
        resultado["motivo"] = "Nenhuma coluna em comum"
        return resultado

    dataframe_canonico_comum = dataframe_canonico.select(sorted(colunas_comuns))
    dataframe_legado_comum = dataframe_legado.select(sorted(colunas_comuns))

    hash_canonico = _hash_dataframe(dataframe_canonico_comum)
    hash_legado = _hash_dataframe(dataframe_legado_comum)

    if hash_canonico == hash_legado and dataframe_canonico.height == dataframe_legado.height:
        resultado["status"] = "CONVERGE"
        resultado["motivo"] = "Conteudo identico"
        return resultado

    divergencias: list[str] = []
    if dataframe_canonico.height != dataframe_legado.height:
        divergencias.append(
            f"Linhas: canonico={dataframe_canonico.height} vs legado={dataframe_legado.height}"
        )
    if hash_canonico != hash_legado:
        divergencias.append("Hash de conteudo diferente nas colunas comuns")
    if colunas_extras_canonico or colunas_extras_legado:
        divergencias.append(
            f"Colunas diferentes: +canon={sorted(colunas_extras_canonico)}, +legado={sorted(colunas_extras_legado)}"
        )

    resultado["status"] = "DIVERGE"
    resultado["motivo"] = "; ".join(divergencias)
    return resultado


def verificar_convergencia_completa(cnpj: str) -> list[dict]:
    """Verifica convergencia de todos os datasets por CNPJ."""

    return [
        comparar_dataset(cnpj, definicao.dataset_id)
        for definicao in CATALOGO
        if definicao.tipo != "dimensao_global"
    ]


def imprimir_relatorio(cnpj: str, resultados: list[dict]) -> None:
    """Imprime o relatorio em formato compativel com console Windows."""

    rprint(f"\n[bold cyan]=== Relatorio de Convergencia - CNPJ {cnpj} ===[/bold cyan]\n")

    tabela = Table(title="Convergencia por Dataset", show_lines=True)
    tabela.add_column("Dataset", style="bold")
    tabela.add_column("Status", justify="center")
    tabela.add_column("Linhas C", justify="right")
    tabela.add_column("Linhas L", justify="right")
    tabela.add_column("Motivo")

    contadores = {
        "CONVERGE": 0,
        "DIVERGE": 0,
        "PENDENTE": 0,
        "AUSENTE": 0,
        "APENAS_CANONICO": 0,
        "ERRO": 0,
        "IGNORADO": 0,
    }

    for resultado in resultados:
        status = resultado.get("status", "?")
        contadores[status] = contadores.get(status, 0) + 1
        cor = {
            "CONVERGE": "green",
            "DIVERGE": "red",
            "PENDENTE": "yellow",
            "AUSENTE": "dim",
            "APENAS_CANONICO": "cyan",
            "ERRO": "red bold",
            "IGNORADO": "dim",
        }.get(status, "white")

        tabela.add_row(
            resultado["dataset_id"],
            f"[{cor}]{status}[/{cor}]",
            str(resultado.get("linhas_canonico", "-")),
            str(resultado.get("linhas_legado", "-")),
            resultado.get("motivo", ""),
        )

    rprint(tabela)
    rprint("\n[bold]Resumo:[/bold]")
    for status, quantidade in sorted(contadores.items()):
        if quantidade > 0:
            rprint(f"  {status}: {quantidade}")

    total_verificados = contadores["CONVERGE"] + contadores["DIVERGE"]
    if total_verificados > 0:
        taxa = contadores["CONVERGE"] / total_verificados * 100
        cor = "green" if taxa == 100 else "yellow" if taxa >= 80 else "red"
        rprint(f"\n[{cor}]Taxa de convergencia: {taxa:.0f}%[/{cor}]")
    else:
        rprint("\n[yellow]Nenhum par canonico/legado para comparar.[/yellow]")


def main() -> None:
    if len(sys.argv) < 2:
        rprint("[red]Uso: python scripts/verificar_convergencia_otimizacao.py <CNPJ>[/red]")
        raise SystemExit(1)

    cnpj = "".join(caractere for caractere in sys.argv[1] if caractere.isdigit())
    rprint(f"[cyan]Verificando convergencia para CNPJ: {cnpj}[/cyan]")

    resultados = verificar_convergencia_completa(cnpj)
    imprimir_relatorio(cnpj, resultados)

    divergencias = sum(1 for resultado in resultados if resultado.get("status") == "DIVERGE")
    raise SystemExit(1 if divergencias > 0 else 0)


if __name__ == "__main__":
    main()
