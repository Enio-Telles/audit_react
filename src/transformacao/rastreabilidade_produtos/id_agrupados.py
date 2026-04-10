from __future__ import annotations

import re
import sys
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT

import polars as pl
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.salvar_para_parquet import salvar_para_parquet
except ImportError as e:
    rprint(f"[red]Erro ao importar modulos:[/red] {e}")
    sys.exit(1)


def _extrair_valores(valores, saida: set[str]) -> None:
    if not valores:
        return
    stack = [valores]
    while stack:
        curr = stack.pop()
        if curr is None:
            continue
        if isinstance(curr, list):
            stack.extend(curr)
        else:
            texto = str(curr).strip()
            if texto:
                saida.add(texto)


def _consolidar_grupo_id_agrupado(df_grupo: pl.DataFrame) -> pl.DataFrame:
    registros = df_grupo.to_dicts()
    descr_padrao = None
    descricoes: set[str] = set()
    descricoes_complementares: set[str] = set()
    codigos: set[str] = set()
    unidades: set[str] = set()

    for row in registros:
        if descr_padrao is None:
            valor = (row.get("descr_padrao") or "").strip() if row.get("descr_padrao") is not None else ""
            if valor:
                descr_padrao = valor

        _extrair_valores(
            [
                row.get("descr_padrao"),
                row.get("descricao_final"),
                row.get("descricao"),
            ],
            descricoes
        )
        _extrair_valores(row.get("lista_desc_compl"), descricoes_complementares)
        _extrair_valores(row.get("lista_codigos"), codigos)
        _extrair_valores(
            [
                row.get("lista_unid"),
                row.get("lista_unidades_agr"),
                row.get("unid_ref_sugerida"),
            ],
            unidades
        )

    return pl.DataFrame(
        {
            "id_agrupado": [str(registros[0]["id_agrupado"])],
            "descr_padrao": [descr_padrao],
            "lista_descricoes": [sorted(descricoes)],
            "lista_desc_compl": [sorted(descricoes_complementares)],
            "lista_codigos": [sorted(codigos)],
            "lista_unidades": [sorted(unidades)],
        },
        schema_overrides={
            "id_agrupado": pl.String,
            "descr_padrao": pl.String,
            "lista_descricoes": pl.List(pl.String),
            "lista_desc_compl": pl.List(pl.String),
            "lista_codigos": pl.List(pl.String),
            "lista_unidades": pl.List(pl.String),
        },
    )


def gerar_id_agrupados(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    cnpj = re.sub(r"\D", "", cnpj or "")
    if len(cnpj) not in {11, 14}:
        raise ValueError("CPF/CNPJ invalido.")

    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    pasta_analises = pasta_cnpj / "analises" / "produtos"
    arq_final = pasta_analises / f"produtos_final_{cnpj}.parquet"
    if not arq_final.exists():
        rprint(f"[red]Arquivo necessario nao encontrado:[/red] {arq_final}")
        return False

    try:
        schema = pl.scan_parquet(arq_final).collect_schema()
        if "id_agrupado" not in schema.names():
            rprint("[yellow]produtos_final sem id_agrupado.[/yellow]")
            return False
            
        lf_final = pl.scan_parquet(arq_final)
        if lf_final.select(pl.len()).collect().item() == 0:
            rprint("[yellow]produtos_final vazio.[/yellow]")
            return False
    except Exception as e:
        rprint(f"[red]Erro ao ler esquema do parquet:[/red] {e}")
        return False

    df_id_agrupados = (
        lf_final
        .with_columns(
            [
                pl.col("id_agrupado").cast(pl.Utf8, strict=False),
                pl.col("descr_padrao").cast(pl.Utf8, strict=False),
                pl.col("descricao_final").cast(pl.Utf8, strict=False),
                pl.col("descricao").cast(pl.Utf8, strict=False),
                pl.col("lista_desc_compl").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_codigos").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_unid").cast(pl.List(pl.Utf8), strict=False),
                pl.col("lista_unidades_agr").cast(pl.List(pl.Utf8), strict=False),
                pl.col("unid_ref_sugerida").cast(pl.Utf8, strict=False),
            ]
        )
        .collect() # Executa o DAG Lazy montado até aqui e aloca na memória otimizada
        .group_by("id_agrupado")
        .map_groups(_consolidar_grupo_id_agrupado)
        .sort("id_agrupado")
    )

    return salvar_para_parquet(df_id_agrupados, pasta_analises, f"id_agrupados_{cnpj}.parquet")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_id_agrupados(sys.argv[1])
    else:
        gerar_id_agrupados(input("CNPJ: "))


