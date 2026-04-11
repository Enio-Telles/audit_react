import polars as pl
from pathlib import Path
from utilitarios.project_paths import PROJECT_ROOT
import sys
import re
from rich import print as rprint

ROOT_DIR = PROJECT_ROOT
SRC_DIR = ROOT_DIR / "src"
DADOS_DIR = ROOT_DIR / "dados"
CNPJ_ROOT = DADOS_DIR / "CNPJ"


try:
    from utilitarios.dataset_registry import criar_metadata, registrar_dataset
except ImportError as e:
    rprint(f"[red]Erro ao importar módulos utilitários em tabela_documentos:[/red] {e}")
    sys.exit(1)


def gerar_tabela_documentos(cnpj: str, pasta_cnpj: Path | None = None) -> bool:
    """
    Consolida cabeçalhos de notas (C100, NFe, NFCe).
    Saída canônica: tb_documentos
    """
    cnpj = re.sub(r"\D", "", cnpj)
    if pasta_cnpj is None:
        pasta_cnpj = CNPJ_ROOT / cnpj

    arq_dir = pasta_cnpj / "arquivos_parquet"
    if not arq_dir.exists():
        rprint(f"[red]Pasta de parquets brutos não encontrada: {arq_dir}[/red]")
        return False

    rprint(f"[bold cyan]Consolidando documentos para CNPJ: {cnpj}[/bold cyan]")

    try:
        def _ler(prefix: str) -> pl.DataFrame:
            paths = list(arq_dir.glob(f"{prefix}*.parquet"))
            if not paths:
                return pl.DataFrame()
            return pl.read_parquet(paths[0])

        df_c100 = _ler("c100")
        df_nfe = _ler("NFe")
        df_nfce = _ler("NFCe")

        fragmentos = []
        origens: list[str] = []
        if not df_c100.is_empty():
            fragmentos.append(df_c100.with_columns(pl.lit("C100").alias("origem")))
            origens.append("C100")
        if not df_nfe.is_empty():
            fragmentos.append(df_nfe.with_columns(pl.lit("NFe").alias("origem")))
            origens.append("NFe")
        if not df_nfce.is_empty():
            fragmentos.append(df_nfce.with_columns(pl.lit("NFCe").alias("origem")))
            origens.append("NFCe")

        if not fragmentos:
            rprint("[yellow]Aviso: Nenhum documento bruto encontrado para consolidar.[/yellow]")
            return False

        df_final = pl.concat(fragmentos, how="diagonal_relaxed")

        cols_categoricas = ["origem", "situacao", "modelo", "serie", "ind_oper", "ind_emit", "cod_sit"]
        for col in cols_categoricas:
            if col in df_final.columns:
                df_final = df_final.with_columns(pl.col(col).cast(pl.Categorical))

        metadata = criar_metadata(
            cnpj=cnpj,
            dataset_id="tb_documentos",
            linhas=df_final.height,
            parametros={"origens": origens},
        )
        destino = registrar_dataset(cnpj, "tb_documentos", df_final, metadata=metadata)
        if destino is None:
            return False

        rprint(f"[green]tb_documentos materializado em {destino}[/green]")
        return True

    except Exception as e:
        rprint(f"[red]Erro ao consolidar documentos para {cnpj}:[/red] {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) > 1:
        gerar_tabela_documentos(sys.argv[1])
    else:
        c = input("CNPJ: ")
        gerar_tabela_documentos(c)
