from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from interface_grafica.services.dossie_extraction_service import executar_sync_secao_sync


@dataclass(frozen=True)
class ResultadoMedicaoContato:
    cnpj: str
    estrategia_solicitada: str
    estrategia_execucao: str | None
    sql_principal: str | None
    linhas_extraidas: int
    total_sql_ids: int
    sql_ids_executados: int
    sql_ids_reutilizados: int
    percentual_reuso_sql: float
    impacto_cache_first: str | None
    tempo_materializacao_ms: int | None
    tempo_total_sync_ms: int | None
    cache_file: str
    metadata_file: str | None


def _executar_medicao(cnpj: str, usar_sql_consolidado: bool) -> ResultadoMedicaoContato:
    parametros = {"usar_sql_consolidado": True} if usar_sql_consolidado else None
    resultado = executar_sync_secao_sync(cnpj, "contato", parametros=parametros)

    return ResultadoMedicaoContato(
        cnpj=cnpj,
        estrategia_solicitada="sql_consolidado" if usar_sql_consolidado else "composicao_polars",
        estrategia_execucao=resultado.get("estrategia_execucao"),
        sql_principal=resultado.get("sql_principal"),
        linhas_extraidas=int(resultado.get("linhas_extraidas") or 0),
        total_sql_ids=int(resultado.get("total_sql_ids") or 0),
        sql_ids_executados=len(resultado.get("sql_ids_executados") or []),
        sql_ids_reutilizados=len(resultado.get("sql_ids_reutilizados") or []),
        percentual_reuso_sql=float(resultado.get("percentual_reuso_sql") or 0.0),
        impacto_cache_first=resultado.get("impacto_cache_first"),
        tempo_materializacao_ms=resultado.get("tempo_materializacao_ms"),
        tempo_total_sync_ms=resultado.get("tempo_total_sync_ms"),
        cache_file=str(resultado.get("cache_file") or ""),
        metadata_file=resultado.get("metadata_file"),
    )


def _gerar_markdown(resultados: list[ResultadoMedicaoContato], caminho_json: Path) -> str:
    linhas = [
        "# Relatorio de Performance do Dossie Contato",
        "",
        f"- Gerado em: `{datetime.now().isoformat()}`",
        f"- Base JSON: `{caminho_json}`",
        "",
    ]

    for resultado in resultados:
        linhas.extend(
            [
                f"## CNPJ {resultado.cnpj} | {resultado.estrategia_solicitada}",
                "",
                f"- Estrategia executada: `{resultado.estrategia_execucao or 'nao informada'}`",
                f"- SQL principal: `{resultado.sql_principal or 'nao informada'}`",
                f"- Linhas extraidas: `{resultado.linhas_extraidas}`",
                f"- Total de SQLs: `{resultado.total_sql_ids}`",
                f"- SQLs executadas no Oracle: `{resultado.sql_ids_executados}`",
                f"- SQLs reutilizadas: `{resultado.sql_ids_reutilizados}`",
                f"- Reuso efetivo: `{resultado.percentual_reuso_sql}%`",
                f"- Impacto cache-first: `{resultado.impacto_cache_first or 'nao informado'}`",
                f"- Tempo de materializacao: `{resultado.tempo_materializacao_ms} ms`",
                f"- Tempo total do sync: `{resultado.tempo_total_sync_ms} ms`",
                f"- Cache materializado: `{resultado.cache_file}`",
                f"- Metadata: `{resultado.metadata_file or 'nao informado'}`",
                "",
            ]
        )

    return "\n".join(linhas) + "\n"


def montar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Executa medicao operacional do sync da secao contato do Dossie."
    )
    parser.add_argument("cnpjs", nargs="+", help="Lista de CNPJs a medir.")
    parser.add_argument(
        "--saida-json",
        default=str(ROOT / "output" / "performance_dossie_contato" / "medicao_dossie_contato.json"),
        help="Arquivo JSON de saida com os resultados consolidados.",
    )
    parser.add_argument(
        "--saida-md",
        default=str(ROOT / "output" / "performance_dossie_contato" / "medicao_dossie_contato.md"),
        help="Arquivo markdown de saida com o relatorio consolidado.",
    )
    parser.add_argument(
        "--somente-polars",
        action="store_true",
        help="Executa apenas a estrategia padrao por composicao Polars.",
    )
    parser.add_argument(
        "--somente-consolidado",
        action="store_true",
        help="Executa apenas a estrategia SQL consolidada.",
    )
    return parser


def main() -> int:
    argumentos = montar_parser().parse_args()

    if argumentos.somente_polars and argumentos.somente_consolidado:
        raise ValueError("Use apenas uma entre --somente-polars e --somente-consolidado.")

    estrategias: list[bool]
    if argumentos.somente_polars:
        estrategias = [False]
    elif argumentos.somente_consolidado:
        estrategias = [True]
    else:
        estrategias = [False, True]

    resultados: list[ResultadoMedicaoContato] = []
    for cnpj in argumentos.cnpjs:
        for usar_sql_consolidado in estrategias:
            resultados.append(_executar_medicao(cnpj, usar_sql_consolidado))

    caminho_json = Path(argumentos.saida_json)
    caminho_md = Path(argumentos.saida_md)
    caminho_json.parent.mkdir(parents=True, exist_ok=True)
    caminho_md.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "gerado_em": datetime.now().isoformat(),
        "resultados": [asdict(resultado) for resultado in resultados],
    }
    caminho_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    caminho_md.write_text(_gerar_markdown(resultados, caminho_json), encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"\nRelatorio markdown salvo em: {caminho_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
