"""
Mede a reducao efetiva de consultas Oracle no Dossie com cache aquecido.

Criterio adotado:
- baseline: quantidade total de SQLs que a secao executaria sem reuso;
- otimizado: quantidade de SQLs realmente executadas no Oracle no estado atual;
- reducao: 1 - (sqls_executadas / baseline).

Uso:
    python scripts/medir_reducao_consultas_oracle.py <CNPJ> [<CNPJ> ...]
"""
from __future__ import annotations

import json
import sys
from dataclasses import asdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from interface_grafica.services.dossie_catalog import listar_secoes_dossie
from interface_grafica.services.dossie_extraction_service import executar_sync_secao_sync


@dataclass(frozen=True)
class MedicaoSecaoOracle:
    cnpj: str
    secao_id: str
    estrategia_execucao: str
    baseline_sqls: int
    sqls_executadas_oracle: int
    sqls_reutilizadas: int
    percentual_reuso_sql: float
    percentual_reducao_consultas: float
    impacto_cache_first: str | None
    erro: str | None = None


@dataclass(frozen=True)
class ResumoCnpjOracle:
    cnpj: str
    secoes_medidas: int
    baseline_sqls: int
    sqls_executadas_oracle: int
    sqls_reutilizadas: int
    percentual_reducao_consultas: float


def _normalizar_cnpj(cnpj: str) -> str:
    return "".join(caractere for caractere in str(cnpj) if caractere.isdigit())


def _medir_secao(cnpj: str, secao_id: str) -> MedicaoSecaoOracle:
    try:
        resultado = executar_sync_secao_sync(cnpj, secao_id)
        baseline_sqls = int(resultado.get("total_sql_ids") or 0)
        sqls_executadas = len(resultado.get("sql_ids_executados") or [])
        sqls_reutilizadas = len(resultado.get("sql_ids_reutilizados") or [])
        percentual_reducao = 0.0
        if baseline_sqls > 0:
            percentual_reducao = round((1 - (sqls_executadas / baseline_sqls)) * 100, 2)

        return MedicaoSecaoOracle(
            cnpj=cnpj,
            secao_id=secao_id,
            estrategia_execucao=str(resultado.get("estrategia_execucao") or ""),
            baseline_sqls=baseline_sqls,
            sqls_executadas_oracle=sqls_executadas,
            sqls_reutilizadas=sqls_reutilizadas,
            percentual_reuso_sql=float(resultado.get("percentual_reuso_sql") or 0.0),
            percentual_reducao_consultas=percentual_reducao,
            impacto_cache_first=resultado.get("impacto_cache_first"),
        )
    except Exception as exc:
        return MedicaoSecaoOracle(
            cnpj=cnpj,
            secao_id=secao_id,
            estrategia_execucao="erro",
            baseline_sqls=0,
            sqls_executadas_oracle=0,
            sqls_reutilizadas=0,
            percentual_reuso_sql=0.0,
            percentual_reducao_consultas=0.0,
            impacto_cache_first=None,
            erro=str(exc),
        )


def medir_cnpj(cnpj: str) -> tuple[ResumoCnpjOracle, list[MedicaoSecaoOracle]]:
    cnpj_limpo = _normalizar_cnpj(cnpj)
    medicoes: list[MedicaoSecaoOracle] = []

    for secao in listar_secoes_dossie():
        if secao.tipo_fonte == "cache_catalog":
            continue
        medicao = _medir_secao(cnpj_limpo, secao.id)
        if medicao.baseline_sqls <= 0:
            continue
        medicoes.append(medicao)

    medicoes_validas = [item for item in medicoes if item.baseline_sqls > 0]
    baseline_total = sum(item.baseline_sqls for item in medicoes_validas)
    executadas_total = sum(item.sqls_executadas_oracle for item in medicoes_validas)
    reutilizadas_total = sum(item.sqls_reutilizadas for item in medicoes_validas)
    percentual_reducao_total = 0.0
    if baseline_total > 0:
        percentual_reducao_total = round((1 - (executadas_total / baseline_total)) * 100, 2)

    resumo = ResumoCnpjOracle(
        cnpj=cnpj_limpo,
        secoes_medidas=len(medicoes_validas),
        baseline_sqls=baseline_total,
        sqls_executadas_oracle=executadas_total,
        sqls_reutilizadas=reutilizadas_total,
        percentual_reducao_consultas=percentual_reducao_total,
    )
    return resumo, medicoes


def _gerar_markdown(
    resumos: list[ResumoCnpjOracle],
    medicoes_por_cnpj: dict[str, list[MedicaoSecaoOracle]],
) -> str:
    linhas = [
        "# Medicao de Reducao de Consultas Oracle",
        "",
        f"- Gerado em: `{datetime.now().isoformat()}`",
        "- Metodo: comparacao entre baseline sem reuso (`total_sql_ids`) e execucao real no estado atual (`sql_ids_executados`).",
        "- Escopo: secoes do Dossie com SQL mapeada; secoes `cache_catalog` ficam fora do baseline por nao demandarem Oracle nesse fluxo.",
        "",
    ]

    for resumo in resumos:
        linhas.extend(
            [
                f"## CNPJ {resumo.cnpj}",
                "",
                f"- Secoes medidas: `{resumo.secoes_medidas}`",
                f"- Baseline de SQLs Oracle: `{resumo.baseline_sqls}`",
                f"- SQLs realmente executadas: `{resumo.sqls_executadas_oracle}`",
                f"- SQLs reutilizadas: `{resumo.sqls_reutilizadas}`",
                f"- Reducao efetiva de consultas Oracle: `{resumo.percentual_reducao_consultas}%`",
                "",
                "| Secao | Baseline SQLs | Executadas | Reutilizadas | Reducao | Estrategia |",
                "|---|---:|---:|---:|---:|---|",
            ]
        )
        for medicao in medicoes_por_cnpj.get(resumo.cnpj, []):
            if medicao.erro:
                continue
            linhas.append(
                f"| `{medicao.secao_id}` | {medicao.baseline_sqls} | {medicao.sqls_executadas_oracle} | "
                f"{medicao.sqls_reutilizadas} | {medicao.percentual_reducao_consultas}% | `{medicao.estrategia_execucao}` |"
            )
        secoes_com_erro = [medicao for medicao in medicoes_por_cnpj.get(resumo.cnpj, []) if medicao.erro]
        if secoes_com_erro:
            linhas.extend(["", "Secoes nao medidas:"])
            for medicao in secoes_com_erro:
                linhas.append(f"- `{medicao.secao_id}`: `{medicao.erro}`")
        linhas.append("")

    return "\n".join(linhas) + "\n"


def main() -> None:
    argumentos = sys.argv[1:]
    if not argumentos:
        print("Uso: python scripts/medir_reducao_consultas_oracle.py <CNPJ> [<CNPJ> ...]")
        raise SystemExit(1)

    resumos: list[ResumoCnpjOracle] = []
    medicoes_por_cnpj: dict[str, list[MedicaoSecaoOracle]] = {}

    for argumento in argumentos:
        resumo, medicoes = medir_cnpj(argumento)
        resumos.append(resumo)
        medicoes_por_cnpj[resumo.cnpj] = medicoes

    pasta_saida = ROOT / "output" / "medicao_oracle"
    pasta_saida.mkdir(parents=True, exist_ok=True)
    caminho_json = pasta_saida / "reducao_consultas_oracle.json"
    caminho_md = pasta_saida / "reducao_consultas_oracle.md"

    payload = {
        "gerado_em": datetime.now().isoformat(),
        "resumos": [asdict(item) for item in resumos],
        "medicoes_por_cnpj": {
            cnpj: [asdict(item) for item in medicoes]
            for cnpj, medicoes in medicoes_por_cnpj.items()
        },
    }
    caminho_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    caminho_md.write_text(_gerar_markdown(resumos, medicoes_por_cnpj), encoding="utf-8")

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    print(f"\nMarkdown salvo em: {caminho_md}")


if __name__ == "__main__":
    main()
