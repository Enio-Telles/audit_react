"""
Orquestrador principal do ETL.
Consolida dados brutos do Oracle em tabelas analiticas.

Usa o padrao Registry para mapear IDs de tabelas a funcoes de geracao,
com dependencias explicitas para execucao inteligente.
"""

from __future__ import annotations

import re
import sys
import time
import traceback
from pathlib import Path
from typing import Callable

from rich import print as rprint

from observabilidade import configure_structured_logging
from observabilidade.openlineage import LineageDataset, get_openlineage_emitter

ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
PIPELINE_LOGGER = configure_structured_logging("audit_react.pipeline")


class _TabelaRegistro:
    __slots__ = ("id", "funcao_path", "deps", "_func")

    def __init__(self, id: str, funcao_path: str, deps: list[str] | None = None):
        self.id = id
        self.funcao_path = funcao_path
        self.deps = deps or []
        self._func: Callable | None = None

    def resolver(self) -> Callable:
        if self._func is None:
            modulo_path, nome_func = self.funcao_path.rsplit(":", 1)
            import importlib
            mod = importlib.import_module(modulo_path)
            self._func = getattr(mod, nome_func)
        return self._func


REGISTO_TABELAS: dict[str, _TabelaRegistro] = {}


def _registar(id: str, funcao_path: str, deps: list[str] | None = None) -> None:
    REGISTO_TABELAS[id] = _TabelaRegistro(id, funcao_path, deps)


_registar("tb_documentos",       "transformacao.tabela_documentos:gerar_tabela_documentos")
_registar("base__efd__arquivos_validos", "transformacao.efd_base:gerar_base_efd_arquivos_validos")
_registar("base__efd__reg_0190_tipado", "transformacao.efd_base:gerar_base_efd_reg_0190_tipado")
_registar("base__efd__reg_0200_tipado", "transformacao.efd_base:gerar_base_efd_reg_0200_tipado")
_registar("base__efd__reg_0220_tipado", "transformacao.efd_base:gerar_base_efd_reg_0220_tipado")
_registar("base__efd__reg_c100_tipado", "transformacao.efd_base:gerar_base_efd_reg_c100_tipado", deps=["base__efd__arquivos_validos"])
_registar("base__efd__reg_c170_tipado", "transformacao.efd_base:gerar_base_efd_reg_c170_tipado", deps=["base__efd__reg_c100_tipado"])
_registar("base__efd__reg_c190_tipado", "transformacao.efd_base:gerar_base_efd_reg_c190_tipado", deps=["base__efd__reg_c100_tipado"])
_registar("base__efd__reg_c176_tipado", "transformacao.efd_base:gerar_base_efd_reg_c176_tipado", deps=["base__efd__reg_c170_tipado"])
_registar("base__efd__bloco_h_tipado", "transformacao.efd_base:gerar_base_efd_bloco_h_tipado")
_registar("item_unidades",       "transformacao.item_unidades:gerar_item_unidades",       deps=["tb_documentos"])
_registar("itens",               "transformacao.itens:gerar_itens",                       deps=["item_unidades"])
_registar("descricao_produtos",  "transformacao.descricao_produtos:gerar_descricao_produtos", deps=["itens"])
_registar("produtos_final",      "transformacao.produtos_final_v2:gerar_produtos_final",  deps=["descricao_produtos"])
_registar("fontes_produtos",     "transformacao.fontes_produtos:gerar_fontes_produtos",   deps=["produtos_final"])
_registar("fatores_conversao",   "transformacao.fatores_conversao:calcular_fatores_conversao", deps=["fontes_produtos"])
_registar("c170_xml",            "transformacao.c170_xml:gerar_c170_xml",                 deps=["fatores_conversao"])
_registar("c176_xml",            "transformacao.c176_xml:gerar_c176_xml",                 deps=["fatores_conversao"])
_registar("movimentacao_estoque","transformacao.movimentacao_estoque:gerar_movimentacao_estoque", deps=["c170_xml", "c176_xml"])
_registar("calculos_mensais",    "transformacao.calculos_mensais:gerar_calculos_mensais", deps=["movimentacao_estoque"])
_registar("calculos_anuais",     "transformacao.calculos_anuais:gerar_calculos_anuais",   deps=["movimentacao_estoque"])


def _ordem_topologica(selecionadas: list[str]) -> list[str]:
    visitados: set[str] = set()
    ordem: list[str] = []

    def _visitar(tab_id: str) -> None:
        if tab_id in visitados:
            return
        visitados.add(tab_id)
        reg = REGISTO_TABELAS.get(tab_id)
        if reg is None:
            return
        for dep in reg.deps:
            if dep in selecionadas or dep in REGISTO_TABELAS:
                _visitar(dep)
        ordem.append(tab_id)

    for tab_id in selecionadas:
        _visitar(tab_id)

    return ordem


def _table_dataset(cnpj: str, table_name: str) -> LineageDataset:
    return LineageDataset(namespace="file://dados", name=f"{cnpj}/{table_name}")


def executar_pipeline_completo(
    cnpj: str,
    consultas_selecionadas: list[Path] | None = None,
    tabelas_selecionadas: list[str] | None = None,
    data_limite: str | None = None,
) -> bool:
    cnpj = re.sub(r"\D", "", cnpj)
    if len(cnpj) != 14:
        rprint(f"[red]Erro:[/red] CNPJ invalido: {cnpj}")
        return False

    lineage = get_openlineage_emitter("audit_react.pipeline")
    lineage.start_run(cnpj=cnpj)

    rprint(f"\n[bold green]Iniciando pipeline para CNPJ: {cnpj}[/bold green]")
    PIPELINE_LOGGER.info("pipeline iniciado", extra={"step": "pipeline", "cnpj": cnpj})
    sucesso_global = True
    outputs_emitidos: list[LineageDataset] = []

    if consultas_selecionadas:
        rprint(f"[bold blue]Fase 1: extraindo {len(consultas_selecionadas)} tabelas brutas...[/bold blue]")
        started_at = time.perf_counter()
        try:
            from extracao.extrair_dados_cnpj import extrair_dados
            extrair_dados(
                cnpj_input=cnpj,
                data_limite_input=data_limite,
                consultas_selecionadas=consultas_selecionadas,
            )
            duration = time.perf_counter() - started_at
            lineage.emit_step_complete("extracao", duration_seconds=duration)
            PIPELINE_LOGGER.info(
                "extracao concluida",
                extra={"step": "extracao", "duration_seconds": round(duration, 6)},
            )
            rprint("[green]Extracao concluida.[/green]")
        except Exception as e:
            duration = time.perf_counter() - started_at
            lineage.emit_step_fail("extracao", error_message=str(e))
            PIPELINE_LOGGER.exception(
                "falha critica na extracao",
                extra={"step": "extracao", "duration_seconds": round(duration, 6), "error_type": e.__class__.__name__},
            )
            rprint(f"[red]Falha critica na extracao para {cnpj}:[/red] {e}")
            lineage.complete_run(success=False)
            return False

    if tabelas_selecionadas:
        ordem = _ordem_topologica(tabelas_selecionadas)
        rprint(f"[bold blue]Fase 2: gerando {len(ordem)} tabelas de negocio...[/bold blue]")

        etapas_executadas: set[str] = set()

        for tab_id in ordem:
            reg = REGISTO_TABELAS.get(tab_id)
            if reg is None:
                rprint(f"[yellow]Tabela desconhecida: {tab_id}[/yellow]")
                continue

            deps_falhadas = [d for d in reg.deps if d not in etapas_executadas and d in tabelas_selecionadas]
            if deps_falhadas:
                rprint(f"[red]Pulando {tab_id}: dependencias falharam ({', '.join(deps_falhadas)})[/red]")
                sucesso_global = False
                lineage.emit_step_fail(tab_id, error_message=f"dependencias falharam: {', '.join(deps_falhadas)}")
                continue

            rprint(f"[yellow]Processando etapa:[/yellow] [bold]{tab_id}[/bold]...")
            started_at = time.perf_counter()

            try:
                funcao = reg.resolver()
                ok = funcao(cnpj)
                duration = time.perf_counter() - started_at
                if not ok:
                    lineage.emit_step_fail(tab_id, error_message="etapa retornou False")
                    PIPELINE_LOGGER.warning(
                        "etapa retornou falha",
                        extra={"step": tab_id, "duration_seconds": round(duration, 6)},
                    )
                    rprint(f"[red]Etapa {tab_id} retornou falha (False).[/red]")
                    sucesso_global = False
                else:
                    etapas_executadas.add(tab_id)
                    dataset = _table_dataset(cnpj, tab_id)
                    outputs_emitidos.append(dataset)
                    lineage.emit_step_complete(tab_id, outputs=[dataset], duration_seconds=duration)
                    PIPELINE_LOGGER.info(
                        "etapa concluida",
                        extra={"step": tab_id, "duration_seconds": round(duration, 6)},
                    )
                    rprint(f"[green]{tab_id} finalizada.[/green]")
            except Exception as e:
                duration = time.perf_counter() - started_at
                lineage.emit_step_fail(tab_id, error_message=str(e))
                PIPELINE_LOGGER.exception(
                    "erro inesperado em etapa",
                    extra={"step": tab_id, "duration_seconds": round(duration, 6), "error_type": e.__class__.__name__},
                )
                rprint(f"[red]Erro inesperado na etapa {tab_id}:[/red] {e}")
                rprint(f"[dim]{traceback.format_exc()}[/dim]")
                sucesso_global = False

    lineage.complete_run(outputs=outputs_emitidos, success=sucesso_global)

    if sucesso_global:
        rprint(f"\n[bold green]Pipeline finalizado com sucesso para {cnpj}![/bold green]\n")
    else:
        rprint(f"\n[bold yellow]Pipeline finalizado com avisos/falhas parciais para {cnpj}.[/bold yellow]\n")

    return sucesso_global


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cnpj_alvo = sys.argv[1]
        executar_pipeline_completo(cnpj_alvo, tabelas_selecionadas=["tb_documentos", "item_unidades", "itens"])
    else:
        rprint("[yellow]Uso: python orquestrador_pipeline.py <CNPJ>[/yellow]")
