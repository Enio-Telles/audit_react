import { useMemo } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { dossieApi } from "../../../api/client";
import { DataTable } from "../../../components/table/DataTable";
import type { DossieSectionData } from "../types";
import { DossieContatoDetalhe } from "./DossieContatoDetalhe";
import type { DossieViewMode } from "../utils/dossie_helpers";
import {
  formatarComparacaoResumo,
  formatarEstrategiaResumo,
  obterVarianteComparacao,
} from "../utils/dossie_helpers";
import { useAppStore } from "../../../store/appStore";
import { DossieBadge } from "./DossieBadge";

interface DossieSectionDetailProps {
  dados: DossieSectionData;
  viewMode?: DossieViewMode;
}

// ⚡ Bolt Optimization: Use cached Intl.DateTimeFormat instance instead of Date.prototype.toLocaleString()
const dateTimeFormatter = new Intl.DateTimeFormat("pt-BR", {
  dateStyle: "short",
  timeStyle: "medium",
});

function formatar_data_atualizacao(updatedAt?: string | null): string | null {
  if (!updatedAt) {
    return null;
  }

  const data = new Date(updatedAt);
  if (Number.isNaN(data.getTime())) {
    return null;
  }

  return dateTimeFormatter.format(data);
}

function escolher_colunas_compactas(colunas: string[]): string[] {
  const prioridades = [
    "origem_dado",
    "tabela_origem",
    "sql_id_origem",
    "data_referencia",
    "cnpj",
    "cpf_cnpj",
    "cpf_cnpj_referencia",
    "nome",
    "nome_referencia",
    "tipo_vinculo",
    "situacao",
    "situacao_cadastral",
    "valor",
    "quantidade",
  ];

  const resultado: string[] = [];
  for (const prioridade of prioridades) {
    const colunaEncontrada = colunas.find(
      (coluna) => coluna.toLowerCase() === prioridade,
    );
    if (colunaEncontrada && !resultado.includes(colunaEncontrada)) {
      resultado.push(colunaEncontrada);
    }
  }

  for (const coluna of colunas) {
    if (resultado.length >= 8) {
      break;
    }
    if (!resultado.includes(coluna)) {
      resultado.push(coluna);
    }
  }

  return resultado;
}

function montar_cartoes_operacionais(dados: {
  estrategiaExecucao: string | null;
  sqlPrincipal: string | null;
  sqlExecutadas: number | null;
  sqlReutilizadas: number | null;
  percentualReuso: number | null;
  tempoMaterializacaoMs: number | null;
  tempoTotalSyncMs: number | null;
  impactoCacheFirst: string | null;
}) {
  const cartoes = [
    {
      rotulo: "Estrategia",
      valor: formatarEstrategiaResumo(dados.estrategiaExecucao) ?? "-",
    },
    { rotulo: "SQL principal", valor: dados.sqlPrincipal ?? "-" },
    {
      rotulo: "Reuso Oracle",
      valor:
        dados.percentualReuso !== null ? `${dados.percentualReuso}%` : "-",
    },
    {
      rotulo: "SQLs executadas",
      valor:
        dados.sqlExecutadas !== null ? String(dados.sqlExecutadas) : "-",
    },
    {
      rotulo: "SQLs reutilizadas",
      valor:
        dados.sqlReutilizadas !== null ? String(dados.sqlReutilizadas) : "-",
    },
    {
      rotulo: "Materializacao",
      valor:
        dados.tempoMaterializacaoMs !== null
          ? `${dados.tempoMaterializacaoMs} ms`
          : "-",
    },
    {
      rotulo: "Sync total",
      valor:
        dados.tempoTotalSyncMs !== null ? `${dados.tempoTotalSyncMs} ms` : "-",
    },
    { rotulo: "Cache-first", valor: dados.impactoCacheFirst ?? "-" },
  ];

  return cartoes.filter((cartao) => cartao.valor !== "-");
}

function LinhaMetadata({
  rotulo,
  valor,
}: {
  rotulo: string;
  valor?: string | number | null;
}) {
  if (valor === null || valor === undefined || valor === "") {
    return null;
  }

  return (
    <div className="flex items-start justify-between gap-3 border-b border-slate-800/80 py-2 text-[11px] last:border-b-0">
      <span className="text-slate-500">{rotulo}</span>
      <span className="max-w-[70%] text-right text-slate-300">{valor}</span>
    </div>
  );
}

export function DossieSectionDetail({
  dados,
  viewMode = "auditoria",
}: DossieSectionDetailProps) {
  const cnpj = extrair_cnpj_do_caminho(dados.cacheFile);
  const sectionKey = `${cnpj}:${dados.id}`;
  const tableProfile = useAppStore((state) => state.dossieTableProfile);
  const setTableProfile = useAppStore((state) => state.setDossieTableProfile);
  const tableState = useAppStore(
    (state) => state.dossieSectionTableStateById[sectionKey],
  );
  const setDossieSectionSort = useAppStore(
    (state) => state.setDossieSectionSort,
  );
  const setDossieSectionColumnFilter = useAppStore(
    (state) => state.setDossieSectionColumnFilter,
  );
  const clearDossieSectionColumnFilters = useAppStore(
    (state) => state.clearDossieSectionColumnFilters,
  );
  const dataAtualizacao = formatar_data_atualizacao(dados.updatedAt);
  const estrategiaExecucao =
    typeof dados.metadata?.estrategia_execucao === "string"
      ? dados.metadata.estrategia_execucao
      : null;
  const sqlPrincipal =
    typeof dados.metadata?.sql_principal === "string"
      ? dados.metadata.sql_principal
      : null;
  const sqlExecutadas = Array.isArray(dados.metadata?.sql_ids_executados)
    ? dados.metadata.sql_ids_executados.length
    : null;
  const sqlReutilizadas = Array.isArray(dados.metadata?.sql_ids_reutilizados)
    ? dados.metadata.sql_ids_reutilizados.length
    : null;
  const totalSql =
    typeof dados.metadata?.total_sql_ids === "number"
      ? dados.metadata.total_sql_ids
      : null;
  const percentualReuso =
    typeof dados.metadata?.percentual_reuso_sql === "number"
      ? dados.metadata.percentual_reuso_sql
      : null;
  const tempoMaterializacaoMs =
    typeof dados.metadata?.tempo_materializacao_ms === "number"
      ? dados.metadata.tempo_materializacao_ms
      : null;
  const tempoTotalSyncMs =
    typeof dados.metadata?.tempo_total_sync_ms === "number"
      ? dados.metadata.tempo_total_sync_ms
      : null;
  const impactoCacheFirst =
    typeof dados.metadata?.impacto_cache_first === "string"
      ? dados.metadata.impacto_cache_first
      : null;
  const comparacaoAlternativa =
    dados.metadata &&
    typeof dados.metadata.comparacao_estrategia_alternativa === "object"
      ? (dados.metadata.comparacao_estrategia_alternativa as Record<
          string,
          unknown
        >)
      : null;
  const caminhoHistoricoComparacao =
    typeof dados.metadata?.comparison_history_file === "string"
      ? dados.metadata.comparison_history_file
      : null;
  const arquivosOrigem = Array.isArray(
    dados.metadata?.arquivos_origem_considerados,
  )
    ? (dados.metadata.arquivos_origem_considerados as string[])
    : [];
  const tabelasOrigemSinteticas = Array.isArray(dados.metadata?.tabela_origem)
    ? (dados.metadata.tabela_origem as string[])
    : [];
  const convergenciaBasica =
    comparacaoAlternativa &&
    typeof comparacaoAlternativa.convergencia_basica === "boolean"
      ? comparacaoAlternativa.convergencia_basica
      : null;
  const convergenciaFuncional =
    comparacaoAlternativa &&
    typeof comparacaoAlternativa.convergencia_funcional === "boolean"
      ? comparacaoAlternativa.convergencia_funcional
      : null;
  const { data: historicoComparacoes } = useQuery({
    queryKey: ["dossie_contact_history", dados.id, dados.cacheFile],
    queryFn: () =>
      dossieApi.getHistoricoComparacoesContato(
        extrair_cnpj_do_caminho(dados.cacheFile),
        5,
      ),
    enabled: dados.id === "contato",
  });
  const { data: resumoComparacoes } = useQuery({
    queryKey: ["dossie_contact_history_summary", dados.id, dados.cacheFile],
    queryFn: () =>
      dossieApi.getResumoComparacoesContato(
        extrair_cnpj_do_caminho(dados.cacheFile),
      ),
    enabled: dados.id === "contato",
  });
  const mutacaoRelatorioComparacao = useMutation({
    mutationFn: () =>
      dossieApi.gerarRelatorioComparacoesContato(
        extrair_cnpj_do_caminho(dados.cacheFile),
      ),
  });

  const colunasCompactas = useMemo(
    () => escolher_colunas_compactas(dados.columns),
    [dados.columns],
  );
  const hiddenColumnsCompacto = useMemo(
    () =>
      new Set(
        dados.columns.filter((coluna) => !colunasCompactas.includes(coluna)),
      ),
    [colunasCompactas, dados.columns],
  );
  const statusComparacao =
    convergenciaFuncional === false
      ? "divergencia_funcional"
      : convergenciaFuncional === true
        ? "convergencia_funcional"
        : convergenciaBasica === false
          ? "divergencia_basica"
          : convergenciaBasica === true
            ? "convergencia_basica"
            : null;
  const comparacaoResumo = formatarComparacaoResumo(statusComparacao);
  const cartoesOperacionais = montar_cartoes_operacionais({
    estrategiaExecucao,
    sqlPrincipal,
    sqlExecutadas,
    sqlReutilizadas,
    percentualReuso,
    tempoMaterializacaoMs,
    tempoTotalSyncMs,
    impactoCacheFirst,
  });

  return (
    <div className="rounded-2xl border border-slate-700 bg-slate-900/60 p-4">
      <div className="mb-4 flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-wide text-slate-500">
            Leitura do cache
          </div>
          <h3 className="text-lg font-semibold text-white">{dados.title}</h3>
          <div className="mt-1 text-xs text-slate-400">
            {dados.rowCount}{" "}
            {dados.rowCount === 1
              ? "linha materializada"
              : "linhas materializadas"}
          </div>
        </div>
        <div className="text-right text-[11px] text-slate-500">
          <div>{dados.cacheFile}</div>
          {dataAtualizacao && (
            <div className="mt-1">Atualizado em {dataAtualizacao}</div>
          )}
        </div>
      </div>

      <div className="mb-4 flex flex-wrap gap-2">
        {estrategiaExecucao && (
          <DossieBadge
            rotulo="Estrategia"
            valor={formatarEstrategiaResumo(estrategiaExecucao)}
            variante="info"
          />
        )}
        {sqlPrincipal && (
          <DossieBadge rotulo="SQL principal" valor={sqlPrincipal} />
        )}
        {comparacaoResumo && (
          <DossieBadge
            rotulo="Comparacao"
            valor={comparacaoResumo.texto}
            variante={obterVarianteComparacao(statusComparacao)}
          />
        )}
        {tabelasOrigemSinteticas.length > 0 && (
          <DossieBadge
            rotulo="Origem"
            valor={`${tabelasOrigemSinteticas.length} artefatos`}
            variante="sucesso"
          />
        )}
      </div>

      {cartoesOperacionais.length > 0 && (
        <div className="mb-4 grid gap-3 md:grid-cols-2 xl:grid-cols-4">
          {cartoesOperacionais.map((cartao) => (
            <div
              key={cartao.rotulo}
              className="rounded-xl border border-slate-800 bg-slate-950/40 p-3"
            >
              <div className="text-[11px] uppercase tracking-wide text-slate-500">
                {cartao.rotulo}
              </div>
              <div className="mt-1 text-sm font-medium text-slate-100">
                {cartao.valor}
              </div>
            </div>
          ))}
        </div>
      )}

      {dados.metadata && viewMode === "auditoria" && (
        <div className="mb-4 rounded-xl border border-slate-800 bg-slate-950/40 p-4">
          <div className="mb-3 text-xs font-semibold uppercase tracking-wide text-slate-500">
            Metadata tecnica
          </div>
          <div className="grid gap-4 lg:grid-cols-2">
            <div>
              <LinhaMetadata rotulo="Total de SQLs da secao" valor={totalSql} />
              <LinhaMetadata
                rotulo="Historico de comparacao"
                valor={caminhoHistoricoComparacao}
              />
              <LinhaMetadata
                rotulo="Arquivos de origem considerados"
                valor={
                  arquivosOrigem.length > 0 ? String(arquivosOrigem.length) : null
                }
              />
              <LinhaMetadata
                rotulo="Tabelas/artefatos de origem"
                valor={
                  tabelasOrigemSinteticas.length > 0
                    ? tabelasOrigemSinteticas.join(" | ")
                    : null
                }
              />
            </div>
            <div>
              <LinhaMetadata
                rotulo="Convergencia funcional"
                valor={
                  convergenciaFuncional === null
                    ? "sem veredito"
                    : convergenciaFuncional
                      ? "sim"
                      : "nao"
                }
              />
              <LinhaMetadata
                rotulo="Estrategia de referencia"
                valor={
                  comparacaoAlternativa &&
                  typeof comparacaoAlternativa.estrategia_referencia === "string"
                    ? comparacaoAlternativa.estrategia_referencia
                    : null
                }
              />
              <LinhaMetadata
                rotulo="SQL principal de referencia"
                valor={
                  comparacaoAlternativa &&
                  typeof comparacaoAlternativa.sql_principal_referencia ===
                    "string"
                    ? comparacaoAlternativa.sql_principal_referencia
                    : null
                }
              />
              <LinhaMetadata
                rotulo="Chaves faltantes / extras"
                valor={
                  comparacaoAlternativa &&
                  typeof comparacaoAlternativa.quantidade_chaves_faltantes ===
                    "number" &&
                  typeof comparacaoAlternativa.quantidade_chaves_extras ===
                    "number"
                    ? `${comparacaoAlternativa.quantidade_chaves_faltantes} / ${comparacaoAlternativa.quantidade_chaves_extras}`
                    : null
                }
              />
            </div>
          </div>
        </div>
      )}

      {dados.id === "contato" ? (
        <div className="space-y-4">
          <DossieContatoDetalhe dados={dados} viewMode={viewMode} />
          {resumoComparacoes && (
            <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-4">
              <div className="mb-3 flex items-start justify-between gap-3">
                <div className="text-sm font-semibold text-white">
                  Resumo consolidado das comparacoes
                </div>
                <button
                  type="button"
                  onClick={() => mutacaoRelatorioComparacao.mutate()}
                  disabled={mutacaoRelatorioComparacao.isPending}
                  className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-[11px] text-slate-200 transition-colors hover:bg-slate-800 disabled:cursor-not-allowed disabled:border-slate-800 disabled:bg-slate-950 disabled:text-slate-500"
                >
                  {mutacaoRelatorioComparacao.isPending
                    ? "Gerando relatorio..."
                    : "Gerar relatorio tecnico"}
                </button>
              </div>
              <div className="space-y-1 text-[11px] text-slate-300">
                <div>
                  Total de comparacoes registradas:{" "}
                  {resumoComparacoes.totalComparacoes}
                </div>
                <div>
                  Funcional: {resumoComparacoes.convergenciasFuncionais}{" "}
                  convergencias | {resumoComparacoes.divergenciasFuncionais}{" "}
                  divergencias
                </div>
                <div>
                  Basica: {resumoComparacoes.convergenciasBasicas} convergencias
                  | {resumoComparacoes.divergenciasBasicas} divergencias
                </div>
                {resumoComparacoes.ultimoStatusComparacao && (
                  <div>
                    Ultimo veredito: {resumoComparacoes.ultimoStatusComparacao}
                  </div>
                )}
                {resumoComparacoes.ultimaEstrategia && (
                  <div>
                    Ultima estrategia: {resumoComparacoes.ultimaEstrategia}
                  </div>
                )}
                {resumoComparacoes.ultimaSqlPrincipal && (
                  <div>
                    Ultima SQL principal: {resumoComparacoes.ultimaSqlPrincipal}
                  </div>
                )}
                {resumoComparacoes.ultimaEstrategiaReferencia && (
                  <div>
                    Estrategia de referencia:{" "}
                    {resumoComparacoes.ultimaEstrategiaReferencia}
                  </div>
                )}
                {resumoComparacoes.ultimaSqlPrincipalReferencia && (
                  <div>
                    SQL principal de referencia:{" "}
                    {resumoComparacoes.ultimaSqlPrincipalReferencia}
                  </div>
                )}
                {resumoComparacoes.ultimoCacheKey && (
                  <div>
                    Ultimo cache comparado: {resumoComparacoes.ultimoCacheKey}
                  </div>
                )}
                {(typeof resumoComparacoes.ultimoTotalChavesFaltantes ===
                  "number" ||
                  typeof resumoComparacoes.ultimoTotalChavesExtras ===
                    "number") && (
                  <div>
                    Ultimas chaves faltantes / extras:{" "}
                    {String(
                      resumoComparacoes.ultimoTotalChavesFaltantes ?? "-",
                    )}{" "}
                    /{" "}
                    {String(resumoComparacoes.ultimoTotalChavesExtras ?? "-")}
                  </div>
                )}
                <div>
                  Historico consolidado: {resumoComparacoes.historyFile}
                </div>
              </div>
              {mutacaoRelatorioComparacao.data && (
                <div className="mt-4 rounded-lg border border-slate-800 bg-slate-900/70 p-3 text-[11px] text-slate-300">
                  <div className="font-medium text-white">
                    Relatorio tecnico materializado
                  </div>
                  <div className="mt-1">
                    Arquivo: {mutacaoRelatorioComparacao.data.reportFile}
                  </div>
                  {mutacaoRelatorioComparacao.data.updatedAt && (
                    <div>
                      Atualizado em:{" "}
                      {formatar_data_atualizacao(
                        mutacaoRelatorioComparacao.data.updatedAt,
                      )}
                    </div>
                  )}
                  <pre className="mt-3 max-h-64 overflow-auto whitespace-pre-wrap rounded border border-slate-800 bg-slate-950/70 p-3 text-[10px] text-slate-300">
                    {mutacaoRelatorioComparacao.data.content}
                  </pre>
                </div>
              )}
              {mutacaoRelatorioComparacao.isError && (
                <div className="mt-3 text-[11px] text-rose-300">
                  Falha ao gerar o relatorio tecnico da comparacao.
                </div>
              )}
            </div>
          )}
          {historicoComparacoes && historicoComparacoes.items.length > 0 && (
            <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-4">
              <div className="mb-3 text-sm font-semibold text-white">
                Ultimas comparacoes entre estrategias
              </div>
              <div className="space-y-2 text-[11px] text-slate-300">
                {historicoComparacoes.items.map((item, indice) => {
                  const registro = item as Record<string, unknown>;
                  const comparacao =
                    registro.comparacao_estrategia_alternativa as
                      | Record<string, unknown>
                      | undefined;
                  return (
                    <div
                      key={`${String(registro.cache_key ?? indice)}_${indice}`}
                      className="rounded-lg border border-slate-800 bg-slate-900/70 p-3"
                    >
                      <div>Cache: {String(registro.cache_key ?? "-")}</div>
                      <div>
                        Estrategia:{" "}
                        {String(registro.estrategia_execucao ?? "-")}
                      </div>
                      <div>
                        SQL principal: {String(registro.sql_principal ?? "-")}
                      </div>
                      {comparacao && (
                        <div>
                          Comparacao: funcional=
                          {String(comparacao.convergencia_funcional ?? "-")} |
                          faltantes=
                          {String(
                            comparacao.quantidade_chaves_faltantes ?? "-",
                          )}{" "}
                          | extras=
                          {String(comparacao.quantidade_chaves_extras ?? "-")}
                        </div>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      ) : (
        <div className="space-y-3">
          <div className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-slate-800 bg-slate-950/40 p-3">
            <div className="text-xs text-slate-400">
              Perfil de tabela: <span className="font-medium text-slate-200">{tableProfile}</span>
            </div>
            <div className="flex items-center gap-2">
              <div className="flex items-center gap-1 rounded-lg border border-slate-700 bg-slate-900/80 p-1">
                <button
                  type="button"
                  onClick={() => setTableProfile("compacto")}
                  className={`rounded-md px-3 py-1.5 text-[11px] font-medium transition-colors ${
                    tableProfile === "compacto"
                      ? "border border-blue-600/60 bg-blue-700/60 text-blue-100"
                      : "text-slate-400 hover:text-slate-200"
                  }`}
                >
                  Compacto
                </button>
                <button
                  type="button"
                  onClick={() => setTableProfile("analitico")}
                  className={`rounded-md px-3 py-1.5 text-[11px] font-medium transition-colors ${
                    tableProfile === "analitico"
                      ? "border border-blue-600/60 bg-blue-700/60 text-blue-100"
                      : "text-slate-400 hover:text-slate-200"
                  }`}
                >
                  Analitico
                </button>
              </div>
              <button
                type="button"
                onClick={() => clearDossieSectionColumnFilters(sectionKey)}
                className="rounded-md border border-slate-700 bg-slate-900 px-2.5 py-1.5 text-[11px] text-slate-300 transition-colors hover:bg-slate-800"
              >
                Limpar filtros
              </button>
            </div>
          </div>
          {arquivosOrigem.length > 0 && (
            <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-4 text-[11px] text-slate-400">
              <div className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
                Arquivos de origem
              </div>
              <div className="space-y-1">
                {arquivosOrigem.map((arquivo) => (
                  <div key={arquivo}>{arquivo}</div>
                ))}
              </div>
            </div>
          )}
          <div className="h-[520px] rounded-xl border border-slate-800 bg-slate-950/40">
            <DataTable
              columns={dados.columns}
              rows={dados.rows}
              totalRows={dados.rowCount}
              sortBy={tableState?.sortBy ?? undefined}
              sortDesc={tableState?.sortDesc ?? false}
              onSortChange={(col, desc) =>
                setDossieSectionSort(sectionKey, col, desc)
              }
              showColumnFilters
              columnFilters={tableState?.columnFilters ?? {}}
              onColumnFilterChange={(col, value) =>
                setDossieSectionColumnFilter(sectionKey, col, value)
              }
              hiddenColumns={
                tableProfile === "compacto" ? hiddenColumnsCompacto : undefined
              }
            />
          </div>
        </div>
      )}
    </div>
  );
}

function extrair_cnpj_do_caminho(caminho: string): string {
  const correspondencias = String(caminho).match(/\d{14}/g);
  return correspondencias?.[0] ?? "";
}

export default DossieSectionDetail;
