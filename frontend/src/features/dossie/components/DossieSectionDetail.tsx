import { useMutation, useQuery } from "@tanstack/react-query";
import { dossieApi } from "../../../api/client";
import { DataTable } from "../../../components/table/DataTable";
import type { DossieSectionData } from "../types";
import { DossieContatoDetalhe } from "./DossieContatoDetalhe";

interface DossieSectionDetailProps {
  dados: DossieSectionData;
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

export function DossieSectionDetail({ dados }: DossieSectionDetailProps) {
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
          {dados.metadata && (
            <div className="mt-2 space-y-1 text-[11px] text-slate-400">
              {estrategiaExecucao && (
                <div>Estrategia: {estrategiaExecucao}</div>
              )}
              {sqlPrincipal && <div>SQL principal: {sqlPrincipal}</div>}
              {sqlExecutadas !== null && sqlReutilizadas !== null && (
                <div>
                  SQL Oracle executadas: {sqlExecutadas} | SQL reutilizadas:{" "}
                  {sqlReutilizadas}
                </div>
              )}
              {totalSql !== null && percentualReuso !== null && (
                <div>
                  Total de SQLs da secao: {totalSql} | Reuso efetivo:{" "}
                  {percentualReuso}%
                </div>
              )}
              {tempoMaterializacaoMs !== null && tempoTotalSyncMs !== null && (
                <div>
                  Materializacao: {tempoMaterializacaoMs} ms | Sync total:{" "}
                  {tempoTotalSyncMs} ms
                </div>
              )}
              {impactoCacheFirst && (
                <div>Impacto cache-first: {impactoCacheFirst}</div>
              )}
              {comparacaoAlternativa && (
                <div
                  className={
                    convergenciaBasica ? "text-emerald-300" : "text-amber-300"
                  }
                >
                  Comparacao com estrategia alternada:{" "}
                  {convergenciaBasica === null
                    ? "sem veredito"
                    : convergenciaBasica
                      ? "convergencia basica"
                      : "divergencia basica"}
                </div>
              )}
              {comparacaoAlternativa && (
                <div
                  className={
                    convergenciaFuncional
                      ? "text-emerald-300"
                      : "text-amber-300"
                  }
                >
                  Convergencia funcional:{" "}
                  {convergenciaFuncional === null
                    ? "sem veredito"
                    : convergenciaFuncional
                      ? "sim"
                      : "nao"}
                </div>
              )}
              {comparacaoAlternativa &&
                typeof comparacaoAlternativa.estrategia_referencia ===
                  "string" && (
                  <div>
                    Estrategia de referencia:{" "}
                    {comparacaoAlternativa.estrategia_referencia}
                  </div>
                )}
              {comparacaoAlternativa &&
                typeof comparacaoAlternativa.sql_principal_referencia ===
                  "string" && (
                  <div>
                    SQL principal de referencia:{" "}
                    {comparacaoAlternativa.sql_principal_referencia}
                  </div>
                )}
              {comparacaoAlternativa &&
                typeof comparacaoAlternativa.quantidade_chaves_faltantes ===
                  "number" &&
                typeof comparacaoAlternativa.quantidade_chaves_extras ===
                  "number" && (
                  <div>
                    Chaves faltantes:{" "}
                    {comparacaoAlternativa.quantidade_chaves_faltantes} | Chaves
                    extras: {comparacaoAlternativa.quantidade_chaves_extras}
                  </div>
                )}
              {caminhoHistoricoComparacao && (
                <div>Historico de comparacao: {caminhoHistoricoComparacao}</div>
              )}
              {arquivosOrigem.length > 0 && (
                <div>
                  Arquivos de origem considerados: {arquivosOrigem.length}
                </div>
              )}
              {tabelasOrigemSinteticas.length > 0 && (
                <div>
                  Tabelas/artefatos de origem:{" "}
                  {tabelasOrigemSinteticas.join(" | ")}
                </div>
              )}
            </div>
          )}
        </div>
        <div className="text-right text-[11px] text-slate-500">
          <div>{dados.cacheFile}</div>
          {dataAtualizacao && (
            <div className="mt-1">Atualizado em {dataAtualizacao}</div>
          )}
        </div>
      </div>

      {dados.id === "contato" ? (
        <div className="space-y-4">
          <DossieContatoDetalhe dados={dados} />
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
                {resumoComparacoes.ultimoCacheKey && (
                  <div>
                    Ultimo cache comparado: {resumoComparacoes.ultimoCacheKey}
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
