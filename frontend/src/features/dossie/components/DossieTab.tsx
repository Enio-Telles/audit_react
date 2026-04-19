import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { dossieApi } from "../../../api/client";
import type {
  DossieSectionData,
  DossieSectionSummary,
  DossieTabProps,
} from "../types";
import { DossieHeader } from "./DossieHeader";
import { DossieKpis } from "./DossieKpis";
import { DossieViewModeToggle } from "./DossieViewModeToggle";
import { DossieDetailPanel } from "./DossieDetailPanel";

import type { EstadoSincronizacaoSecao } from "../utils/dossie_helpers";
import {
  extrairMensagemErro,
  filtrarSecoesPendentesSincronizacao,
  montarMensagemSucesso,
  selecionarSecaoPrioritaria,
} from "../utils/dossie_helpers";
import { useAppStore } from "../../../store/appStore";

// ─── Status dot colors ───────────────────────────────────────────────────────
const STATUS_CORES: Record<string, string> = {
  fresh: "bg-emerald-400",
  cached: "bg-sky-400",
  loading: "bg-amber-400 animate-pulse",
  error: "bg-rose-400",
  idle: "bg-slate-500",
};

function obterCorStatus(status: string): string {
  return STATUS_CORES[status] ?? STATUS_CORES.idle;
}

// ─── Section Strip Pill ──────────────────────────────────────────────────────
function SectionPill({
  section,
  isActive,
  isSyncing,
  onClick,
  onSync,
}: {
  section: DossieSectionSummary;
  isActive: boolean;
  isSyncing: boolean;
  onClick: () => void;
  onSync: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`group flex shrink-0 items-center gap-2 rounded-lg border px-3 py-2 text-xs font-medium transition-all ${
        isActive
          ? "border-blue-500/60 bg-blue-950/50 text-blue-100 shadow-sm shadow-blue-900/30"
          : "border-slate-700/80 bg-slate-900/40 text-slate-300 hover:border-slate-600 hover:bg-slate-800/50"
      }`}
    >
      <span
        className={`h-2 w-2 shrink-0 rounded-full ${obterCorStatus(section.status)}`}
      />
      <span className="truncate max-w-[120px]">{section.title}</span>
      {typeof section.rowCount === "number" && section.rowCount > 0 && (
        <span className="rounded-md bg-slate-800/80 px-1.5 py-0.5 text-[10px] font-medium text-slate-400">
          {section.rowCount}
        </span>
      )}
      {section.syncEnabled && (
        <button
          type="button"
          onClick={(e) => {
            e.stopPropagation();
            onSync();
          }}
          disabled={isSyncing}
          title="Sincronizar seção"
          className={`ml-0.5 rounded p-0.5 text-[11px] outline-none transition-colors ${
            isSyncing
              ? "animate-spin text-blue-400"
              : "text-slate-500 opacity-0 group-hover:opacity-100 focus-visible:opacity-100 focus-visible:ring-2 focus-visible:ring-blue-400 hover:text-blue-300"
          }`}
        >
          <svg xmlns="http://www.w3.org/2000/svg" width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M21 12a9 9 0 1 1-9-9c2.52 0 4.93 1 6.74 2.74L21 8"></path>
            <path d="M21 3v5h-5"></path>
          </svg>
        </button>
      )}
    </button>
  );
}

export function DossieTab({ cnpj, razaoSocial }: DossieTabProps) {
  const queryClient = useQueryClient();
  const [secaoEmSincronizacao, setSecaoEmSincronizacao] = useState<
    string | null
  >(null);
  const [estadoPorSecao, setEstadoPorSecao] = useState<
    Record<string, EstadoSincronizacaoSecao>
  >({});
  const [secaoSelecionada, setSecaoSelecionada] =
    useState<DossieSectionSummary | null>(null);
  const viewMode = useAppStore((state) => state.dossieViewMode);
  const setViewMode = useAppStore((state) => state.setDossieViewMode);
  const usarSqlConsolidadoContato = useAppStore(
    (state) => state.dossieUsarSqlConsolidadoContato,
  );
  const setUsarSqlConsolidadoContato = useAppStore(
    (state) => state.setDossieUsarSqlConsolidadoContato,
  );

  const [didAutoSelect, setDidAutoSelect] = useState(false);

  const {
    data: sections,
    isLoading,
    isError,
  } = useQuery<DossieSectionSummary[]>({
    queryKey: ["dossie_sections", cnpj],
    queryFn: () => dossieApi.getSecoes(cnpj!),
    enabled: !!cnpj,
  });

  // Auto-select priority section once query data is available
  if (sections?.length && !secaoSelecionada && !didAutoSelect) {
    const prioritaria = selecionarSecaoPrioritaria(sections);
    if (prioritaria) {
      // Schedule state updates for next microtask to avoid setState during render
      queueMicrotask(() => {
        setSecaoSelecionada(prioritaria);
        setDidAutoSelect(true);
      });
    }
  }

  const {
    data: dadosSecao,
    isLoading: carregandoDadosSecao,
    isError: erroDadosSecao,
  } = useQuery<DossieSectionData>({
    queryKey: ["dossie_section_data", cnpj, secaoSelecionada?.id],
    queryFn: () => dossieApi.getDadosSecao(cnpj!, secaoSelecionada!.id),
    enabled: !!cnpj && !!secaoSelecionada,
  });

  const mutacaoSincronizacao = useMutation({
    mutationFn: async (
      secao: DossieSectionSummary,
    ) => {
      const parametros =
        secao.id === "contato" && usarSqlConsolidadoContato
          ? { usar_sql_consolidado: true }
          : undefined;
      return dossieApi.syncSecao(cnpj!, secao.id, parametros);
    },
    onMutate: (secao) => {
      setSecaoEmSincronizacao(secao.id);
      setEstadoPorSecao((estadoAnterior) => ({
        ...estadoAnterior,
        [secao.id]: {},
      }));
    },
    onSuccess: async (resultado, secao) => {
      setEstadoPorSecao((estadoAnterior) => ({
        ...estadoAnterior,
        [secao.id]: { tipo: "sucesso", mensagem: montarMensagemSucesso(resultado) },
      }));

      await queryClient.invalidateQueries({
        queryKey: ["dossie_sections", cnpj],
      });
      await queryClient.invalidateQueries({
        queryKey: ["dossie_section_data", cnpj, secao.id],
      });
    },
    onError: (error, secao) => {
      setEstadoPorSecao((estadoAnterior) => ({
        ...estadoAnterior,
        [secao.id]: { tipo: "erro", mensagem: extrairMensagemErro(error) },
      }));
    },
    onSettled: () => {
      setSecaoEmSincronizacao(null);
    },
  });

  async function sincronizarSecao(secao: DossieSectionSummary) {
    return mutacaoSincronizacao.mutateAsync(secao);
  }

  function abrirSecaoPrioritaria() {
    if (!sections?.length) return;
    const secaoPrioritaria = selecionarSecaoPrioritaria(sections);
    if (secaoPrioritaria) {
      setSecaoSelecionada(secaoPrioritaria);
    }
  }

  async function sincronizarPendentes() {
    if (!sections?.length || mutacaoSincronizacao.isPending) return;
    const secoesPendentes = filtrarSecoesPendentesSincronizacao(sections);
    for (const secao of secoesPendentes) {
      // Mantem execucao sequencial para preservar feedback por secao e evitar corrida visual.
      await sincronizarSecao(secao);
    }
  }

  if (!cnpj) {
    return (
      <div className="flex h-full w-full items-center justify-center p-6 text-slate-300">
        <div className="max-w-xl rounded-2xl border border-slate-700 bg-slate-900/60 p-6">
          <h2 className="mb-2 text-lg font-semibold text-white">
            Dossie indisponivel
          </h2>
          <p className="text-sm text-slate-400">
            Selecione um CNPJ para abrir o dossie, reaproveitar extracoes ja
            existentes e evitar duplicacao de dados.
          </p>
        </div>
      </div>
    );
  }

  // Sync feedback message for active section
  const estadoAtivo = secaoSelecionada
    ? estadoPorSecao[secaoSelecionada.id]
    : undefined;

  return (
    <div className="h-full overflow-auto p-4 text-slate-200">
      <DossieHeader
        cnpj={cnpj}
        razaoSocial={razaoSocial}
        sections={sections}
        usarSqlConsolidadoContato={usarSqlConsolidadoContato}
        onToggleSqlConsolidadoContato={setUsarSqlConsolidadoContato}
        sincronizacaoPendente={mutacaoSincronizacao.isPending}
        onAbrirSecaoPrioritaria={abrirSecaoPrioritaria}
        onSincronizarPendentes={sincronizarPendentes}
      />

      {isLoading && (
        <div className="mt-4 text-sm text-slate-400">
          Carregando secoes do dossie...
        </div>
      )}
      {isError && (
        <div className="mt-4 text-sm text-red-400">
          Erro ao carregar as secoes do dossie.
        </div>
      )}

      {!isLoading && !isError && sections && (
        <div className="mt-4 space-y-4">
          <div>
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
              <span className="text-xs font-medium uppercase tracking-wide text-slate-400">
                Status geral
              </span>
              <DossieViewModeToggle mode={viewMode} onChange={setViewMode} />
            </div>
            <DossieKpis sections={sections} />
          </div>

          {/* ─── Section Strip ──────────────────────────────────────── */}
          <div className="rounded-xl border border-slate-700/80 bg-slate-950/40 p-2">
            <div className="flex items-center gap-1.5 overflow-x-auto pb-1 scrollbar-thin scrollbar-thumb-slate-700">
              {sections.map((section) => (
                <SectionPill
                  key={section.id}
                  section={section}
                  isActive={secaoSelecionada?.id === section.id}
                  isSyncing={secaoEmSincronizacao === section.id}
                  onClick={() => setSecaoSelecionada(section)}
                  onSync={() => mutacaoSincronizacao.mutate(section)}
                />
              ))}
            </div>
            {/* Sync feedback for active section */}
            {estadoAtivo?.tipo && (
              <div
                className={`mt-2 rounded-lg px-3 py-1.5 text-[11px] ${
                  estadoAtivo.tipo === "sucesso"
                    ? "border border-emerald-800/60 bg-emerald-950/30 text-emerald-300"
                    : "border border-rose-800/60 bg-rose-950/30 text-rose-300"
                }`}
              >
                {estadoAtivo.mensagem}
              </div>
            )}
          </div>

          {/* ─── Content Area (full-width) ──────────────────────────── */}
          {secaoSelecionada ? (
            <DossieDetailPanel
              secaoSelecionada={secaoSelecionada}
              dadosSecao={dadosSecao}
              carregando={carregandoDadosSecao}
              erro={erroDadosSecao}
              viewMode={viewMode}
              onFechar={() => setSecaoSelecionada(null)}
            />
          ) : (
            <div className="rounded-2xl border border-dashed border-slate-700 bg-slate-900/30 p-5">
              <div className="text-xs uppercase tracking-wide text-slate-500">
                Painel de detalhe
              </div>
              <div className="mt-2 text-sm font-medium text-slate-200">
                Nenhuma secao selecionada
              </div>
              <p className="mt-2 text-sm text-slate-400">
                Clique em uma secao na barra acima para inspecionar o cache materializado.
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default DossieTab;
