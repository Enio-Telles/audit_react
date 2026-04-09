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
import { DossieSectionGrid } from "./DossieSectionGrid";
import { DossieDetailPanel } from "./DossieDetailPanel";
import type { EstadoSincronizacaoSecao } from "../utils/dossie_helpers";
import {
  extrairMensagemErro,
  filtrarSecoesPendentesSincronizacao,
  montarMensagemSucesso,
  selecionarSecaoPrioritaria,
} from "../utils/dossie_helpers";
import { useAppStore } from "../../../store/appStore";

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

  const {
    data: sections,
    isLoading,
    isError,
  } = useQuery<DossieSectionSummary[]>({
    queryKey: ["dossie_sections", cnpj],
    queryFn: () => dossieApi.getSecoes(cnpj!),
    enabled: !!cnpj,
  });

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

          <div className="grid gap-4 xl:grid-cols-[minmax(0,1.35fr)_minmax(360px,0.95fr)] xl:items-start">
            <div className="min-w-0">
              <DossieSectionGrid
                sections={sections}
                viewMode={viewMode}
                secaoEmSincronizacao={secaoEmSincronizacao}
                estadoPorSecao={estadoPorSecao}
                secaoSelecionadaId={secaoSelecionada?.id ?? null}
                onSincronizar={(section) => mutacaoSincronizacao.mutate(section)}
                onVisualizar={(section) => setSecaoSelecionada(section)}
                sincronizacaoPendente={mutacaoSincronizacao.isPending}
                usarSqlConsolidadoContato={usarSqlConsolidadoContato}
              />
            </div>

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
              <div className="rounded-2xl border border-dashed border-slate-700 bg-slate-900/30 p-5 xl:sticky xl:top-4">
                <div className="text-xs uppercase tracking-wide text-slate-500">
                  Painel de detalhe
                </div>
                <div className="mt-2 text-sm font-medium text-slate-200">
                  Nenhuma secao selecionada
                </div>
                <p className="mt-2 text-sm text-slate-400">
                  Use o botao <span className="font-medium text-slate-200">Visualizar</span> em uma secao
                  ou abra a secao prioritaria para inspecionar o cache materializado sem perder o
                  contexto do grid.
                </p>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

export default DossieTab;
