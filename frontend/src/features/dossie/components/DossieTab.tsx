import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { dossieApi } from '../../../api/client';
import type { DossieSectionData, DossieSectionSummary, DossieSyncResponse, DossieTabProps } from '../types';
import { DossieSectionDetail } from './DossieSectionDetail';

type EstadoSincronizacaoSecao = {
  mensagem?: string;
  tipo?: 'sucesso' | 'erro';
};

function obter_rotulo_status(status: DossieSectionSummary['status']): string {
  switch (status) {
    case 'cached':
      return 'Cache disponivel';
    case 'loading':
      return 'Sincronizando';
    case 'fresh':
      return 'Atualizado';
    case 'error':
      return 'Erro';
    default:
      return 'Aguardando';
  }
}

function obter_classes_status(status: DossieSectionSummary['status']): string {
  if (status === 'cached' || status === 'fresh') {
    return 'border-green-800 bg-green-900/30 text-green-300';
  }

  if (status === 'loading') {
    return 'border-amber-700 bg-amber-900/30 text-amber-200';
  }

  if (status === 'error') {
    return 'border-rose-800 bg-rose-900/30 text-rose-300';
  }

  return 'border-slate-600 text-slate-300';
}

function formatar_quantidade_linhas(rowCount?: number): string {
  if (rowCount === undefined || rowCount === null) {
    return 'sem carga';
  }

  return `${rowCount} ${rowCount === 1 ? 'linha' : 'linhas'}`;
}

function formatar_data_atualizacao(updatedAt?: string | null): string | null {
  if (!updatedAt) {
    return null;
  }

  const data = new Date(updatedAt);
  if (Number.isNaN(data.getTime())) {
    return null;
  }

  return data.toLocaleString('pt-BR');
}

function extrair_mensagem_erro(error: unknown): string {
  if (typeof error === 'object' && error !== null && 'response' in error) {
    const erroHttp = error as {
      response?: { data?: { detail?: string } };
      message?: string;
    };

    if (erroHttp.response?.data?.detail) {
      return erroHttp.response.data.detail;
    }

    if (erroHttp.message) {
      return erroHttp.message;
    }
  }

  if (error instanceof Error) {
    return error.message;
  }

  return 'Falha ao sincronizar a secao.';
}

function obter_rotulo_estrategia_execucao(resultado: DossieSyncResponse): string {
  if (resultado.estrategia_execucao === 'sql_consolidado') {
    return 'via SQL consolidado';
  }

  if (resultado.estrategia_execucao === 'composicao_polars') {
    return 'via composicao reutilizavel';
  }

  return 'via SQL direto';
}

function formatar_estrategia_resumo(estrategia?: string | null): string | null {
  if (estrategia === 'sql_consolidado') {
    return 'SQL consolidado';
  }

  if (estrategia === 'composicao_polars') {
    return 'composicao Polars';
  }

  if (estrategia === 'sql_direto') {
    return 'SQL direto';
  }

  return estrategia ?? null;
}

function formatar_comparacao_resumo(comparacao?: string | null): { texto: string; classe: string } | null {
  if (comparacao === 'convergencia_funcional') {
    return { texto: 'Convergencia funcional com estrategia alternada', classe: 'text-emerald-300' };
  }

  if (comparacao === 'divergencia_funcional') {
    return { texto: 'Divergencia funcional com estrategia alternada', classe: 'text-amber-300' };
  }

  if (comparacao === 'convergencia_basica') {
    return { texto: 'Convergencia basica com estrategia alternada', classe: 'text-emerald-300' };
  }

  if (comparacao === 'divergencia_basica') {
    return { texto: 'Divergencia basica com estrategia alternada', classe: 'text-amber-300' };
  }

  return null;
}

function montar_mensagem_sucesso(resultado: DossieSyncResponse): string {
  const quantidadeLinhas = `${resultado.linhas_extraidas} ${resultado.linhas_extraidas === 1 ? 'linha atualizada' : 'linhas atualizadas'}`;
  const sqlExecutadas = resultado.sql_ids_executados?.length ?? 0;
  const sqlReutilizadas = resultado.sql_ids_reutilizados?.length ?? 0;
  const estrategia = obter_rotulo_estrategia_execucao(resultado);
  const sqlPrincipal = resultado.sql_principal ? ` - principal ${resultado.sql_principal}` : '';
  const comparacaoAlternativa =
    resultado.comparacao_estrategia_alternativa &&
    typeof resultado.comparacao_estrategia_alternativa === 'object'
      ? resultado.comparacao_estrategia_alternativa
      : null;
  const convergenciaBasica =
    comparacaoAlternativa && typeof comparacaoAlternativa.convergencia_basica === 'boolean'
      ? (comparacaoAlternativa.convergencia_basica as boolean)
      : null;
  const convergenciaFuncional =
    comparacaoAlternativa && typeof comparacaoAlternativa.convergencia_funcional === 'boolean'
      ? (comparacaoAlternativa.convergencia_funcional as boolean)
      : null;
  const sufixoComparacao =
    convergenciaFuncional === false
      ? ' - divergencia funcional com estrategia alternada'
      : convergenciaBasica === null
      ? ''
      : convergenciaBasica
        ? ' - convergencia basica com estrategia alternada'
        : ' - divergencia basica com estrategia alternada';

  return `${quantidadeLinhas} - ${estrategia}${sqlPrincipal} - ${sqlExecutadas} SQL Oracle - ${sqlReutilizadas} reutilizadas${sufixoComparacao}`;
}

function montar_status_apresentado(
  secao: DossieSectionSummary,
  secaoEmSincronizacao: string | null,
  estadoLocal?: EstadoSincronizacaoSecao,
): DossieSectionSummary['status'] {
  if (secaoEmSincronizacao === secao.id) {
    return 'loading';
  }

  if (estadoLocal?.tipo === 'erro') {
    return 'error';
  }

  if (estadoLocal?.tipo === 'sucesso') {
    return 'fresh';
  }

  return secao.status;
}

export function DossieTab({ cnpj, razaoSocial }: DossieTabProps) {
  const queryClient = useQueryClient();
  const [secaoEmSincronizacao, setSecaoEmSincronizacao] = useState<string | null>(null);
  const [estadoPorSecao, setEstadoPorSecao] = useState<Record<string, EstadoSincronizacaoSecao>>({});
  const [secaoSelecionada, setSecaoSelecionada] = useState<DossieSectionSummary | null>(null);
  const [usarSqlConsolidadoContato, setUsarSqlConsolidadoContato] = useState(false);

  const { data: sections, isLoading, isError } = useQuery<DossieSectionSummary[]>({
    queryKey: ['dossie_sections', cnpj],
    queryFn: () => dossieApi.getSecoes(cnpj!),
    enabled: !!cnpj,
  });

  const {
    data: dadosSecao,
    isLoading: carregandoDadosSecao,
    isError: erroDadosSecao,
  } = useQuery<DossieSectionData>({
    queryKey: ['dossie_section_data', cnpj, secaoSelecionada?.id],
    queryFn: () => dossieApi.getDadosSecao(cnpj!, secaoSelecionada!.id),
    enabled: !!cnpj && !!secaoSelecionada,
  });

  const mutacaoSincronizacao = useMutation({
    mutationFn: async (secao: DossieSectionSummary): Promise<DossieSyncResponse> => {
      const parametros =
        secao.id === 'contato' && usarSqlConsolidadoContato
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
        [secao.id]: {
          tipo: 'sucesso',
          mensagem: montar_mensagem_sucesso(resultado),
        },
      }));

      await queryClient.invalidateQueries({ queryKey: ['dossie_sections', cnpj] });
      await queryClient.invalidateQueries({ queryKey: ['dossie_section_data', cnpj, secao.id] });
    },
    onError: (error, secao) => {
      setEstadoPorSecao((estadoAnterior) => ({
        ...estadoAnterior,
        [secao.id]: {
          tipo: 'erro',
          mensagem: extrair_mensagem_erro(error),
        },
      }));
    },
    onSettled: () => {
      setSecaoEmSincronizacao(null);
    },
  });

  if (!cnpj) {
    return (
      <div className="flex h-full w-full items-center justify-center p-6 text-slate-300">
        <div className="max-w-xl rounded-2xl border border-slate-700 bg-slate-900/60 p-6">
          <h2 className="mb-2 text-lg font-semibold text-white">Dossie indisponivel</h2>
          <p className="text-sm text-slate-400">
            Selecione um CNPJ para abrir o dossie, reaproveitar extracoes ja existentes e evitar duplicacao de dados.
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full overflow-auto p-4 text-slate-200">
      <div className="mb-4 rounded-2xl border border-slate-700 bg-slate-900/60 p-4">
        <div className="mb-1 text-xs uppercase tracking-wide text-slate-400">Dossie principal</div>
        <h2 className="text-lg font-semibold text-white">{cnpj}</h2>
        {razaoSocial && <div className="mt-1 text-sm text-slate-400">{razaoSocial}</div>}
        <p className="mt-3 text-sm text-slate-400">
          Esta area concentra a navegacao do dossie por CNPJ, priorizando reuso de consultas SQL, persistencia por secao e
          leitura amigavel dos dados.
        </p>
        <label className="mt-4 flex items-start gap-3 rounded-xl border border-slate-700/80 bg-slate-950/60 p-3 text-sm text-slate-300">
          <input
            type="checkbox"
            checked={usarSqlConsolidadoContato}
            onChange={(event) => setUsarSqlConsolidadoContato(event.target.checked)}
            className="mt-0.5 h-4 w-4 rounded border-slate-600 bg-slate-900 text-blue-500"
          />
          <span>
            <span className="block font-medium text-white">Secao contato: usar SQL consolidado</span>
            <span className="block text-xs text-slate-400">
              Quando habilitado, a sincronizacao de `contato` prioriza `dossie_contato.sql`. Quando desabilitado, permanece o fluxo
              padrao por composicao em Polars sobre datasets compartilhados.
            </span>
          </span>
        </label>
      </div>

      {isLoading && <div className="text-sm text-slate-400">Carregando secoes do dossie...</div>}
      {isError && <div className="text-sm text-red-400">Erro ao carregar as secoes do dossie.</div>}

      {!isLoading && !isError && sections && (
        <div className="space-y-4">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {sections.map((section) => {
              const dataAtualizacao = formatar_data_atualizacao(section.updatedAt);
              const estrategiaResumo = formatar_estrategia_resumo(section.executionStrategy);
              const comparacaoResumo = formatar_comparacao_resumo(section.alternateStrategyComparison);
              const estadoLocal = estadoPorSecao[section.id];
              const statusApresentado = montar_status_apresentado(section, secaoEmSincronizacao, estadoLocal);
              const estaSincronizando = secaoEmSincronizacao === section.id;
              const podeVisualizar = section.status !== 'idle' || estadoLocal?.tipo === 'sucesso';
              const permiteSincronizacao = section.syncEnabled;
              const estaSelecionada = secaoSelecionada?.id === section.id;

              return (
                <div
                  key={section.id}
                  className={`rounded-2xl border border-slate-700 border-t-2 bg-slate-900/50 p-4 transition-colors hover:border-t-blue-500 hover:bg-slate-800/80 ${estaSelecionada ? 'border-blue-600/70 ring-1 ring-blue-500/40' : ''}`}
                >
                  <div className="mb-2 flex items-center justify-between gap-2">
                    <h3 className="text-sm font-semibold text-white">{section.title}</h3>
                    <span className={`rounded-full border px-2 py-0.5 text-[10px] ${obter_classes_status(statusApresentado)}`}>
                      {obter_rotulo_status(statusApresentado)}
                    </span>
                  </div>
                  <p className="mb-3 text-sm text-slate-400">{section.description}</p>
                  <div className="flex items-center justify-between text-xs text-slate-500">
                    <span>Fonte: {section.sourceType}</span>
                    <span>{formatar_quantidade_linhas(section.rowCount)}</span>
                  </div>
                  {section.sourceFiles && section.sourceFiles.length > 0 && (
                    <div className="mt-2 text-[11px] text-slate-500">
                      Arquivos de origem: {section.sourceFiles.length}
                    </div>
                  )}
                  {estrategiaResumo && (
                    <div className="mt-2 text-[11px] text-slate-500">
                      Ultima materializacao: {estrategiaResumo}
                      {section.primarySql ? ` - ${section.primarySql}` : ''}
                    </div>
                  )}
                  {comparacaoResumo && (
                    <div className={`mt-2 text-[11px] ${comparacaoResumo.classe}`}>
                      {comparacaoResumo.texto}
                    </div>
                  )}
                  {comparacaoResumo &&
                    (section.alternateStrategyMissingKeys !== undefined || section.alternateStrategyExtraKeys !== undefined) && (
                      <div className="mt-1 text-[11px] text-slate-500">
                        Chaves faltantes: {section.alternateStrategyMissingKeys ?? 0} | Chaves extras: {section.alternateStrategyExtraKeys ?? 0}
                      </div>
                    )}
                  {section.id === 'contato' && (
                    <div className="mt-2 text-[11px] text-slate-500">
                      Estrategia solicitada: {usarSqlConsolidadoContato ? 'SQL consolidado' : 'composicao Polars com reuso'}
                    </div>
                  )}
                  {dataAtualizacao && <div className="mt-2 text-[11px] text-slate-500">Atualizado em {dataAtualizacao}</div>}

                  <div className="mt-4 flex items-center justify-between gap-3">
                    <div className="flex gap-2">
                      <button
                        type="button"
                        onClick={() => mutacaoSincronizacao.mutate(section)}
                        disabled={!permiteSincronizacao || estaSincronizando || mutacaoSincronizacao.isPending}
                        className="rounded-lg border border-blue-700 bg-blue-900/30 px-3 py-1.5 text-xs font-medium text-blue-200 transition-colors hover:bg-blue-800/50 disabled:cursor-not-allowed disabled:border-slate-700 disabled:bg-slate-800 disabled:text-slate-500"
                      >
                        {!permiteSincronizacao ? 'Cache reutilizado' : estaSincronizando ? 'Sincronizando...' : 'Sincronizar'}
                      </button>
                      <button
                        type="button"
                        onClick={() => setSecaoSelecionada(section)}
                        disabled={!podeVisualizar}
                        className="rounded-lg border border-slate-600 bg-slate-800/70 px-3 py-1.5 text-xs font-medium text-slate-200 transition-colors hover:bg-slate-700 disabled:cursor-not-allowed disabled:border-slate-800 disabled:bg-slate-900 disabled:text-slate-500"
                      >
                        Visualizar
                      </button>
                    </div>
                    <div className="min-h-[20px] flex-1 text-right text-[11px]">
                      {estadoLocal?.mensagem && (
                        <span className={estadoLocal.tipo === 'erro' ? 'text-rose-300' : 'text-emerald-300'}>
                          {estadoLocal.mensagem}
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              );
            })}
          </div>

          {secaoSelecionada && (
            <div className="space-y-3">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <div className="text-xs uppercase tracking-wide text-slate-500">Secao selecionada</div>
                  <h3 className="text-base font-semibold text-white">{secaoSelecionada.title}</h3>
                </div>
                <button
                  type="button"
                  onClick={() => setSecaoSelecionada(null)}
                  className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs text-slate-300 transition-colors hover:bg-slate-800"
                >
                  Fechar visualizacao
                </button>
              </div>

              {carregandoDadosSecao && <div className="text-sm text-slate-400">Carregando dados materializados da secao...</div>}
              {erroDadosSecao && <div className="text-sm text-rose-400">Erro ao carregar o cache materializado da secao.</div>}
              {dadosSecao && <DossieSectionDetail dados={dadosSecao} />}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default DossieTab;
