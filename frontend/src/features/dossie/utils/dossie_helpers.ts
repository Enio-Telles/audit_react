import type { DossieSectionSummary, DossieSyncResponse } from '../types';

export type DossieViewMode = 'executivo' | 'auditoria';
export type DossieTableProfile = 'compacto' | 'analitico';

export type EstadoSincronizacaoSecao = {
  mensagem?: string;
  tipo?: 'sucesso' | 'erro';
};

// ⚡ Cached Intl.DateTimeFormat instance — use instead of Date.prototype.toLocaleString()
export const dateTimeFormatter = new Intl.DateTimeFormat('pt-BR', {
  dateStyle: 'short',
  timeStyle: 'medium',
});

export function obterRotuloStatus(status: DossieSectionSummary['status']): string {
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

export function obterClassesStatus(status: DossieSectionSummary['status']): string {
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

export function obterVarianteStatus(
  status: DossieSectionSummary['status'],
): 'neutra' | 'info' | 'sucesso' | 'alerta' | 'erro' {
  if (status === 'cached') return 'info';
  if (status === 'fresh') return 'sucesso';
  if (status === 'loading') return 'alerta';
  if (status === 'error') return 'erro';
  return 'neutra';
}

export function formatarQuantidadeLinhas(rowCount?: number): string {
  if (rowCount === undefined || rowCount === null) {
    return 'sem carga';
  }
  return `${rowCount} ${rowCount === 1 ? 'linha' : 'linhas'}`;
}

export function formatarDataAtualizacao(updatedAt?: string | null): string | null {
  if (!updatedAt) return null;
  const data = new Date(updatedAt);
  if (Number.isNaN(data.getTime())) return null;
  return dateTimeFormatter.format(data);
}

export function formatarEstrategiaResumo(estrategia?: string | null): string | null {
  if (estrategia === 'sql_consolidado') return 'SQL consolidado';
  if (estrategia === 'composicao_polars') return 'composicao Polars';
  if (estrategia === 'sql_direto') return 'SQL direto';
  return estrategia ?? null;
}

export function formatarFonteResumo(
  sourceType?: DossieSectionSummary['sourceType'] | null,
): string | null {
  if (sourceType === 'sql_catalog') return 'SQL catalogada';
  if (sourceType === 'xml_fallback') return 'Fallback XML';
  if (sourceType === 'mixed') return 'Fonte mista';
  if (sourceType === 'composed') return 'Composicao';
  if (sourceType === 'cache_catalog') return 'Cache catalogado';
  return sourceType ?? null;
}

export function obterVarianteFonte(
  sourceType?: DossieSectionSummary['sourceType'] | null,
): 'neutra' | 'info' | 'sucesso' | 'alerta' | 'erro' {
  if (sourceType === 'composed') return 'sucesso';
  if (sourceType === 'sql_catalog' || sourceType === 'cache_catalog') return 'info';
  if (sourceType === 'mixed') return 'alerta';
  if (sourceType === 'xml_fallback') return 'erro';
  return 'neutra';
}

export function formatarComparacaoResumo(
  comparacao?: string | null,
): { texto: string; classe: string } | null {
  if (comparacao === 'convergencia_funcional') {
    return {
      texto: 'Convergencia funcional com estrategia alternada',
      classe: 'text-emerald-300',
    };
  }
  if (comparacao === 'divergencia_funcional') {
    return {
      texto: 'Divergencia funcional com estrategia alternada',
      classe: 'text-amber-300',
    };
  }
  if (comparacao === 'convergencia_basica') {
    return {
      texto: 'Convergencia basica com estrategia alternada',
      classe: 'text-emerald-300',
    };
  }
  if (comparacao === 'divergencia_basica') {
    return {
      texto: 'Divergencia basica com estrategia alternada',
      classe: 'text-amber-300',
    };
  }
  return null;
}

export function obterVarianteComparacao(
  comparacao?: string | null,
): 'neutra' | 'info' | 'sucesso' | 'alerta' | 'erro' {
  if (
    comparacao === 'convergencia_funcional' ||
    comparacao === 'convergencia_basica'
  ) {
    return 'sucesso';
  }
  if (
    comparacao === 'divergencia_funcional' ||
    comparacao === 'divergencia_basica'
  ) {
    return 'alerta';
  }
  return 'neutra';
}

export function obterRotuloEstrategiaExecucao(resultado: DossieSyncResponse): string {
  if (resultado.estrategia_execucao === 'sql_consolidado') return 'via SQL consolidado';
  if (resultado.estrategia_execucao === 'composicao_polars') return 'via composicao reutilizavel';
  return 'via SQL direto';
}

export function montarMensagemSucesso(resultado: DossieSyncResponse): string {
  const quantidadeLinhas = `${resultado.linhas_extraidas} ${resultado.linhas_extraidas === 1 ? 'linha atualizada' : 'linhas atualizadas'}`;
  const sqlExecutadas = resultado.sql_ids_executados?.length ?? 0;
  const sqlReutilizadas = resultado.sql_ids_reutilizados?.length ?? 0;
  const estrategia = obterRotuloEstrategiaExecucao(resultado);
  const sqlPrincipal = resultado.sql_principal ? ` - principal ${resultado.sql_principal}` : '';

  const comparacaoAlternativa =
    resultado.comparacao_estrategia_alternativa &&
    typeof resultado.comparacao_estrategia_alternativa === 'object'
      ? resultado.comparacao_estrategia_alternativa
      : null;
  const convergenciaBasica =
    comparacaoAlternativa &&
    typeof comparacaoAlternativa.convergencia_basica === 'boolean'
      ? (comparacaoAlternativa.convergencia_basica as boolean)
      : null;
  const convergenciaFuncional =
    comparacaoAlternativa &&
    typeof comparacaoAlternativa.convergencia_funcional === 'boolean'
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

export function extrairMensagemErro(error: unknown): string {
  if (typeof error === 'object' && error !== null && 'response' in error) {
    const erroHttp = error as {
      response?: { data?: { detail?: string } };
      message?: string;
    };
    if (erroHttp.response?.data?.detail) return erroHttp.response.data.detail;
    if (erroHttp.message) return erroHttp.message;
  }
  if (error instanceof Error) return error.message;
  return 'Falha ao sincronizar a secao.';
}

export function montarStatusApresentado(
  secao: DossieSectionSummary,
  secaoEmSincronizacao: string | null,
  estadoLocal?: EstadoSincronizacaoSecao,
): DossieSectionSummary['status'] {
  if (secaoEmSincronizacao === secao.id) return 'loading';
  if (estadoLocal?.tipo === 'erro') return 'error';
  if (estadoLocal?.tipo === 'sucesso') return 'fresh';
  return secao.status;
}

export function selecionarSecaoPrioritaria(
  secoes: DossieSectionSummary[],
): DossieSectionSummary | null {
  if (!secoes.length) return null;

  const comErro = secoes.find((secao) => secao.status === 'error');
  if (comErro) return comErro;

  const comDivergencia = secoes.find(
    (secao) =>
      secao.alternateStrategyComparison === 'divergencia_funcional' ||
      secao.alternateStrategyComparison === 'divergencia_basica',
  );
  if (comDivergencia) return comDivergencia;

  const pendente = secoes.find((secao) => secao.syncEnabled && secao.status === 'idle');
  if (pendente) return pendente;

  const comCache = secoes.find(
    (secao) => secao.status === 'cached' || secao.status === 'fresh',
  );
  if (comCache) return comCache;

  return secoes[0] ?? null;
}

export function filtrarSecoesPendentesSincronizacao(
  secoes: DossieSectionSummary[],
): DossieSectionSummary[] {
  return secoes.filter(
    (secao) =>
      secao.syncEnabled &&
      (secao.status === 'idle' || secao.status === 'error'),
  );
}
