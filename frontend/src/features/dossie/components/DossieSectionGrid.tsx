import type { DossieSectionSummary } from '../types';
import type { DossieViewMode, EstadoSincronizacaoSecao } from '../utils/dossie_helpers';
import {
  obterRotuloStatus,
  obterVarianteStatus,
  formatarQuantidadeLinhas,
  formatarDataAtualizacao,
  formatarEstrategiaResumo,
  formatarComparacaoResumo,
  montarStatusApresentado,
  formatarFonteResumo,
  obterVarianteFonte,
  obterVarianteComparacao,
} from '../utils/dossie_helpers';
import { DossieBadge } from './DossieBadge';

interface DossieSectionCardProps {
  section: DossieSectionSummary;
  viewMode: DossieViewMode;
  secaoEmSincronizacao: string | null;
  estadoLocal?: EstadoSincronizacaoSecao;
  estaSelecionada: boolean;
  sincronizacaoPendente: boolean;
  usarSqlConsolidadoContato: boolean;
  onSincronizar: () => void;
  onVisualizar: () => void;
}

function DossieSectionCard({
  section,
  viewMode,
  secaoEmSincronizacao,
  estadoLocal,
  estaSelecionada,
  sincronizacaoPendente,
  usarSqlConsolidadoContato,
  onSincronizar,
  onVisualizar,
}: DossieSectionCardProps) {
  const dataAtualizacao = formatarDataAtualizacao(section.updatedAt);
  const estrategiaResumo = formatarEstrategiaResumo(section.executionStrategy);
  const comparacaoResumo = formatarComparacaoResumo(section.alternateStrategyComparison);
  const statusApresentado = montarStatusApresentado(section, secaoEmSincronizacao, estadoLocal);
  const estaSincronizando = secaoEmSincronizacao === section.id;
  const podeVisualizar = section.status !== 'idle' || estadoLocal?.tipo === 'sucesso';
  const permiteSincronizacao = section.syncEnabled;

  const statusAccentClass =
    statusApresentado === 'error'
      ? 'border-t-rose-600'
      : statusApresentado === 'loading'
        ? 'border-t-amber-500'
        : statusApresentado === 'fresh'
          ? 'border-t-emerald-500'
          : statusApresentado === 'cached'
            ? 'border-t-blue-500'
            : 'border-t-slate-600';

  return (
    <div
      className={`rounded-2xl border border-slate-700 border-t-2 bg-slate-900/50 p-4 transition-colors hover:bg-slate-800/80 ${statusAccentClass} ${
        estaSelecionada ? 'border-blue-600/70 ring-1 ring-blue-500/40' : ''
      }`}
    >
      <div className="mb-2 flex items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-white">{section.title}</h3>
        <DossieBadge
          rotulo="Status"
          valor={obterRotuloStatus(statusApresentado)}
          variante={obterVarianteStatus(statusApresentado)}
        />
      </div>
      <p className="mb-3 text-sm text-slate-400">{section.description}</p>
      <div className="flex flex-wrap items-center gap-2 text-xs text-slate-500">
        <DossieBadge
          rotulo="Fonte"
          valor={formatarFonteResumo(section.sourceType)}
          variante={obterVarianteFonte(section.sourceType)}
        />
        <DossieBadge rotulo="Linhas" valor={formatarQuantidadeLinhas(section.rowCount)} />
      </div>

      {viewMode === 'auditoria' && (
        <>
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
            <div className="mt-2">
              <DossieBadge
                rotulo="Comparacao"
                valor={comparacaoResumo.texto}
                variante={obterVarianteComparacao(section.alternateStrategyComparison)}
              />
            </div>
          )}
          {comparacaoResumo &&
            (section.alternateStrategyMissingKeys !== undefined ||
              section.alternateStrategyExtraKeys !== undefined) && (
              <div className="mt-1 text-[11px] text-slate-500">
                Chaves faltantes: {section.alternateStrategyMissingKeys ?? 0} | Chaves extras:{' '}
                {section.alternateStrategyExtraKeys ?? 0}
              </div>
            )}
          {section.id === 'contato' && (
            <div className="mt-2 text-[11px] text-slate-500">
              Estrategia solicitada:{' '}
              {usarSqlConsolidadoContato ? 'SQL consolidado' : 'composicao Polars com reuso'}
            </div>
          )}
        </>
      )}

      {dataAtualizacao && (
        <div className="mt-2 text-[11px] text-slate-500">Atualizado em {dataAtualizacao}</div>
      )}

      <div className="mt-4 flex items-center justify-between gap-3">
        <div className="flex gap-2">
          <button
            type="button"
            onClick={onSincronizar}
            disabled={!permiteSincronizacao || estaSincronizando || sincronizacaoPendente}
            className="rounded-lg border border-blue-700 bg-blue-900/30 px-3 py-1.5 text-xs font-medium text-blue-200 transition-colors hover:bg-blue-800/50 disabled:cursor-not-allowed disabled:border-slate-700 disabled:bg-slate-800 disabled:text-slate-500"
          >
            {!permiteSincronizacao
              ? 'Cache reutilizado'
              : estaSincronizando
                ? 'Sincronizando...'
                : 'Sincronizar'}
          </button>
          <button
            type="button"
            onClick={onVisualizar}
            disabled={!podeVisualizar}
            className="rounded-lg border border-slate-600 bg-slate-800/70 px-3 py-1.5 text-xs font-medium text-slate-200 transition-colors hover:bg-slate-700 disabled:cursor-not-allowed disabled:border-slate-800 disabled:bg-slate-900 disabled:text-slate-500"
          >
            Visualizar
          </button>
        </div>
        <div className="min-h-[20px] flex-1 text-right text-[11px]">
          {estadoLocal?.mensagem && (
            <span
              className={estadoLocal.tipo === 'erro' ? 'text-rose-300' : 'text-emerald-300'}
            >
              {estadoLocal.mensagem}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

export interface DossieSectionGridProps {
  sections: DossieSectionSummary[];
  viewMode: DossieViewMode;
  secaoEmSincronizacao: string | null;
  estadoPorSecao: Record<string, EstadoSincronizacaoSecao>;
  secaoSelecionadaId: string | null;
  onSincronizar: (section: DossieSectionSummary) => void;
  onVisualizar: (section: DossieSectionSummary) => void;
  sincronizacaoPendente: boolean;
  usarSqlConsolidadoContato: boolean;
}

export function DossieSectionGrid({
  sections,
  viewMode,
  secaoEmSincronizacao,
  estadoPorSecao,
  secaoSelecionadaId,
  onSincronizar,
  onVisualizar,
  sincronizacaoPendente,
  usarSqlConsolidadoContato,
}: DossieSectionGridProps) {
  return (
    <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
      {sections.map((section) => (
        <DossieSectionCard
          key={section.id}
          section={section}
          viewMode={viewMode}
          secaoEmSincronizacao={secaoEmSincronizacao}
          estadoLocal={estadoPorSecao[section.id]}
          estaSelecionada={secaoSelecionadaId === section.id}
          sincronizacaoPendente={sincronizacaoPendente}
          usarSqlConsolidadoContato={usarSqlConsolidadoContato}
          onSincronizar={() => onSincronizar(section)}
          onVisualizar={() => onVisualizar(section)}
        />
      ))}
    </div>
  );
}
