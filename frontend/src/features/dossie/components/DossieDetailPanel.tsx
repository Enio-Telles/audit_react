import type { DossieSectionData } from "../types";
import { DossieSectionDetail } from "./DossieSectionDetail";
import type { DossieViewMode } from "../utils/dossie_helpers";
import type { DossieSectionSummary } from "../types";

interface DossieDetailPanelProps {
  secaoSelecionada: DossieSectionSummary;
  dadosSecao?: DossieSectionData;
  carregando: boolean;
  erro: boolean;
  viewMode?: DossieViewMode;
  onFechar: () => void;
}

export function DossieDetailPanel({
  secaoSelecionada,
  dadosSecao,
  carregando,
  erro,
  viewMode,
  onFechar,
}: DossieDetailPanelProps) {
  return (
    <div>
      <div className="mb-3 flex items-center justify-between">
        <div className="flex items-center gap-2 text-xs text-slate-400">
          <span className="font-medium text-slate-200">
            {secaoSelecionada.title}
          </span>
          <span>·</span>
          <span>{secaoSelecionada.id}</span>
        </div>
        <button
          type="button"
          onClick={onFechar}
          aria-label="Fechar detalhe"
          className="rounded-md border border-slate-700 bg-slate-900 px-2.5 py-1 text-[11px] text-slate-400 transition-colors hover:bg-slate-800 hover:text-slate-200"
        >
          ✕ Fechar detalhe
        </button>
      </div>

      {carregando && (
        <div className="flex h-40 items-center justify-center rounded-xl border border-slate-800 bg-slate-950/40 text-sm text-slate-400">
          Carregando dados da secao...
        </div>
      )}
      {erro && (
        <div className="flex flex-col h-40 items-center justify-center rounded-xl border border-amber-800/40 bg-amber-950/20 text-sm text-amber-500/80 p-4 text-center">
          <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" className="mb-2 opacity-50">
            <circle cx="12" cy="12" r="10"></circle>
            <line x1="12" y1="8" x2="12" y2="12"></line>
            <line x1="12" y1="16" x2="12.01" y2="16"></line>
          </svg>
          <div className="font-medium text-amber-500/90 mb-1">Nenhum dado armazenado para esta seção</div>
          <div className="text-xs">Se não houver dados em cache, por favor, execute a sincronização.</div>
        </div>
      )}
      {!carregando && !erro && dadosSecao && (
        <DossieSectionDetail dados={dadosSecao} viewMode={viewMode} />
      )}
    </div>
  );
}
