import type { DossieSectionData, DossieSectionSummary } from "../types";
import { DossieSectionDetail } from "./DossieSectionDetail";
import type { DossieViewMode } from "../utils/dossie_helpers";

interface DossieDetailPanelProps {
  secaoSelecionada: DossieSectionSummary;
  dadosSecao?: DossieSectionData;
  carregando: boolean;
  erro: boolean;
  viewMode: DossieViewMode;
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
    <aside className="rounded-2xl border border-slate-700 bg-slate-900/70 p-4 xl:sticky xl:top-4">
      <div className="mb-4 flex items-start justify-between gap-3">
        <div>
          <div className="text-xs uppercase tracking-wide text-slate-500">
            Painel de detalhe
          </div>
          <h3 className="text-base font-semibold text-white">
            {secaoSelecionada.title}
          </h3>
          <p className="mt-1 text-xs text-slate-400">
            {secaoSelecionada.description}
          </p>
        </div>
        <button
          type="button"
          onClick={onFechar}
          className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs text-slate-300 transition-colors hover:bg-slate-800"
        >
          Fechar painel
        </button>
      </div>

      {carregando && (
        <div className="text-sm text-slate-400">
          Carregando dados materializados da secao...
        </div>
      )}

      {erro && (
        <div className="text-sm text-rose-400">
          Erro ao carregar o cache materializado da secao.
        </div>
      )}

      {dadosSecao && (
        <DossieSectionDetail dados={dadosSecao} viewMode={viewMode} />
      )}
    </aside>
  );
}

export default DossieDetailPanel;
