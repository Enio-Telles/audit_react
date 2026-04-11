import type { DossieSectionSummary } from "../types";
import { formatarDataAtualizacao } from "../utils/dossie_helpers";
import { DossieBadge } from "./DossieBadge";

interface DossieHeaderProps {
  cnpj: string;
  razaoSocial?: string | null;
  sections?: DossieSectionSummary[];
  usarSqlConsolidadoContato: boolean;
  onToggleSqlConsolidadoContato: (value: boolean) => void;
  sincronizacaoPendente?: boolean;
  onAbrirSecaoPrioritaria?: () => void;
  onSincronizarPendentes?: () => void;
}

export function DossieHeader({
  cnpj,
  razaoSocial,
  sections,
  usarSqlConsolidadoContato,
  onToggleSqlConsolidadoContato,
  sincronizacaoPendente = false,
  onAbrirSecaoPrioritaria,
  onSincronizarPendentes,
}: DossieHeaderProps) {
  const ultimaAtualizacao = sections
    ?.map((s) => s.updatedAt)
    .filter(Boolean)
    .sort()
    .at(-1);

  const totalPendentes =
    sections?.filter((secao) => secao.syncEnabled && secao.status === "idle")
      .length ?? 0;
  const totalErros =
    sections?.filter((secao) => secao.status === "error").length ?? 0;
  const totalDivergencias =
    sections?.filter(
      (secao) =>
        secao.alternateStrategyComparison === "divergencia_funcional" ||
        secao.alternateStrategyComparison === "divergencia_basica",
    ).length ?? 0;

  const dataFormatada = formatarDataAtualizacao(ultimaAtualizacao);

  return (
    <div className="rounded-2xl border border-slate-700 bg-slate-900/60 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="mb-1 text-xs uppercase tracking-wide text-slate-400">
            Dossie principal
          </div>
          <h2 className="font-mono text-xl font-bold tracking-wide text-white">
            {cnpj}
          </h2>
          {razaoSocial && (
            <div className="mt-1 text-sm font-medium text-slate-300">
              {razaoSocial}
            </div>
          )}
          {dataFormatada && (
            <div className="mt-1 text-xs text-slate-500">
              Ultima atualizacao: {dataFormatada}
            </div>
          )}
        </div>
        <div className="flex flex-col items-end gap-2">
          {sections && (
            <DossieBadge
              rotulo="Escopo"
              valor={`${sections.length} ${sections.length === 1 ? "secao" : "secoes"}`}
            />
          )}
          <div className="flex flex-wrap items-center justify-end gap-2 text-[11px]">
            <DossieBadge rotulo="Pendentes" valor={totalPendentes} />
            <DossieBadge rotulo="Erros" valor={totalErros} variante="erro" />
            <DossieBadge
              rotulo="Divergencias"
              valor={totalDivergencias}
              variante="alerta"
            />
          </div>
        </div>
      </div>
      <div className="mt-4 flex flex-wrap items-center gap-2">
        <button
          type="button"
          onClick={onAbrirSecaoPrioritaria}
          disabled={!sections?.length}
          className="rounded-lg border border-slate-700 bg-slate-950/70 px-3 py-1.5 text-xs font-medium text-slate-200 transition-colors hover:bg-slate-800 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-500 disabled:cursor-not-allowed disabled:border-slate-800 disabled:bg-slate-950 disabled:text-slate-500"
        >
          Abrir secao prioritaria
        </button>
        <button
          type="button"
          onClick={onSincronizarPendentes}
          disabled={(!totalPendentes && !totalErros) || sincronizacaoPendente}
          aria-busy={sincronizacaoPendente}
          className="rounded-lg border border-blue-700 bg-blue-900/30 px-3 py-1.5 text-xs font-medium text-blue-200 transition-colors hover:bg-blue-800/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 disabled:cursor-not-allowed disabled:border-slate-700 disabled:bg-slate-800 disabled:text-slate-500 flex items-center justify-center min-w-[150px]"
        >
          {sincronizacaoPendente ? (
            <>
              <svg
                className="animate-spin -ml-1 mr-2 h-4 w-4 text-blue-200"
                xmlns="http://www.w3.org/2000/svg"
                fill="none"
                viewBox="0 0 24 24"
              >
                <circle
                  className="opacity-25"
                  cx="12"
                  cy="12"
                  r="10"
                  stroke="currentColor"
                  strokeWidth="4"
                ></circle>
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                ></path>
              </svg>
              Sincronizando...
            </>
          ) : (
            "Sincronizar pendentes"
          )}
        </button>
      </div>
      <label className="mt-4 flex cursor-pointer items-start gap-3 rounded-xl border border-slate-700/80 bg-slate-950/60 p-3 text-sm text-slate-300">
        <input
          type="checkbox"
          checked={usarSqlConsolidadoContato}
          onChange={(e) => onToggleSqlConsolidadoContato(e.target.checked)}
          className="mt-0.5 h-4 w-4 rounded border-slate-600 bg-slate-900 text-blue-500"
        />
        <span>
          <span className="block font-medium text-white">
            Secao contato: usar SQL consolidado
          </span>
          <span className="block text-xs text-slate-400">
            Quando habilitado, a sincronizacao de `contato` prioriza
            `dossie_contato.sql`. Quando desabilitado, permanece o fluxo padrao
            por composicao em Polars sobre datasets compartilhados.
          </span>
        </span>
      </label>
    </div>
  );
}
