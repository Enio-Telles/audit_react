import type { FiscalDomainSummary } from "../types";

interface FiscalDomainOverviewProps {
  data?: FiscalDomainSummary;
  isLoading: boolean;
  errorMessage?: string;
  onOpenShortcut?: (id: string) => void;
}

function StatusBadge({ status }: { status: string }) {
  return (
    <span className="rounded-full border border-slate-600 bg-slate-800 px-2 py-1 text-[11px] uppercase tracking-wide text-slate-300">
      {status.replaceAll("_", " ")}
    </span>
  );
}

export function FiscalDomainOverview({
  data,
  isLoading,
  errorMessage,
  onOpenShortcut,
}: FiscalDomainOverviewProps) {
  if (isLoading) {
    return (
      <div className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4 text-sm text-slate-400">
        Carregando resumo do domínio fiscal...
      </div>
    );
  }

  if (errorMessage) {
    return (
      <div className="rounded-2xl border border-rose-800/60 bg-rose-950/20 p-4 text-sm text-rose-200">
        {errorMessage}
      </div>
    );
  }

  if (!data) {
    return (
      <div className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4 text-sm text-slate-400">
        Nenhum resumo disponível para este domínio.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
        <div className="flex flex-wrap items-center gap-2">
          <div className="text-sm font-semibold text-white">{data.title}</div>
          <StatusBadge status={data.status} />
          {data.cnpj ? (
            <span className="rounded-full border border-blue-700/60 bg-blue-950/30 px-2 py-1 font-mono text-[11px] text-blue-200">
              {data.cnpj}
            </span>
          ) : (
            <span className="rounded-full border border-amber-700/60 bg-amber-950/20 px-2 py-1 text-[11px] text-amber-200">
              sem CNPJ selecionado
            </span>
          )}
        </div>
        <p className="mt-2 text-sm text-slate-400">{data.subtitle}</p>
        <div className="mt-3 text-xs uppercase tracking-wide text-slate-500">
          Pipeline oficial: {data.pipeline}
        </div>
      </section>

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        {data.cards.map((card) => (
          <article
            key={card.id}
            className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4"
          >
            <div className="text-xs uppercase tracking-wide text-slate-500">{card.title}</div>
            <div className="mt-2 text-sm font-semibold text-white">{card.value}</div>
            <p className="mt-2 text-sm text-slate-400">{card.description}</p>
          </article>
        ))}
      </section>

      <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
        <div className="text-sm font-semibold text-white">Datasets previstos</div>
        <div className="mt-3 space-y-3">
          {data.datasets.map((dataset) => (
            <div
              key={dataset.id}
              className="rounded-xl border border-slate-700 bg-slate-950/40 p-3"
            >
              <div className="flex flex-wrap items-center gap-2">
                <div className="text-sm font-medium text-white">{dataset.label}</div>
                <span className="rounded-full border border-slate-600 bg-slate-800 px-2 py-0.5 text-[11px] uppercase tracking-wide text-slate-300">
                  {dataset.stage.replaceAll("_", " ")}
                </span>
              </div>
              <div className="mt-2 font-mono text-xs text-blue-300">{dataset.id}</div>
              <p className="mt-2 text-sm text-slate-400">{dataset.description}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="grid gap-4 xl:grid-cols-[1fr_1fr]">
        <div className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="text-sm font-semibold text-white">Próximos passos</div>
          <ul className="mt-3 space-y-2 text-sm text-slate-400">
            {data.next_steps.map((step) => (
              <li key={step} className="rounded-lg border border-slate-800 bg-slate-950/30 px-3 py-2">
                {step}
              </li>
            ))}
          </ul>
        </div>

        {data.legacy_shortcuts.length > 0 ? (
          <div className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
            <div className="text-sm font-semibold text-white">Compatibilidade legada</div>
            <div className="mt-3 space-y-3">
              {data.legacy_shortcuts.map((shortcut) => (
                <div
                  key={shortcut.id}
                  className="rounded-xl border border-slate-700 bg-slate-950/40 p-3"
                >
                  <div className="text-sm font-medium text-white">{shortcut.label}</div>
                  <p className="mt-2 text-sm text-slate-400">{shortcut.description}</p>
                  {onOpenShortcut ? (
                    <button
                      onClick={() => onOpenShortcut(shortcut.id)}
                      className="mt-3 rounded-lg border border-slate-600 bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700"
                    >
                      Abrir aba legada
                    </button>
                  ) : null}
                </div>
              ))}
            </div>
          </div>
        ) : null}
      </section>
    </div>
  );
}

export default FiscalDomainOverview;
