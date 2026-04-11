interface FiscalRowDetailPanelProps {
  row?: Record<string, unknown> | null;
  title?: string;
  subtitle?: string;
  emptyMessage?: string;
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.map((item) => formatValue(item)).join(", ");
  if (typeof value === "object") return JSON.stringify(value, null, 2);
  return String(value);
}

export function FiscalRowDetailPanel({
  row,
  title = "Detalhe do registro",
  subtitle = "Selecione uma linha da tabela para inspecionar todos os campos.",
  emptyMessage = "Nenhuma linha selecionada.",
}: FiscalRowDetailPanelProps) {
  const entries = Object.entries(row ?? {});

  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 text-sm font-semibold text-white">{title}</div>
      <div className="text-xs text-slate-500">{subtitle}</div>

      {entries.length === 0 ? (
        <div className="mt-4 rounded-xl border border-slate-800 bg-slate-950/30 px-4 py-6 text-sm text-slate-500">
          {emptyMessage}
        </div>
      ) : (
        <div className="mt-4 space-y-4">
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {entries.map(([key, value]) => (
              <div key={key} className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
                <div className="text-[11px] uppercase tracking-wide text-slate-500">{key}</div>
                <div className="mt-2 break-words text-sm text-slate-200">{formatValue(value)}</div>
              </div>
            ))}
          </div>

          <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
            <div className="text-[11px] uppercase tracking-wide text-slate-500">JSON bruto</div>
            <pre className="mt-2 overflow-auto whitespace-pre-wrap break-words text-xs text-slate-300">
              {JSON.stringify(row, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </section>
  );
}

export default FiscalRowDetailPanel;
