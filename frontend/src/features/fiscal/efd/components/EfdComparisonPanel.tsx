import type { EfdCompareResponse } from "../../api";

interface EfdComparisonPanelProps {
  data: EfdCompareResponse | null;
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
      <div className="text-[11px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-2 text-sm font-semibold text-white">{value}</div>
    </div>
  );
}

function KeyList({ title, values }: { title: string; values: string[] }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
      <div className="mb-2 text-xs font-semibold text-slate-300">{title}</div>
      <div className="max-h-40 space-y-2 overflow-auto">
        {values.length === 0 ? (
          <div className="text-xs text-slate-500">Sem amostra disponivel.</div>
        ) : (
          values.map((value) => (
            <div key={value} className="rounded-lg border border-slate-800 bg-slate-950/60 px-2 py-1 text-xs text-slate-300">
              {value}
            </div>
          ))
        )}
      </div>
    </div>
  );
}

export function EfdComparisonPanel({ data }: EfdComparisonPanelProps) {
  if (!data) {
    return (
      <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4 text-sm text-slate-500">
        Informe dois periodos para comparar o registro atual.
      </section>
    );
  }

  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 text-sm font-semibold text-white">Comparacao entre periodos</div>
      <div className="grid gap-3 md:grid-cols-5">
        <Stat label="Periodo A" value={data.periodo_a} />
        <Stat label="Periodo B" value={data.periodo_b} />
        <Stat label="Incluidos" value={String(data.summary.added)} />
        <Stat label="Removidos" value={String(data.summary.removed)} />
        <Stat label="Interseccao" value={String(data.summary.intersection)} />
      </div>
      <div className="mt-4 grid gap-3 md:grid-cols-3">
        <KeyList title="Chaves incluidas" values={data.sample.added_keys} />
        <KeyList title="Chaves removidas" values={data.sample.removed_keys} />
        <KeyList title="Chaves em comum" values={data.sample.intersection_keys} />
      </div>
    </section>
  );
}

export default EfdComparisonPanel;
