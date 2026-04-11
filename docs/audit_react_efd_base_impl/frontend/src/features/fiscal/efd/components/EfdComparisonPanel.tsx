import React from "react";
import type { EfdCompareResponse } from "../api";

export function EfdComparisonPanel({ data }: { data: EfdCompareResponse | null }) {
  if (!data) {
    return (
      <div className="rounded border bg-white p-3 text-sm text-slate-500">
        Informe dois períodos para comparar.
      </div>
    );
  }

  return (
    <div className="rounded border bg-white p-3">
      <div className="mb-3 text-sm font-semibold">Comparação entre períodos</div>
      <div className="grid grid-cols-2 gap-3 md:grid-cols-5">
        <Stat label="Período A" value={data.periodo_a} />
        <Stat label="Período B" value={data.periodo_b} />
        <Stat label="Incluídos" value={String(data.summary.added)} />
        <Stat label="Removidos" value={String(data.summary.removed)} />
        <Stat label="Interseção" value={String(data.summary.intersection)} />
      </div>
      <div className="mt-3 grid gap-3 md:grid-cols-3">
        <KeyList title="Chaves incluídas" values={data.sample.added_keys} />
        <KeyList title="Chaves removidas" values={data.sample.removed_keys} />
        <KeyList title="Chaves em comum" values={data.sample.intersection_keys} />
      </div>
    </div>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded border p-2">
      <div className="text-xs text-slate-500">{label}</div>
      <div className="text-sm font-semibold">{value}</div>
    </div>
  );
}

function KeyList({ title, values }: { title: string; values: string[] }) {
  return (
    <div className="rounded border p-2">
      <div className="mb-2 text-xs font-semibold text-slate-600">{title}</div>
      <div className="max-h-40 overflow-auto space-y-1">
        {values.length === 0 ? <div className="text-xs text-slate-400">Sem amostra.</div> : null}
        {values.map((value) => (
          <div key={value} className="rounded bg-slate-50 px-2 py-1 text-xs">
            {value}
          </div>
        ))}
      </div>
    </div>
  );
}
