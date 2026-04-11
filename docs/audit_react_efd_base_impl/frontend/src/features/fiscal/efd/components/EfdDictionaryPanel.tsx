import React from "react";

type FieldItem = { field: string; label: string; description: string };

export function EfdDictionaryPanel({ fields }: { fields: FieldItem[] }) {
  return (
    <div className="rounded border bg-white p-3">
      <div className="mb-2 text-sm font-semibold">Dicionário do registro</div>
      <div className="space-y-2">
        {fields.map((field) => (
          <div key={field.field} className="rounded border p-2">
            <div className="text-sm font-medium">{field.label} <span className="text-slate-500">({field.field})</span></div>
            <div className="text-xs text-slate-600">{field.description}</div>
          </div>
        ))}
        {fields.length === 0 ? <div className="text-sm text-slate-500">Sem dicionário cadastrado.</div> : null}
      </div>
    </div>
  );
}
