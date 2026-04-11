import type { EfdDictionaryField } from "../../api";

interface EfdDictionaryPanelProps {
  fields: EfdDictionaryField[];
}

export function EfdDictionaryPanel({ fields }: EfdDictionaryPanelProps) {
  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 text-sm font-semibold text-white">Dicionario do registro</div>
      {fields.length === 0 ? (
        <div className="rounded-xl border border-slate-800 bg-slate-950/30 px-4 py-6 text-sm text-slate-500">
          Sem dicionario cadastrado para este registro.
        </div>
      ) : (
        <div className="space-y-3">
          {fields.map((field) => (
            <div key={field.field} className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
              <div className="text-sm font-medium text-white">
                {field.label} <span className="font-mono text-xs text-slate-500">({field.field})</span>
              </div>
              <div className="mt-2 text-sm text-slate-400">{field.description}</div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export default EfdDictionaryPanel;
