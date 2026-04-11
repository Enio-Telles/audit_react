import type { EfdRecordOption } from "../../api";

interface EfdRecordSwitcherProps {
  records: EfdRecordOption[];
  value: string;
  onChange: (value: string) => void;
}

export function EfdRecordSwitcher({
  records,
  value,
  onChange,
}: EfdRecordSwitcherProps) {
  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 text-sm font-semibold text-white">Registros EFD</div>
      <div className="flex flex-wrap gap-2">
        {records.map((item) => {
          const active = item.record === value;
          return (
            <button
              key={item.record}
              type="button"
              onClick={() => onChange(item.record)}
              className={`rounded-xl border px-3 py-2 text-left text-xs transition-colors ${
                active
                  ? "border-blue-500 bg-blue-950/40 text-blue-100"
                  : "border-slate-700 bg-slate-950/40 text-slate-300 hover:bg-slate-900/50"
              }`}
            >
              <div className="font-medium">{item.title}</div>
              <div className="mt-1 max-w-xs text-[11px] text-slate-400">{item.description}</div>
            </button>
          );
        })}
      </div>
    </section>
  );
}

export default EfdRecordSwitcher;
