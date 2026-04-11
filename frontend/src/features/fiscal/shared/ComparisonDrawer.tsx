import { useMemo } from "react";

interface ComparisonDrawerProps {
  /** The rows selected for comparison (max 2) */
  rows: Array<Record<string, unknown>>;
  /** Whether the drawer is open */
  open: boolean;
  /** Close callback */
  onClose: () => void;
  /** Title of the comparison */
  title?: string;
}

function formatValue(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.map(formatValue).join(", ");
  if (typeof value === "object") return JSON.stringify(value, null, 2);
  return String(value);
}

function DiffIndicator({ a, b }: { a: unknown; b: unknown }) {
  const aStr = formatValue(a);
  const bStr = formatValue(b);
  if (aStr === bStr) {
    return <span className="text-[10px] text-slate-600">═</span>;
  }
  return <span className="text-[10px] text-amber-400" title="Valores diferem">≠</span>;
}

export function ComparisonDrawer({
  rows,
  open,
  onClose,
  title = "Comparação de registros",
}: ComparisonDrawerProps) {
  const allKeys = useMemo(() => {
    const keys = new Set<string>();
    rows.forEach((row) => Object.keys(row).forEach((k) => keys.add(k)));
    return Array.from(keys);
  }, [rows]);

  const diffKeys = useMemo(() => {
    if (rows.length < 2) return new Set<string>();
    return new Set(
      allKeys.filter((k) => formatValue(rows[0][k]) !== formatValue(rows[1][k]))
    );
  }, [rows, allKeys]);

  if (!open) return null;

  return (
    <div className="fixed inset-y-0 right-0 z-50 flex w-full max-w-2xl flex-col border-l border-slate-700 bg-slate-950/95 shadow-2xl backdrop-blur-sm">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-slate-800 px-4 py-3">
        <div>
          <div className="text-sm font-semibold text-white">{title}</div>
          <div className="flex items-center gap-2 text-xs text-slate-500">
            <span>{rows.length} registro(s) selecionado(s)</span>
            {diffKeys.size > 0 && (
              <span className="rounded-full border border-amber-700/60 bg-amber-950/30 px-2 py-0.5 text-[10px] text-amber-300">
                {diffKeys.size} diferença(s)
              </span>
            )}
          </div>
        </div>
        <button
          onClick={onClose}
          className="rounded-lg border border-slate-700 bg-slate-900 px-3 py-1.5 text-xs text-slate-400 transition-colors hover:bg-slate-800 hover:text-white"
        >
          Fechar
        </button>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto p-4">
        {rows.length === 0 ? (
          <div className="mt-12 text-center text-sm text-slate-500">
            Selecione linhas na tabela para comparar.
          </div>
        ) : rows.length === 1 ? (
          <div className="space-y-2">
            {allKeys.map((key) => (
              <div
                key={key}
                className="rounded-lg border border-slate-800 bg-slate-900/40 p-3"
              >
                <div className="text-[11px] uppercase tracking-wide text-slate-500">{key}</div>
                <div className="mt-1 break-words text-sm text-slate-200">
                  {formatValue(rows[0][key])}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="space-y-1">
            {/* Column headers */}
            <div className="sticky top-0 z-10 grid grid-cols-[180px_1fr_24px_1fr] gap-2 rounded-lg bg-slate-900 px-3 py-2 text-[11px] uppercase tracking-wide text-slate-500">
              <div>Campo</div>
              <div>Registro A</div>
              <div />
              <div>Registro B</div>
            </div>

            {allKeys.map((key) => {
              const isDiff = diffKeys.has(key);
              return (
                <div
                  key={key}
                  className={`grid grid-cols-[180px_1fr_24px_1fr] gap-2 rounded-lg border px-3 py-2 ${
                    isDiff
                      ? "border-amber-800/40 bg-amber-950/10"
                      : "border-slate-800/40 bg-slate-950/30"
                  }`}
                >
                  <div
                    className={`truncate text-[11px] font-medium ${
                      isDiff ? "text-amber-300" : "text-slate-500"
                    }`}
                    title={key}
                  >
                    {key}
                  </div>
                  <div className="break-words text-xs text-slate-200">
                    {formatValue(rows[0][key])}
                  </div>
                  <div className="flex items-center justify-center">
                    <DiffIndicator a={rows[0][key]} b={rows[1][key]} />
                  </div>
                  <div className="break-words text-xs text-slate-200">
                    {formatValue(rows[1][key])}
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Footer */}
      <div className="border-t border-slate-800 px-4 py-2 text-[11px] text-slate-600">
        Campos com diferença são destacados em âmbar. Use ≠ para localizar divergências rapidamente.
      </div>
    </div>
  );
}

export default ComparisonDrawer;
