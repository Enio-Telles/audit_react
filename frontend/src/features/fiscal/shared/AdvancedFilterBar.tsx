import { useState, useCallback } from "react";

interface FilterRule {
  id: string;
  column: string;
  operator: "contém" | "igual" | "diferente" | "maior" | "menor" | "vazio" | "não vazio";
  value: string;
}

interface AdvancedFilterBarProps {
  /** Available column names for filtering */
  columns: string[];
  /** Current global search text */
  filterText: string;
  /** Callback when global search changes */
  onFilterTextChange: (text: string) => void;
  /** Current column-level filter */
  filterColumn?: string;
  filterValue?: string;
  /** Callback when column filter changes */
  onColumnFilterChange?: (column: string | undefined, value: string | undefined) => void;
  /** Column presets for quick selection */
  columnPresets?: { label: string; columns: string[] }[];
  /** Callback when a column preset is selected */
  onPresetSelect?: (columns: string[]) => void;
}

const OPERATORS = [
  { value: "contém", label: "contém" },
  { value: "igual", label: "= igual" },
  { value: "diferente", label: "≠ diferente" },
  { value: "maior", label: "> maior" },
  { value: "menor", label: "< menor" },
  { value: "vazio", label: "∅ vazio" },
  { value: "não vazio", label: "✦ não vazio" },
] as const;

export function AdvancedFilterBar({
  columns,
  filterText,
  onFilterTextChange,
  filterColumn,
  filterValue,
  onColumnFilterChange,
  columnPresets,
  onPresetSelect,
}: AdvancedFilterBarProps) {
  const [showAdvanced, setShowAdvanced] = useState(false);
  const [localColumn, setLocalColumn] = useState(filterColumn ?? "");
  const [localOperator, setLocalOperator] = useState<string>("contém");
  const [localValue, setLocalValue] = useState(filterValue ?? "");

  const applyColumnFilter = useCallback(() => {
    if (onColumnFilterChange && localColumn) {
      onColumnFilterChange(localColumn, localValue || undefined);
    }
  }, [onColumnFilterChange, localColumn, localValue]);

  const clearColumnFilter = useCallback(() => {
    setLocalColumn("");
    setLocalValue("");
    onColumnFilterChange?.(undefined, undefined);
  }, [onColumnFilterChange]);

  return (
    <div className="space-y-2">
      {/* Main search bar */}
      <div className="flex items-center gap-2">
        <div className="relative flex-1">
          <svg
            className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-500"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            strokeWidth={2}
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
            />
          </svg>
          <input
            type="text"
            placeholder="Buscar em todas as colunas…"
            value={filterText}
            onChange={(e) => onFilterTextChange(e.target.value)}
            className="w-full rounded-lg border border-slate-700 bg-slate-900/60 py-2 pl-10 pr-4 text-sm text-slate-200 placeholder-slate-500 focus:border-blue-600 focus:outline-none focus:ring-1 focus:ring-blue-600"
          />
        </div>

        <button
          onClick={() => setShowAdvanced(!showAdvanced)}
          className={`rounded-lg border px-3 py-2 text-xs font-medium transition-colors ${
            showAdvanced || filterColumn
              ? "border-blue-600 bg-blue-950/40 text-blue-300"
              : "border-slate-700 bg-slate-900/60 text-slate-400 hover:bg-slate-800"
          }`}
          title="Filtros avançados"
        >
          <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M3 4a1 1 0 011-1h16a1 1 0 011 1v2.586a1 1 0 01-.293.707l-6.414 6.414a1 1 0 00-.293.707V17l-4 4v-6.586a1 1 0 00-.293-.707L3.293 7.293A1 1 0 013 6.586V4z" />
          </svg>
        </button>

        {/* Column presets */}
        {columnPresets && columnPresets.length > 0 && onPresetSelect && (
          <div className="flex items-center gap-1">
            {columnPresets.map((preset) => (
              <button
                key={preset.label}
                onClick={() => onPresetSelect(preset.columns)}
                className="rounded-lg border border-slate-700 bg-slate-900/60 px-2.5 py-2 text-[11px] font-medium text-slate-400 transition-colors hover:bg-slate-800 hover:text-slate-200"
                title={`Preset: ${preset.columns.join(", ")}`}
              >
                {preset.label}
              </button>
            ))}
          </div>
        )}
      </div>

      {/* Advanced filter row */}
      {showAdvanced && (
        <div className="flex flex-wrap items-center gap-2 rounded-lg border border-slate-700/60 bg-slate-950/40 px-3 py-2">
          <span className="text-[11px] uppercase tracking-wide text-slate-500">Filtro por coluna</span>

          <select
            value={localColumn}
            onChange={(e) => setLocalColumn(e.target.value)}
            className="rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-slate-300 focus:border-blue-600 focus:outline-none"
          >
            <option value="">— coluna —</option>
            {columns.map((col) => (
              <option key={col} value={col}>
                {col}
              </option>
            ))}
          </select>

          <select
            value={localOperator}
            onChange={(e) => setLocalOperator(e.target.value)}
            className="rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-slate-300 focus:border-blue-600 focus:outline-none"
          >
            {OPERATORS.map((op) => (
              <option key={op.value} value={op.value}>
                {op.label}
              </option>
            ))}
          </select>

          {localOperator !== "vazio" && localOperator !== "não vazio" && (
            <input
              type="text"
              placeholder="valor…"
              value={localValue}
              onChange={(e) => setLocalValue(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && applyColumnFilter()}
              className="flex-1 rounded border border-slate-700 bg-slate-900 px-2 py-1 text-xs text-slate-300 placeholder-slate-600 focus:border-blue-600 focus:outline-none"
            />
          )}

          <button
            onClick={applyColumnFilter}
            disabled={!localColumn}
            className="rounded border border-blue-700 bg-blue-950/40 px-2.5 py-1 text-[11px] font-medium text-blue-300 transition-colors hover:bg-blue-900/40 disabled:opacity-40"
          >
            Aplicar
          </button>

          {(filterColumn || filterValue) && (
            <button
              onClick={clearColumnFilter}
              className="rounded border border-slate-600 px-2.5 py-1 text-[11px] font-medium text-slate-400 transition-colors hover:bg-slate-800"
            >
              Limpar
            </button>
          )}
        </div>
      )}
    </div>
  );
}

export default AdvancedFilterBar;
