import React from "react";

type Props = {
  columns: string[];
  rows: Record<string, unknown>[];
  onRowClick?: (row: Record<string, unknown>) => void;
};

export function FiscalDataTable({ columns, rows, onRowClick }: Props) {
  return (
    <div className="overflow-auto rounded border bg-white">
      <table className="min-w-full border-collapse text-sm">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column} className="border-b bg-slate-50 px-3 py-2 text-left font-semibold">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr
              key={idx}
              className="hover:bg-slate-50"
              onClick={() => onRowClick?.(row)}
              style={{ cursor: onRowClick ? "pointer" : "default" }}
            >
              {columns.map((column) => (
                <td key={column} className="border-b px-3 py-2 align-top">
                  {formatCell(row[column])}
                </td>
              ))}
            </tr>
          ))}
          {rows.length === 0 ? (
            <tr>
              <td colSpan={Math.max(columns.length, 1)} className="px-3 py-6 text-center text-slate-500">
                Sem dados para o filtro atual.
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}

function formatCell(value: unknown) {
  if (value === null || value === undefined) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}
