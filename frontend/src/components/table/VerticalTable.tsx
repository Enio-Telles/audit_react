import { memo, useMemo } from "react";
import type { ReactNode } from "react";

interface VerticalTableProps<T> {
  data: T | null;
  columns: {
    header: string;
    accessorKey: keyof T;
    cell?: (value: T[keyof T], row: T) => ReactNode;
    hidden?: boolean;
  }[];
  title?: string;
}

const VerticalTableBase = <T,>({
  data,
  columns,
  title,
}: VerticalTableProps<T>) => {
  const visibleColumns = useMemo(() => columns.filter((c) => !c.hidden), [columns]);

  if (!data) {
    return (
      <div className="rounded-xl border border-dashed border-slate-800 bg-slate-950/30 px-4 py-8 text-center text-sm text-slate-500">
        Nenhum dado disponível.
      </div>
    );
  }

  return (
    <div className="overflow-hidden rounded-xl border border-slate-800 bg-slate-900/50">
      {title && (
        <div className="border-b border-slate-800 bg-slate-900/80 px-4 py-3 text-xs font-semibold uppercase tracking-wider text-slate-400">
          {title}
        </div>
      )}
      <table className="w-full text-left text-sm">
        <tbody className="divide-y divide-slate-800/60">
          {visibleColumns.map((col, index) => {
            const rawValue = data[col.accessorKey];
            const renderedValue = col.cell ? col.cell(rawValue, data) : String(rawValue ?? "—");

            return (
              <tr
                key={String(col.accessorKey)}
                className={`transition-colors hover:bg-slate-800/30 ${
                  index % 2 === 0 ? "bg-transparent" : "bg-slate-900/20"
                }`}
              >
                {/* Cabeçalho da linha (célula da esquerda) */}
                <th
                  scope="row"
                  className="w-1/3 min-w-[140px] border-r border-slate-800/60 bg-slate-900/40 px-4 py-3 align-top font-medium text-slate-400"
                >
                  {col.header}
                </th>
                {/* Valor da linha (célula da direita) */}
                <td className="px-4 py-3 align-top text-slate-200">
                  {renderedValue}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
};

export const VerticalTable = memo(VerticalTableBase) as typeof VerticalTableBase;
