import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent as EventoMouseReact,
} from "react";
import {
  useReactTable,
  getCoreRowModel,
  getSortedRowModel,
  flexRender,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import type { HighlightRule } from "../../api/types";

interface CompiledHighlightRule extends HighlightRule {
  valueLower: string;
  valueFloat: number;
}

interface DataTableProps {
  columns: string[];
  appearanceKey?: string;
  orderedColumns?: string[];
  columnWidths?: Record<string, number>;
  onOrderedColumnsChange?: (orderedColumns: string[]) => void;
  onColumnWidthChange?: (column: string, width: number) => void;
  rows: Record<string, unknown>[];
  totalRows?: number;
  loading?: boolean;
  page?: number;
  totalPages?: number;
  onPageChange?: (p: number) => void;
  /** @deprecated use autoHighlight */
  highlightRows?: boolean;
  autoHighlight?: boolean;
  rowKey?: string;
  selectedRowKeys?: Set<string>;
  onRowSelect?: (key: string, checked: boolean) => void;
  onSelectAll?: (checked: boolean, visibleKeys: string[]) => void;
  sortBy?: string;
  sortDesc?: boolean;
  onSortChange?: (col: string, desc: boolean) => void;
  hiddenColumns?: Set<string>;
  columnFilters?: Record<string, string>;
  onColumnFilterChange?: (col: string, val: string) => void;
  showColumnFilters?: boolean;
  highlightRules?: HighlightRule[];
}

interface TableAppearance {
  headerBg: string;
  headerText: string;
  filterBg: string;
  rowEvenBg: string;
  rowOddBg: string;
  rowText: string;
  entradaBg: string;
  saidaBg: string;
  selectedBg: string;
}

const DEFAULT_APPEARANCE: TableAppearance = {
  headerBg: "#1b2943",
  headerText: "#dbe7ff",
  filterBg: "#131d31",
  rowEvenBg: "#101927",
  rowOddBg: "#0b1422",
  rowText: "#f8fafc",
  entradaBg: "#184a35",
  saidaBg: "#5e2033",
  selectedBg: "#1d4ed8",
};

function readAppearance(storageKey: string | undefined): TableAppearance {
  if (!storageKey || typeof window === "undefined") return DEFAULT_APPEARANCE;
  try {
    const raw = window.localStorage.getItem(storageKey);
    if (!raw) return DEFAULT_APPEARANCE;
    return { ...DEFAULT_APPEARANCE, ...(JSON.parse(raw) as Partial<TableAppearance>) };
  } catch {
    return DEFAULT_APPEARANCE;
  }
}

const intlInteger = new Intl.NumberFormat("pt-BR");
const intlDecimal = new Intl.NumberFormat("pt-BR", {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.join(" | ");
  if (typeof value === "number") {
    // ⚡ Bolt Optimization: Use cached Intl.NumberFormat instances instead of Number.prototype.toLocaleString()
    // This avoids repeatedly allocating locale data and parsing options on every cell render, improving performance up to ~100x for number formatting.
    if (Number.isInteger(value)) return intlInteger.format(value);
    return intlDecimal.format(value);
  }
  return String(value);
}

function obterLarguraColuna(
  largurasColunas: Record<string, number> | undefined,
  nomeColuna: string,
): number {
  const largura = largurasColunas?.[nomeColuna];
  if (!largura || Number.isNaN(largura)) return 120;
  return Math.max(80, largura);
}

function montarOrdemBaseColunas(
  columns: string[],
  orderedColumns?: string[],
): string[] {
  if (!orderedColumns?.length) return columns;
  return [
    ...orderedColumns.filter((coluna) => columns.includes(coluna)),
    ...columns.filter((coluna) => !orderedColumns.includes(coluna)),
  ];
}

function moverColunaNaOrdem(
  ordemAtual: string[],
  colunaOrigem: string,
  colunaDestino: string,
): string[] {
  if (colunaOrigem === colunaDestino) return ordemAtual;

  const proximaOrdem = [...ordemAtual];
  const indiceOrigem = proximaOrdem.indexOf(colunaOrigem);
  const indiceDestino = proximaOrdem.indexOf(colunaDestino);

  if (indiceOrigem < 0 || indiceDestino < 0) return ordemAtual;

  const [colunaMovida] = proximaOrdem.splice(indiceOrigem, 1);
  proximaOrdem.splice(indiceDestino, 0, colunaMovida);
  return proximaOrdem;
}

function matchesRule(
  rule: CompiledHighlightRule,
  row: Record<string, unknown>,
): boolean {
  const cellVal = String(row[rule.column] ?? "");

  switch (rule.operator) {
    case "igual":
      return cellVal === (rule.value ?? "");
    case "contem":
      return cellVal.toLowerCase().includes(rule.valueLower);
    case "maior":
      return parseFloat(cellVal.replace(",", ".")) > rule.valueFloat;
    case "menor":
      return parseFloat(cellVal.replace(",", ".")) < rule.valueFloat;
    case "e_nulo":
      return cellVal === "" || cellVal === "null" || cellVal === "undefined";
    case "nao_e_nulo":
      return cellVal !== "" && cellVal !== "null" && cellVal !== "undefined";
    default:
      return false;
  }
}

export function DataTable({
  columns,
  appearanceKey,
  orderedColumns,
  columnWidths,
  onOrderedColumnsChange,
  onColumnWidthChange,
  rows,
  totalRows,
  loading,
  page,
  totalPages,
  onPageChange,
  highlightRows,
  autoHighlight,
  rowKey,
  selectedRowKeys,
  onRowSelect,
  onSelectAll,
  sortBy,
  sortDesc,
  onSortChange,
  hiddenColumns,
  columnFilters,
  onColumnFilterChange,
  showColumnFilters,
  highlightRules,
}: DataTableProps) {
  const storageKey = appearanceKey ? `datatable_appearance_${appearanceKey}` : undefined;
  const selectable = !!onRowSelect && !!rowKey;
  const shouldAutoHighlight = autoHighlight ?? highlightRows ?? false;
  const isServerSort = !!onSortChange;
  const podeReordenarColunas = !!onOrderedColumnsChange;
  const podeRedimensionarColunas = !!onColumnWidthChange;
  const shouldShowColumnFilters = showColumnFilters ?? true;

  const [localSort, setLocalSort] = useState<SortingState>([]);
  const [localColFilters, setLocalColFilters] = useState<
    Record<string, string>
  >({});
  const [appearanceOpen, setAppearanceOpen] = useState(false);
  const [appearance, setAppearance] = useState<TableAppearance>(() =>
    readAppearance(storageKey),
  );
  const colunaArrastadaRef = useRef<string | null>(null);
  const momentoUltimaInteracaoCabecalhoRef = useRef(0);

  useEffect(() => {
    setAppearance(readAppearance(storageKey));
  }, [storageKey]);

  useEffect(() => {
    if (!storageKey || typeof window === "undefined") return;
    window.localStorage.setItem(storageKey, JSON.stringify(appearance));
  }, [appearance, storageKey]);

  const effectiveColFilters =
    columnFilters !== undefined ? columnFilters : localColFilters;

  const handleColFilterChange = (col: string, val: string) => {
    if (onColumnFilterChange) {
      onColumnFilterChange(col, val);
    } else {
      setLocalColFilters((prev) => ({ ...prev, [col]: val }));
    }
  };

  const effectiveRows = useMemo(() => {
    if (onColumnFilterChange) return rows;
    const hasFilters = Object.values(effectiveColFilters).some((v) => v !== "");
    if (!hasFilters) return rows;

    // ⚡ Bolt Optimization: Pre-compute active filter lowercases and use for...of loop with early return instead of Object.entries().every() to prevent O(N*C) array allocations.
    const activeFilters = Object.entries(effectiveColFilters)
      .filter(([, val]) => val !== "")
      .map(([col, val]) => [col, val.toLowerCase()]);

    return rows.filter((row) => {
      for (const [col, valLower] of activeFilters) {
        if (
          !String(row[col] ?? "")
            .toLowerCase()
            .includes(valLower)
        ) {
          return false;
        }
      }
      return true;
    });
  }, [rows, effectiveColFilters, onColumnFilterChange]);

  const controlledSort: SortingState = useMemo(
    () => (sortBy ? [{ id: sortBy, desc: sortDesc ?? false }] : []),
    [sortBy, sortDesc],
  );

  const activeSorting: SortingState = isServerSort ? controlledSort : localSort;

  const ordemBaseColunas = useMemo(
    () => montarOrdemBaseColunas(columns, orderedColumns),
    [columns, orderedColumns],
  );

  const visibleCols = useMemo(() => {
    if (!hiddenColumns?.size) return ordemBaseColunas;
    return ordemBaseColunas.filter((coluna) => !hiddenColumns.has(coluna));
  }, [hiddenColumns, ordemBaseColunas]);

  const colDefs = useMemo<ColumnDef<Record<string, unknown>>[]>(
    () =>
      visibleCols.map((col) => ({
        id: col,
        accessorKey: col,
        header: col,
        cell: (info) => formatCell(info.getValue()),
        size: 120,
      })),
    [visibleCols],
  );

  const table = useReactTable({
    data: effectiveRows,
    columns: colDefs,
    state: { sorting: activeSorting },
    onSortingChange: isServerSort ? undefined : setLocalSort,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    manualSorting: isServerSort,
    manualPagination: true,
  });

  const handleHeaderClick = (colId: string) => {
    if (isServerSort) {
      const newDesc = sortBy === colId ? !(sortDesc ?? false) : false;
      onSortChange?.(colId, newDesc);
    } else {
      const current = localSort.find((s) => s.id === colId);
      if (!current) {
        setLocalSort([{ id: colId, desc: false }]);
      } else if (!current.desc) {
        setLocalSort([{ id: colId, desc: true }]);
      } else {
        setLocalSort([]);
      }
    }
  };

  const getSortIcon = (colId: string): string => {
    const s = activeSorting.find((x) => x.id === colId);
    if (!s) return "<>";
    return s.desc ? "v" : "^";
  };

  const compiledHighlightRules = useMemo(() => {
    return (highlightRules ?? []).map((r) => ({
      ...r,
      valueLower: (r.value ?? "").toLowerCase(),
      valueFloat: parseFloat((r.value ?? "").replace(",", ".")),
    }));
  }, [highlightRules]);

  const rowRules = useMemo(
    () => compiledHighlightRules.filter((r) => r.type === "row"),
    [compiledHighlightRules],
  );
  const colRules = useMemo(
    () => compiledHighlightRules.filter((r) => r.type === "column"),
    [compiledHighlightRules],
  );

  const getRowHighlightColor = (
    row: Record<string, unknown>,
  ): string | undefined => {
    for (const rule of rowRules) {
      if (matchesRule(rule, row)) return rule.color;
    }
    return undefined;
  };

  const getCellHighlightColor = (
    colId: string,
    row: Record<string, unknown>,
  ): string | undefined => {
    for (const rule of colRules) {
      if (rule.column === colId && (!rule.value || matchesRule(rule, row))) {
        return rule.color;
      }
    }
    return undefined;
  };

  function marcarInteracaoCabecalho() {
    momentoUltimaInteracaoCabecalhoRef.current = Date.now();
  }

  function cliqueCabecalhoDeveSerIgnorado(): boolean {
    return Date.now() - momentoUltimaInteracaoCabecalhoRef.current < 200;
  }

  function iniciarArrasteColuna(nomeColuna: string) {
    if (!podeReordenarColunas) return;
    colunaArrastadaRef.current = nomeColuna;
    marcarInteracaoCabecalho();
  }

  function finalizarArrasteColuna() {
    colunaArrastadaRef.current = null;
    marcarInteracaoCabecalho();
  }

  function soltarColuna(nomeColunaDestino: string) {
    if (!podeReordenarColunas) return;

    const colunaOrigem = colunaArrastadaRef.current;
    if (!colunaOrigem) return;

    onOrderedColumnsChange?.(
      moverColunaNaOrdem(ordemBaseColunas, colunaOrigem, nomeColunaDestino),
    );
    finalizarArrasteColuna();
  }

  function iniciarRedimensionamentoColuna(
    evento: EventoMouseReact<HTMLDivElement>,
    nomeColuna: string,
  ) {
    if (!podeRedimensionarColunas) return;

    evento.preventDefault();
    evento.stopPropagation();

    const posicaoInicial = evento.clientX;
    const larguraInicial = obterLarguraColuna(columnWidths, nomeColuna);
    marcarInteracaoCabecalho();

    const aoMoverMouse = (movimento: MouseEvent) => {
      const deslocamento = movimento.clientX - posicaoInicial;
      onColumnWidthChange?.(nomeColuna, larguraInicial + deslocamento);
    };

    const aoSoltarMouse = () => {
      window.removeEventListener("mousemove", aoMoverMouse);
      window.removeEventListener("mouseup", aoSoltarMouse);
      marcarInteracaoCabecalho();
    };

    window.addEventListener("mousemove", aoMoverMouse);
    window.addEventListener("mouseup", aoSoltarMouse);
  }

  const paginaAtual = page ?? 1;
  const totalPaginas = totalPages ?? 1;
  const paginaAnteriorHabilitada = !!onPageChange && paginaAtual > 1;
  const proximaPaginaHabilitada =
    !!onPageChange && totalPaginas > 1 && paginaAtual < totalPaginas;

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center justify-end gap-2 border-b border-slate-800/60 bg-slate-950/50 px-3 py-2">
        <button
          type="button"
          className="rounded-lg bg-slate-800/90 px-3 py-1.5 text-[11px] font-medium text-slate-200 transition-colors hover:bg-slate-700"
          onClick={() => setAppearanceOpen((prev) => !prev)}
        >
          Aparencia da tabela
        </button>
        {appearanceOpen && (
          <button
            type="button"
            className="rounded-lg bg-slate-900/80 px-3 py-1.5 text-[11px] font-medium text-slate-300 transition-colors hover:bg-slate-800"
            onClick={() => setAppearance(DEFAULT_APPEARANCE)}
          >
            Restaurar cores
          </button>
        )}
      </div>
      {appearanceOpen && (
        <div className="grid grid-cols-2 gap-2 border-b border-slate-800/60 bg-slate-950/70 px-3 py-3 md:grid-cols-3 xl:grid-cols-5">
          {[
            ["Cabecalho", "headerBg"],
            ["Texto cabecalho", "headerText"],
            ["Filtros", "filterBg"],
            ["Linha par", "rowEvenBg"],
            ["Linha impar", "rowOddBg"],
            ["Texto linhas", "rowText"],
            ["Entrada", "entradaBg"],
            ["Saida", "saidaBg"],
            ["Selecionada", "selectedBg"],
          ].map(([label, key]) => (
            <label
              key={key}
              className="flex items-center justify-between gap-2 rounded-xl bg-slate-900/80 px-3 py-2 text-[11px] text-slate-300"
            >
              <span>{label}</span>
              <input
                type="color"
                value={appearance[key as keyof TableAppearance]}
                onChange={(e) =>
                  setAppearance((prev) => ({
                    ...prev,
                    [key]: e.target.value,
                  }))
                }
                className="h-8 w-10 cursor-pointer rounded border border-slate-700 bg-transparent p-0"
              />
            </label>
          ))}
        </div>
      )}
      <div className="overflow-auto flex-1">
        {loading ? (
          <div className="flex items-center justify-center h-32 text-slate-400">
            Carregando...
          </div>
        ) : (
          <table
            className="w-full border-collapse text-xs"
            style={{
              tableLayout: "fixed",
              minWidth:
                visibleCols.reduce(
                  (total, coluna) =>
                    total + obterLarguraColuna(columnWidths, coluna),
                  0,
                ) + (selectable ? 36 : 0),
            }}
          >
            <colgroup>
              {selectable && <col style={{ width: 36 }} />}
              <col style={{ width: 40 }} />
              {visibleCols.map((coluna) => (
                <col
                  key={coluna}
                  style={{ width: obterLarguraColuna(columnWidths, coluna) }}
                />
              ))}
            </colgroup>
            <thead
              className="sticky top-0 z-10"
              style={{ background: appearance.headerBg }}
            >
              {table.getHeaderGroups().map((hg) => {
                const visibleKeys = selectable
                  ? table
                      .getRowModel()
                      .rows.map((r) => String(r.original[rowKey!] ?? ""))
                  : [];
                const allSelected =
                  selectable &&
                  visibleKeys.length > 0 &&
                  visibleKeys.every((k) => selectedRowKeys!.has(k));
                return (
                  <tr key={hg.id}>
                    {selectable && (
                      <th className="w-9 px-2 py-2 border-b border-slate-700 text-center">
                        <input
                          type="checkbox"
                          aria-label="Selecionar todas as linhas"
                          title="Selecionar todas as linhas"
                          checked={allSelected}
                          onChange={(e) =>
                            onSelectAll?.(e.target.checked, visibleKeys)
                          }
                          className="accent-blue-500 cursor-pointer"
                        />
                      </th>
                    )}
                    <th className="w-10 px-2 py-2 text-slate-400 text-right border-b border-slate-700">
                      #
                    </th>
                    {hg.headers.map((h) => (
                      <th
                        key={h.id}
                        className="group relative px-2 py-2 text-left font-semibold truncate select-none cursor-pointer transition-colors"
                        style={{
                          width: obterLarguraColuna(columnWidths, h.column.id),
                          maxWidth: obterLarguraColuna(
                            columnWidths,
                            h.column.id,
                          ),
                          color: appearance.headerText,
                          borderBottom: "1px solid rgba(71, 85, 105, 0.35)",
                        }}
                        title={`${h.column.id} - clique para ordenar`}
                        draggable={podeReordenarColunas}
                        onDragStart={() => iniciarArrasteColuna(h.column.id)}
                        onDragOver={(evento) => {
                          if (!podeReordenarColunas) return;
                          evento.preventDefault();
                        }}
                        onDrop={() => soltarColuna(h.column.id)}
                        onDragEnd={finalizarArrasteColuna}
                        onClick={() => {
                          if (cliqueCabecalhoDeveSerIgnorado()) return;
                          handleHeaderClick(h.column.id);
                        }}
                        onMouseEnter={(evento) => {
                          (evento.currentTarget.style.backgroundColor =
                            "rgba(51, 65, 85, 0.35)");
                        }}
                        onMouseLeave={(evento) => {
                          (evento.currentTarget.style.backgroundColor =
                            "transparent");
                        }}
                      >
                        <span className="flex items-center gap-1 pr-3">
                          <span className="truncate flex-1">
                            {flexRender(
                              h.column.columnDef.header,
                              h.getContext(),
                            )}
                          </span>
                          <span className="text-slate-500 group-hover:text-slate-300 shrink-0 text-[10px]">
                            {getSortIcon(h.column.id)}
                          </span>
                        </span>
                        {podeRedimensionarColunas && (
                          <div
                            className="absolute top-0 right-0 h-full w-2 cursor-col-resize"
                            onMouseDown={(evento) =>
                              iniciarRedimensionamentoColuna(
                                evento,
                                h.column.id,
                              )
                            }
                            title={`Redimensionar ${h.column.id}`}
                          />
                        )}
                      </th>
                    ))}
                  </tr>
                );
              })}
              {shouldShowColumnFilters && (
                <tr style={{ background: appearance.filterBg }}>
                  {selectable && <th className="w-9" />}
                  <th className="w-10" />
                  {visibleCols.map((col) => (
                    <th
                      key={col}
                      className="px-1 py-1 border-b border-slate-700"
                    >
                      <input
                        aria-label={`Filtrar coluna ${col}`}
                        className="w-full rounded-lg border border-slate-700/70 bg-slate-950/80 px-2 py-1 text-xs text-slate-100 shadow-inner focus:border-blue-500 focus:outline-none placeholder-slate-500"
                        placeholder={`Filtrar ${col}`}
                        value={effectiveColFilters[col] ?? ""}
                        onChange={(e) =>
                          handleColFilterChange(col, e.target.value)
                        }
                      />
                    </th>
                  ))}
                </tr>
              )}
            </thead>
            <tbody>
              {table.getRowModel().rows.map((row, idx) => {
                const tipoOp = row.original["Tipo_operacao"] as
                  | string
                  | undefined;
                const isEntrada = tipoOp?.includes("ENTRADA");
                const isSaida =
                  tipoOp?.includes("SAIDA") || tipoOp?.includes("SAIDAS");
                const rowKeyVal = rowKey
                  ? String(row.original[rowKey] ?? "")
                  : "";
                const isSelected =
                  selectable && selectedRowKeys!.has(rowKeyVal);
                const ruleColor = getRowHighlightColor(row.original);
                const bg = isSelected
                  ? appearance.selectedBg
                  : ruleColor
                    ? ruleColor
                    : shouldAutoHighlight
                      ? isEntrada
                        ? appearance.entradaBg
                        : isSaida
                          ? appearance.saidaBg
                          : idx % 2 === 0
                            ? appearance.rowEvenBg
                            : appearance.rowOddBg
                      : idx % 2 === 0
                        ? appearance.rowEvenBg
                        : appearance.rowOddBg;
                return (
                  <tr
                    key={row.id}
                    style={{
                      background: bg,
                      outline: isSelected
                        ? "1px solid rgba(59,130,246,0.5)"
                        : undefined,
                      cursor: selectable ? "pointer" : undefined,
                    }}
                    className="hover:brightness-125 transition-all"
                    onClick={
                      selectable
                        ? () => onRowSelect?.(rowKeyVal, !isSelected)
                        : undefined
                    }
                  >
                    {selectable && (
                      <td className="px-2 py-1.5 text-center text-slate-100">
                        <input
                          type="checkbox"
                          aria-label={`Selecionar linha ${rowKeyVal}`}
                          title={`Selecionar linha ${rowKeyVal}`}
                          checked={isSelected}
                          onChange={(e) => {
                            e.stopPropagation();
                            onRowSelect?.(rowKeyVal, e.target.checked);
                          }}
                          onClick={(e) => e.stopPropagation()}
                          className="accent-blue-500 cursor-pointer"
                        />
                      </td>
                    )}
                    <td
                      className="px-2 py-1.5 text-right"
                      style={{ color: appearance.rowText }}
                    >
                      {idx + 1}
                    </td>
                    {row.getVisibleCells().map((cell) => {
                      const cellColor = getCellHighlightColor(
                        cell.column.id,
                        row.original,
                      );
                      return (
                        <td
                          key={cell.id}
                          className="px-2 py-1.5 truncate"
                          style={{
                            width: obterLarguraColuna(
                              columnWidths,
                              cell.column.id,
                            ),
                            maxWidth: obterLarguraColuna(
                              columnWidths,
                              cell.column.id,
                            ),
                            background: cellColor ?? undefined,
                            color: appearance.rowText,
                          }}
                          title={formatCell(cell.getValue())}
                        >
                          {flexRender(
                            cell.column.columnDef.cell,
                            cell.getContext(),
                          )}
                        </td>
                      );
                    })}
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      <div className="flex flex-wrap items-center justify-between gap-3 border-t border-slate-800/60 bg-slate-950/70 px-3 py-2 text-xs text-slate-400">
        <div>
          Linhas filtradas: {intlInteger.format(totalRows ?? rows.length)}
        </div>
        {!!onPageChange && totalPaginas > 1 && (
          <div className="flex items-center gap-2">
            <button
              type="button"
              className="rounded-md bg-slate-800 px-2.5 py-1 text-slate-200 transition-colors hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
              onClick={() => onPageChange(Math.max(1, paginaAtual - 1))}
              disabled={!paginaAnteriorHabilitada}
            >
              Anterior
            </button>
            <span className="min-w-28 text-center text-slate-300">
              Pagina {intlInteger.format(paginaAtual)} de{" "}
              {intlInteger.format(totalPaginas)}
            </span>
            <button
              type="button"
              className="rounded-md bg-slate-800 px-2.5 py-1 text-slate-200 transition-colors hover:bg-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
              onClick={() =>
                onPageChange(Math.min(totalPaginas, paginaAtual + 1))
              }
              disabled={!proximaPaginaHabilitada}
            >
              Proxima
            </button>
          </div>
        )}
      </div>
    </div>
  );
}
