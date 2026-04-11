import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import type { PageResult } from "../../../api/types";
import { ColumnToggle } from "../../../components/table/ColumnToggle";
import { DataTable } from "../../../components/table/DataTable";
import { useAppStore } from "../../../store/appStore";
import { abrirFiscalEmNovaAba, lerBootstrapFiscalDaUrl } from "../navigation";
import type { FiscalDomainSummary } from "../types";
import { FiscalDomainOverview } from "./FiscalDomainOverview";
import { FiscalPageShell } from "./FiscalPageShell";
import { FiscalRowDetailPanel } from "./FiscalRowDetailPanel";
import { ProvenanceBadge } from "./ProvenanceBadge";

interface DatasetOption {
  id: string;
  label: string;
  description: string;
}

interface PageQueryOptions {
  page?: number;
  pageSize?: number;
  sortBy?: string;
  sortDesc?: boolean;
  filterText?: string;
  filterColumn?: string;
  filterValue?: string;
}

interface FiscalDatasetExplorerProps {
  tabId: string;
  domainKey: string;
  title: string;
  subtitle: string;
  detailTitle: string;
  detailSubtitle: string;
  emptyMessage: string;
  datasetOptions: DatasetOption[];
  loadSummary: (cnpj?: string | null) => Promise<FiscalDomainSummary>;
  loadPage: (
    datasetId: string,
    cnpj: string,
    options: PageQueryOptions,
  ) => Promise<PageResult>;
}

const ROW_ID_COLUMN = "__fiscal_dataset_row_id";

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (Array.isArray(value)) return value.map((item) => formatCell(item)).join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function escapeCsvCell(value: unknown): string {
  const normalized = formatCell(value).replace(/\r?\n/g, " ");
  if (/[;"\n,]/.test(normalized)) {
    return `"${normalized.replace(/"/g, '""')}"`;
  }
  return normalized;
}

function exportCurrentPageCsv(
  filename: string,
  columns: string[],
  rows: Record<string, unknown>[],
) {
  if (typeof window === "undefined" || columns.length === 0) return;

  const header = columns.map((column) => escapeCsvCell(column)).join(";");
  const body = rows
    .map((row) => columns.map((column) => escapeCsvCell(row[column])).join(";"))
    .join("\n");
  const csv = `${header}\n${body}`;
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  link.click();
  window.URL.revokeObjectURL(url);
}

function montarColunasVisiveis(
  columns: string[],
  orderedColumns: string[],
  hiddenColumns: Set<string>,
): string[] {
  const ordered =
    orderedColumns.length > 0
      ? [
          ...orderedColumns.filter((column) => columns.includes(column)),
          ...columns.filter((column) => !orderedColumns.includes(column)),
        ]
      : columns;

  return ordered.filter((column) => !hiddenColumns.has(column));
}

export function FiscalDatasetExplorer({
  tabId,
  domainKey,
  title,
  subtitle,
  detailTitle,
  detailSubtitle,
  emptyMessage,
  datasetOptions,
  loadSummary,
  loadPage,
}: FiscalDatasetExplorerProps) {
  const bootstrap = useMemo(() => lerBootstrapFiscalDaUrl(), []);
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);
  const setActiveTab = useAppStore((state) => state.setActiveTab);
  const [activeDataset, setActiveDataset] = useState(
    () =>
      (bootstrap?.abaAtiva === tabId ? bootstrap.dataset : null) ??
      datasetOptions[0]?.id ??
      "",
  );
  const [page, setPage] = useState(1);
  const [filterText, setFilterText] = useState("");
  const [filterColumn, setFilterColumn] = useState("");
  const [filterValue, setFilterValue] = useState("");
  const [sortBy, setSortBy] = useState("");
  const [sortDesc, setSortDesc] = useState(false);
  const [selectedRowKey, setSelectedRowKey] = useState<string | null>(null);
  const [hiddenColumns, setHiddenColumns] = useState<Set<string>>(() => new Set());
  const [orderedColumns, setOrderedColumns] = useState<string[]>([]);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const pageSize = 50;

  const summaryQuery = useQuery({
    queryKey: ["fiscal", domainKey, "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => loadSummary(selectedCnpj),
  });

  const tableQuery = useQuery({
    queryKey: [
      "fiscal",
      domainKey,
      activeDataset,
      selectedCnpj ?? "sem-cnpj",
      page,
      filterText,
      filterColumn,
      filterValue,
      sortBy,
      sortDesc,
    ],
    queryFn: () => {
      if (!selectedCnpj) {
        throw new Error("Selecione um CNPJ para carregar este dominio.");
      }
      return loadPage(activeDataset, selectedCnpj, {
        page,
        pageSize,
        sortBy: sortBy || undefined,
        sortDesc,
        filterText: filterText.trim() || undefined,
        filterColumn: filterColumn || undefined,
        filterValue: filterValue.trim() || undefined,
      });
    },
    enabled: Boolean(selectedCnpj && activeDataset),
  });

  useEffect(() => {
    setPage(1);
    setSortBy("");
    setSortDesc(false);
    setFilterColumn("");
    setFilterValue("");
    setSelectedRowKey(null);
    setHiddenColumns(new Set());
    setOrderedColumns([]);
    setColumnWidths({});
  }, [activeDataset]);

  useEffect(() => {
    setSelectedRowKey(null);
  }, [page, filterText, filterColumn, filterValue, sortBy, sortDesc, selectedCnpj]);

  const tableColumns = useMemo(
    () => tableQuery.data?.all_columns ?? tableQuery.data?.columns ?? [],
    [tableQuery.data],
  );

  const rowsWithIds = useMemo(
    () =>
      (tableQuery.data?.rows ?? []).map((row, index) => ({
        ...row,
        [ROW_ID_COLUMN]: `${page}-${index}`,
      })),
    [page, tableQuery.data?.rows],
  );

  const selectedRow = useMemo(
    () =>
      rowsWithIds.find(
        (row) => String(row[ROW_ID_COLUMN] ?? "") === (selectedRowKey ?? ""),
      ) ?? null,
    [rowsWithIds, selectedRowKey],
  );

  const selectedRowDetail = useMemo(() => {
    if (!selectedRow) return null;
    const { [ROW_ID_COLUMN]: _ignored, ...rest } = selectedRow;
    return rest;
  }, [selectedRow]);

  const selectedRowKeys = useMemo(
    () => (selectedRowKey ? new Set([selectedRowKey]) : new Set<string>()),
    [selectedRowKey],
  );

  const totalPages = tableQuery.data?.total_pages ?? 1;

  const visibleColumns = useMemo(
    () => montarColunasVisiveis(tableColumns, orderedColumns, hiddenColumns),
    [hiddenColumns, orderedColumns, tableColumns],
  );

  const exportFileName = useMemo(() => {
    const cnpj = selectedCnpj ?? "sem_cnpj";
    return `fiscal_${domainKey}_${activeDataset}_${cnpj}_pagina_${page}.csv`;
  }, [activeDataset, domainKey, page, selectedCnpj]);

  return (
    <FiscalPageShell title={title} subtitle={subtitle}>
      <div className="space-y-4">
        <FiscalDomainOverview
          data={summaryQuery.data}
          isLoading={summaryQuery.isLoading}
          errorMessage={summaryQuery.error instanceof Error ? summaryQuery.error.message : undefined}
          onOpenShortcut={setActiveTab}
        />

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-3 text-sm font-semibold text-white">Visoes operacionais disponiveis</div>
          <div className="flex flex-wrap gap-2">
            {datasetOptions.map((option) => {
              const active = option.id === activeDataset;
              return (
                <button
                  key={option.id}
                  onClick={() => {
                    setActiveDataset(option.id);
                    setPage(1);
                  }}
                  className={`rounded-xl border px-3 py-2 text-left text-xs transition-colors ${
                    active
                      ? "border-blue-500 bg-blue-950/40 text-blue-100"
                      : "border-slate-700 bg-slate-950/40 text-slate-300 hover:bg-slate-900/50"
                  }`}
                >
                  <div className="font-medium">{option.label}</div>
                  <div className="mt-1 max-w-xs text-[11px] text-slate-400">{option.description}</div>
                </button>
              );
            })}
          </div>
        </section>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="text-sm font-semibold text-white">Controles da grade</div>
              <div className="text-xs text-slate-500">
                Interface enxuta com foco na tabela analitica e nas operacoes de exploracao.
              </div>
            </div>
            <div className="flex flex-wrap gap-2">
              <ColumnToggle
                allColumns={tableColumns}
                orderedColumns={orderedColumns}
                hiddenColumns={hiddenColumns}
                columnWidths={columnWidths}
                onChange={(column, visible) => {
                  setHiddenColumns((current) => {
                    const next = new Set(current);
                    if (visible) next.delete(column);
                    else next.add(column);
                    return next;
                  });
                }}
                onOrderChange={setOrderedColumns}
                onWidthChange={(column, width) => {
                  setColumnWidths((current) => ({ ...current, [column]: width }));
                }}
                onReset={() => {
                  setHiddenColumns(new Set());
                  setOrderedColumns([]);
                  setColumnWidths({});
                }}
              />
              <button
                onClick={() =>
                  abrirFiscalEmNovaAba({
                    tab: tabId,
                    cnpj: selectedCnpj,
                    dataset: activeDataset,
                  })
                }
                disabled={!selectedCnpj}
                className="rounded-lg border border-slate-700 bg-slate-950/40 px-3 py-2 text-xs text-slate-200 hover:bg-slate-900/50 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Abrir em nova aba
              </button>
              <button
                onClick={() => {
                  setFilterText("");
                  setFilterColumn("");
                  setFilterValue("");
                  setSortBy("");
                  setSortDesc(false);
                  setPage(1);
                }}
                className="rounded-lg border border-slate-700 bg-slate-950/40 px-3 py-2 text-xs text-slate-200 hover:bg-slate-900/50"
              >
                Limpar consulta
              </button>
              <button
                onClick={() =>
                  exportCurrentPageCsv(exportFileName, visibleColumns, rowsWithIds)
                }
                disabled={rowsWithIds.length === 0}
                className="rounded-lg border border-slate-700 bg-slate-950/40 px-3 py-2 text-xs text-slate-200 hover:bg-slate-900/50 disabled:cursor-not-allowed disabled:opacity-40"
              >
                Exportar pagina CSV
              </button>
            </div>
          </div>

          <div className="mt-4 grid gap-3 lg:grid-cols-2 xl:grid-cols-[1.1fr_0.9fr_1.1fr_0.9fr_auto_auto]">
            <input
              value={filterText}
              onChange={(event) => {
                setFilterText(event.target.value);
                setPage(1);
              }}
              placeholder="Buscar texto em qualquer coluna"
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            />
            <select
              value={filterColumn}
              onChange={(event) => {
                setFilterColumn(event.target.value);
                setPage(1);
              }}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            >
              <option value="">Sem filtro por coluna</option>
              {tableColumns.map((column) => (
                <option key={column} value={column}>
                  {column}
                </option>
              ))}
            </select>
            <input
              value={filterValue}
              onChange={(event) => {
                setFilterValue(event.target.value);
                setPage(1);
              }}
              placeholder="Valor para a coluna selecionada"
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            />
            <select
              value={sortBy}
              onChange={(event) => {
                setSortBy(event.target.value);
                setPage(1);
              }}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            >
              <option value="">Sem ordenacao</option>
              {tableColumns.map((column) => (
                <option key={column} value={column}>
                  {column}
                </option>
              ))}
            </select>
            <button
              onClick={() => {
                setSortDesc((current) => !current);
                setPage(1);
              }}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 hover:bg-slate-900/50"
            >
              {sortDesc ? "Desc" : "Asc"}
            </button>
            <div className="rounded-xl border border-slate-800 bg-slate-950/30 px-3 py-2 text-xs text-slate-400">
              {tableQuery.isLoading
                ? "Carregando..."
                : `${tableQuery.data?.total_rows ?? 0} linha(s) · ${visibleColumns.length}/${tableColumns.length} colunas visiveis`}
            </div>
          </div>
        </section>

        <section className="overflow-hidden rounded-2xl border border-slate-700 bg-slate-900/30">
          <div className="border-b border-slate-800 px-4 py-3">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <div>
                <div className="text-sm font-semibold text-white">Tabela operacional</div>
                <div className="text-xs text-slate-500">
                  Leitura real dos datasets materializados ja disponiveis para este dominio.
                </div>
              </div>
              <ProvenanceBadge
                datasetId={tableQuery.data?._provenance?.dataset_id ?? activeDataset}
                camada={tableQuery.data?._provenance?.camada ?? "legado"}
                sourcePath={tableQuery.data?._provenance?.source_path}
              />
            </div>
          </div>

          <DataTable
            columns={tableColumns}
            rows={rowsWithIds}
            totalRows={tableQuery.data?.total_rows ?? 0}
            loading={tableQuery.isLoading}
            page={page}
            totalPages={totalPages}
            onPageChange={setPage}
            rowKey={ROW_ID_COLUMN}
            selectedRowKeys={selectedRowKeys}
            onRowSelect={(key, checked) => setSelectedRowKey(checked ? key : null)}
            showSelectionCheckboxes={false}
            sortBy={sortBy || undefined}
            sortDesc={sortDesc}
            onSortChange={(column, desc) => {
              setSortBy(column);
              setSortDesc(desc);
              setPage(1);
            }}
            hiddenColumns={hiddenColumns}
            orderedColumns={orderedColumns}
            columnWidths={columnWidths}
            onOrderedColumnsChange={setOrderedColumns}
            onColumnWidthChange={(column, width) => {
              setColumnWidths((current) => ({ ...current, [column]: width }));
            }}
            showColumnFilters
            appearanceKey={`fiscal_${domainKey}_${activeDataset}`}
          />
        </section>

        <FiscalRowDetailPanel
          row={selectedRowDetail}
          title={detailTitle}
          subtitle={detailSubtitle}
          emptyMessage={emptyMessage}
        />
      </div>
    </FiscalPageShell>
  );
}

export default FiscalDatasetExplorer;
