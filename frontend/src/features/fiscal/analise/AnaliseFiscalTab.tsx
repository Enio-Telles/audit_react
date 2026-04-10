import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import type { PageResult } from "../../../api/types";
import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";
import { FiscalPageShell } from "../shared/FiscalPageShell";
import { FiscalRowDetailPanel } from "../shared/FiscalRowDetailPanel";

type AnaliseDatasetKey =
  | "estoque-mov"
  | "estoque-mensal"
  | "estoque-anual"
  | "agregacao"
  | "conversao"
  | "produtos-base";

const DATASET_OPTIONS: Array<{ id: AnaliseDatasetKey; label: string; description: string }> = [
  {
    id: "estoque-mov",
    label: "Estoque · Movimentação",
    description: "Camada cronológica legada que vira a primeira visão operacional de cruzamentos.",
  },
  {
    id: "estoque-mensal",
    label: "Estoque · Mensal",
    description: "Série mensal legada reaproveitada dentro da nova Análise Fiscal.",
  },
  {
    id: "estoque-anual",
    label: "Estoque · Anual",
    description: "Série anual legada usada para auditoria agregada e cruzamentos.",
  },
  {
    id: "agregacao",
    label: "Verificações · Agregação",
    description: "Tabela agrupada legada, agora tratada como verificação estrutural.",
  },
  {
    id: "conversao",
    label: "Verificações · Conversão",
    description: "Fatores de conversão legados, agora integrados ao domínio de verificações.",
  },
  {
    id: "produtos-base",
    label: "Classificação · Produtos base",
    description: "Base de produtos reaproveitada como ponte para a futura classificação dos produtos.",
  },
];

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.map((item) => formatCell(item)).join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function AnaliseTable({
  data,
  isLoading,
  selectedRow,
  onSelectRow,
}: {
  data?: PageResult;
  isLoading: boolean;
  selectedRow?: Record<string, unknown> | null;
  onSelectRow: (row: Record<string, unknown>) => void;
}) {
  const columns = data?.columns ?? [];
  const rows = data?.rows ?? [];

  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div>
          <div className="text-sm font-semibold text-white">Tabela operacional</div>
          <div className="text-xs text-slate-500">
            Leitura real dos artefatos legados que agora compõem cruzamentos, verificações e classificação.
          </div>
        </div>
        <div className="text-xs text-slate-400">
          {isLoading
            ? "Carregando..."
            : `${data?.total_rows ?? 0} linha(s) · página ${data?.page ?? 1} de ${data?.total_pages ?? 1}`}
        </div>
      </div>

      <div className="overflow-auto rounded-xl border border-slate-800">
        <table className="min-w-full border-collapse text-xs">
          <thead className="sticky top-0 z-10 bg-slate-950/95">
            <tr>
              {columns.map((column) => (
                <th
                  key={column}
                  className="border-b border-slate-800 px-3 py-2 text-left font-medium text-slate-300"
                >
                  {column}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={Math.max(columns.length, 1)} className="px-3 py-8 text-center text-slate-500">
                  {isLoading ? "Carregando linhas..." : "Nenhum parquet encontrado para esta visão analítica."}
                </td>
              </tr>
            ) : (
              rows.map((row, rowIndex) => {
                const active = row === selectedRow;
                return (
                  <tr
                    key={rowIndex}
                    onClick={() => onSelectRow(row)}
                    className={`cursor-pointer ${active ? "bg-blue-950/30" : rowIndex % 2 === 0 ? "bg-slate-900/20" : "bg-slate-950/30"}`}
                  >
                    {columns.map((column) => (
                      <td
                        key={`${rowIndex}-${column}`}
                        className="max-w-[280px] truncate border-b border-slate-800 px-3 py-2 text-slate-300"
                        title={formatCell(row[column])}
                      >
                        {formatCell(row[column])}
                      </td>
                    ))}
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function AnaliseFiscalTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);
  const setActiveTab = useAppStore((state) => state.setActiveTab);
  const [activeDataset, setActiveDataset] = useState<AnaliseDatasetKey>("estoque-mov");
  const [page, setPage] = useState(1);
  const [filterText, setFilterText] = useState("");
  const [sortBy, setSortBy] = useState("");
  const [sortDesc, setSortDesc] = useState(false);
  const [selectedRow, setSelectedRow] = useState<Record<string, unknown> | null>(null);
  const pageSize = 50;

  const summaryQuery = useQuery({
    queryKey: ["fiscal", "analise", "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getAnaliseResumo(selectedCnpj),
  });

  const tableQuery = useQuery({
    queryKey: [
      "fiscal",
      "analise",
      activeDataset,
      selectedCnpj ?? "sem-cnpj",
      page,
      filterText,
      sortBy,
      sortDesc,
    ],
    queryFn: () => {
      if (!selectedCnpj) {
        throw new Error("Selecione um CNPJ para carregar a análise fiscal.");
      }
      const options = {
        page,
        pageSize,
        sortBy: sortBy || undefined,
        sortDesc,
        filterText: filterText.trim() || undefined,
      };
      switch (activeDataset) {
        case "estoque-mov":
          return fiscalFeatureApi.getAnaliseEstoqueMov(selectedCnpj, options);
        case "estoque-mensal":
          return fiscalFeatureApi.getAnaliseEstoqueMensal(selectedCnpj, options);
        case "estoque-anual":
          return fiscalFeatureApi.getAnaliseEstoqueAnual(selectedCnpj, options);
        case "agregacao":
          return fiscalFeatureApi.getAnaliseAgregacao(selectedCnpj, options);
        case "conversao":
          return fiscalFeatureApi.getAnaliseConversao(selectedCnpj, options);
        case "produtos-base":
          return fiscalFeatureApi.getAnaliseProdutosBase(selectedCnpj, options);
      }
    },
    enabled: Boolean(selectedCnpj),
  });

  useEffect(() => {
    setPage(1);
    setSortBy("");
    setSortDesc(false);
    setSelectedRow(null);
  }, [activeDataset]);

  useEffect(() => {
    setSelectedRow(null);
  }, [page, filterText, sortBy, sortDesc, selectedCnpj]);

  const totalPages = tableQuery.data?.total_pages ?? 1;
  const sortColumns = useMemo(() => tableQuery.data?.all_columns ?? [], [tableQuery.data]);

  return (
    <FiscalPageShell
      title="Análise Fiscal"
      subtitle="Cruzamentos, verificações e classificação dos produtos."
    >
      <div className="space-y-4">
        <FiscalDomainOverview
          data={summaryQuery.data}
          isLoading={summaryQuery.isLoading}
          errorMessage={summaryQuery.error instanceof Error ? summaryQuery.error.message : undefined}
          onOpenShortcut={setActiveTab}
        />

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-3 text-sm font-semibold text-white">Visões analíticas já disponíveis</div>
          <div className="flex flex-wrap gap-2">
            {DATASET_OPTIONS.map((option) => {
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
          <div className="mb-3 text-sm font-semibold text-white">Filtro e ordenação</div>
          <div className="grid gap-3 md:grid-cols-[1.4fr_1fr_auto]">
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
              value={sortBy}
              onChange={(event) => {
                setSortBy(event.target.value);
                setPage(1);
              }}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            >
              <option value="">Sem ordenação</option>
              {sortColumns.map((column) => (
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
          </div>
        </section>

        <AnaliseTable
          data={tableQuery.data}
          isLoading={tableQuery.isLoading}
          selectedRow={selectedRow}
          onSelectRow={setSelectedRow}
        />

        <FiscalRowDetailPanel
          row={selectedRow}
          title="Detalhe do registro analítico"
          subtitle="Clique em uma linha das visões de estoque, agregação, conversão ou produtos-base para inspecionar todos os campos."
          emptyMessage="Selecione uma linha da tabela analítica para ver o registro completo."
        />

        <div className="flex items-center justify-between rounded-2xl border border-slate-700 bg-slate-900/30 p-4 text-xs text-slate-400">
          <div>
            Fonte atual: <span className="font-mono text-slate-300">{activeDataset}</span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => {
                setPage((current) => Math.max(1, current - 1));
                setSelectedRow(null);
              }}
              disabled={page <= 1 || !selectedCnpj || tableQuery.isLoading}
              className="rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 text-slate-200 disabled:cursor-not-allowed disabled:opacity-40"
            >
              ← Anterior
            </button>
            <span>
              Página {page} de {totalPages}
            </span>
            <button
              onClick={() => {
                setPage((current) => Math.min(totalPages, current + 1));
                setSelectedRow(null);
              }}
              disabled={page >= totalPages || !selectedCnpj || tableQuery.isLoading}
              className="rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 text-slate-200 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Próxima →
            </button>
          </div>
        </div>
      </div>
    </FiscalPageShell>
  );
}

export default AnaliseFiscalTab;
