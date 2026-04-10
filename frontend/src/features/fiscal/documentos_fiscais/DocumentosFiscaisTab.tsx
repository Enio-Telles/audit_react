import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import type { PageResult } from "../../../api/types";
import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import { FiscalPageShell } from "../shared/FiscalPageShell";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";

type DocumentoDatasetKey = "nfe" | "nfce" | "cte" | "info-complementar" | "contatos";

const DATASET_OPTIONS: Array<{ id: DocumentoDatasetKey; label: string; description: string }> = [
  {
    id: "nfe",
    label: "NF-e",
    description: "Espelho principal de documentos fiscais eletrônicos por contribuinte.",
  },
  {
    id: "nfce",
    label: "NFC-e",
    description: "Camada complementar de documentos ao consumidor, quando já materializada no CNPJ.",
  },
  {
    id: "cte",
    label: "CT-e",
    description: "Conhecimentos de transporte reaproveitados pela nova área documental.",
  },
  {
    id: "info-complementar",
    label: "Informações complementares",
    description: "Conteúdo textual associado aos documentos, útil para auditoria e leitura humana.",
  },
  {
    id: "contatos",
    label: "Contatos extraídos",
    description: "Emails, telefones e sinais auxiliares derivados dos documentos fiscais.",
  },
];

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.map((item) => formatCell(item)).join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function DocumentosTable({ data, isLoading }: { data?: PageResult; isLoading: boolean }) {
  const columns = data?.columns ?? [];
  const rows = data?.rows ?? [];

  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div>
          <div className="text-sm font-semibold text-white">Tabela operacional</div>
          <div className="text-xs text-slate-500">
            Leitura real dos artefatos documentais já materializados no projeto.
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
                  {isLoading ? "Carregando linhas..." : "Nenhum parquet encontrado para esta visão documental."}
                </td>
              </tr>
            ) : (
              rows.map((row, rowIndex) => (
                <tr key={rowIndex} className="odd:bg-slate-950/30 even:bg-slate-900/20">
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
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

export function DocumentosFiscaisTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);
  const [activeDataset, setActiveDataset] = useState<DocumentoDatasetKey>("nfe");
  const [page, setPage] = useState(1);
  const [filterText, setFilterText] = useState("");
  const [sortBy, setSortBy] = useState("");
  const [sortDesc, setSortDesc] = useState(false);
  const pageSize = 50;

  const summaryQuery = useQuery({
    queryKey: ["fiscal", "documentos-fiscais", "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getDocumentosResumo(selectedCnpj),
  });

  const tableQuery = useQuery({
    queryKey: [
      "fiscal",
      "documentos-fiscais",
      activeDataset,
      selectedCnpj ?? "sem-cnpj",
      page,
      filterText,
      sortBy,
      sortDesc,
    ],
    queryFn: () => {
      if (!selectedCnpj) {
        throw new Error("Selecione um CNPJ para carregar os documentos fiscais.");
      }
      const options = {
        page,
        pageSize,
        sortBy: sortBy || undefined,
        sortDesc,
        filterText: filterText.trim() || undefined,
      };
      switch (activeDataset) {
        case "nfe":
          return fiscalFeatureApi.getDocumentosNfe(selectedCnpj, options);
        case "nfce":
          return fiscalFeatureApi.getDocumentosNfce(selectedCnpj, options);
        case "cte":
          return fiscalFeatureApi.getDocumentosCte(selectedCnpj, options);
        case "info-complementar":
          return fiscalFeatureApi.getDocumentosInfoComplementar(selectedCnpj, options);
        case "contatos":
          return fiscalFeatureApi.getDocumentosContatos(selectedCnpj, options);
      }
    },
    enabled: Boolean(selectedCnpj),
  });

  useEffect(() => {
    setPage(1);
    setSortBy("");
    setSortDesc(false);
  }, [activeDataset]);

  const totalPages = tableQuery.data?.total_pages ?? 1;
  const sortColumns = useMemo(() => tableQuery.data?.all_columns ?? [], [tableQuery.data]);

  return (
    <FiscalPageShell
      title="Documentos Fiscais"
      subtitle="NF-e, NFC-e, CT-e, informações complementares e contatos."
    >
      <div className="space-y-4">
        <FiscalDomainOverview
          data={summaryQuery.data}
          isLoading={summaryQuery.isLoading}
          errorMessage={summaryQuery.error instanceof Error ? summaryQuery.error.message : undefined}
        />

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-3 text-sm font-semibold text-white">Visões documentais já disponíveis</div>
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

        <DocumentosTable data={tableQuery.data} isLoading={tableQuery.isLoading} />

        <div className="flex items-center justify-between rounded-2xl border border-slate-700 bg-slate-900/30 p-4 text-xs text-slate-400">
          <div>
            Fonte atual: <span className="font-mono text-slate-300">{activeDataset}</span>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage((current) => Math.max(1, current - 1))}
              disabled={page <= 1 || !selectedCnpj || tableQuery.isLoading}
              className="rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 text-slate-200 disabled:cursor-not-allowed disabled:opacity-40"
            >
              ← Anterior
            </button>
            <span>
              Página {page} de {totalPages}
            </span>
            <button
              onClick={() => setPage((current) => Math.min(totalPages, current + 1))}
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

export default DocumentosFiscaisTab;
