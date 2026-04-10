import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import type { PageResult } from "../../../api/types";
import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import type { FiscalizacaoCadastroRecord, FiscalizacaoDsfRecord } from "../types";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";
import { FiscalPageShell } from "../shared/FiscalPageShell";
import { FiscalRowDetailPanel } from "../shared/FiscalRowDetailPanel";

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (Array.isArray(value)) return value.map((item) => formatCell(item)).join(", ");
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function KeyValuePanel({ data, isLoading }: { data?: FiscalizacaoCadastroRecord; isLoading: boolean }) {
  const entries = Object.entries(data ?? {}).filter(([, value]) => value !== null && value !== "");

  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 text-sm font-semibold text-white">Cadastro fiscalizatório</div>
      <div className="text-xs text-slate-500">Cache cadastral do Fisconforme reaproveitado na nova área fiscal.</div>
      <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
        {isLoading ? (
          <div className="text-sm text-slate-400">Carregando cadastro...</div>
        ) : entries.length === 0 ? (
          <div className="text-sm text-slate-500">Nenhum cadastro encontrado para o CNPJ selecionado.</div>
        ) : (
          entries.map(([key, value]) => (
            <div key={key} className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
              <div className="text-[11px] uppercase tracking-wide text-slate-500">{key}</div>
              <div className="mt-2 text-sm text-slate-200">{formatCell(value)}</div>
            </div>
          ))
        )}
      </div>
    </section>
  );
}

function MalhasTable({
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
          <div className="text-sm font-semibold text-white">Malhas e pendências</div>
          <div className="text-xs text-slate-500">Leitura direta do parquet de malhas do Fisconforme.</div>
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
                  {isLoading ? "Carregando linhas..." : "Nenhuma malha encontrada para este CNPJ."}
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

function DsfPanel({ items, isLoading }: { items?: FiscalizacaoDsfRecord[]; isLoading: boolean }) {
  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 text-sm font-semibold text-white">DSFs relacionadas</div>
      <div className="text-xs text-slate-500">Itens do acervo persistido que já referenciam o CNPJ selecionado.</div>
      <div className="mt-4 space-y-3">
        {isLoading ? (
          <div className="text-sm text-slate-400">Carregando DSFs...</div>
        ) : !items || items.length === 0 ? (
          <div className="text-sm text-slate-500">Nenhuma DSF relacionada encontrada.</div>
        ) : (
          items.map((item) => (
            <article key={item.id} className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
              <div className="flex flex-wrap items-center gap-2">
                <div className="text-sm font-medium text-white">{item.dsf || "DSF sem título"}</div>
                <span className="rounded-full border border-slate-600 bg-slate-800 px-2 py-0.5 text-[11px] text-slate-300">
                  {item.updated_at || item.created_at || "sem data"}
                </span>
              </div>
              <div className="mt-2 text-xs text-slate-400">Referência: {item.referencia || "—"}</div>
              <div className="mt-1 text-xs text-slate-400">Auditor: {item.auditor || "—"}</div>
              <div className="mt-1 text-xs text-slate-400">Órgão: {item.orgao_origem || "—"}</div>
              <div className="mt-1 text-xs text-slate-400">PDF: {item.pdf_file_name || "não informado"}</div>
            </article>
          ))
        )}
      </div>
    </section>
  );
}

export function FiscalizacaoTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);
  const [page, setPage] = useState(1);
  const [filterText, setFilterText] = useState("");
  const [sortBy, setSortBy] = useState("");
  const [sortDesc, setSortDesc] = useState(false);
  const [selectedRow, setSelectedRow] = useState<Record<string, unknown> | null>(null);
  const pageSize = 50;

  const summaryQuery = useQuery({
    queryKey: ["fiscal", "fiscalizacao", "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getFiscalizacaoResumo(selectedCnpj),
  });

  const cadastroQuery = useQuery({
    queryKey: ["fiscal", "fiscalizacao", "cadastro", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => {
      if (!selectedCnpj) throw new Error("Selecione um CNPJ para carregar o cadastro.");
      return fiscalFeatureApi.getFiscalizacaoCadastro(selectedCnpj);
    },
    enabled: Boolean(selectedCnpj),
  });

  const malhasQuery = useQuery({
    queryKey: [
      "fiscal",
      "fiscalizacao",
      "malhas",
      selectedCnpj ?? "sem-cnpj",
      page,
      filterText,
      sortBy,
      sortDesc,
    ],
    queryFn: () => {
      if (!selectedCnpj) throw new Error("Selecione um CNPJ para carregar as malhas.");
      return fiscalFeatureApi.getFiscalizacaoMalhas(selectedCnpj, {
        page,
        pageSize,
        sortBy: sortBy || undefined,
        sortDesc,
        filterText: filterText.trim() || undefined,
      });
    },
    enabled: Boolean(selectedCnpj),
  });

  const dsfsQuery = useQuery({
    queryKey: ["fiscal", "fiscalizacao", "dsfs", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => {
      if (!selectedCnpj) throw new Error("Selecione um CNPJ para carregar as DSFs.");
      return fiscalFeatureApi.getFiscalizacaoDsfs(selectedCnpj);
    },
    enabled: Boolean(selectedCnpj),
  });

  const totalPages = malhasQuery.data?.total_pages ?? 1;
  const sortColumns = useMemo(() => malhasQuery.data?.all_columns ?? [], [malhasQuery.data]);

  return (
    <FiscalPageShell
      title="Fiscalização"
      subtitle="Fronteira, Fisconforme, malhas, chaves e resoluções."
    >
      <div className="space-y-4">
        <FiscalDomainOverview
          data={summaryQuery.data}
          isLoading={summaryQuery.isLoading}
          errorMessage={summaryQuery.error instanceof Error ? summaryQuery.error.message : undefined}
        />

        <KeyValuePanel data={cadastroQuery.data} isLoading={cadastroQuery.isLoading} />

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-3 text-sm font-semibold text-white">Filtro e ordenação das malhas</div>
          <div className="grid gap-3 md:grid-cols-[1.4fr_1fr_auto]">
            <input
              value={filterText}
              onChange={(event) => {
                setFilterText(event.target.value);
                setPage(1);
                setSelectedRow(null);
              }}
              placeholder="Buscar texto em qualquer coluna"
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            />
            <select
              value={sortBy}
              onChange={(event) => {
                setSortBy(event.target.value);
                setPage(1);
                setSelectedRow(null);
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
                setSelectedRow(null);
              }}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 hover:bg-slate-900/50"
            >
              {sortDesc ? "Desc" : "Asc"}
            </button>
          </div>
        </section>

        <MalhasTable
          data={malhasQuery.data}
          isLoading={malhasQuery.isLoading}
          selectedRow={selectedRow}
          onSelectRow={setSelectedRow}
        />

        <FiscalRowDetailPanel
          row={selectedRow}
          title="Detalhe da malha selecionada"
          subtitle="Clique em uma linha da tabela de malhas para inspecionar todos os campos do registro."
          emptyMessage="Selecione uma malha da tabela para ver o registro completo."
        />

        <div className="flex items-center justify-end rounded-2xl border border-slate-700 bg-slate-900/30 p-4 text-xs text-slate-400">
          <div className="flex items-center gap-2">
            <button
              onClick={() => {
                setPage((current) => Math.max(1, current - 1));
                setSelectedRow(null);
              }}
              disabled={page <= 1 || !selectedCnpj || malhasQuery.isLoading}
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
              disabled={page >= totalPages || !selectedCnpj || malhasQuery.isLoading}
              className="rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 text-slate-200 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Próxima →
            </button>
          </div>
        </div>

        <DsfPanel items={dsfsQuery.data} isLoading={dsfsQuery.isLoading} />
      </div>
    </FiscalPageShell>
  );
}

export default FiscalizacaoTab;
