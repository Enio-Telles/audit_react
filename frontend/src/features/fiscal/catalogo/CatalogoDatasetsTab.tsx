import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import type { DatasetAvailabilityItem } from "../types";
import { FiscalPageShell } from "../shared/FiscalPageShell";

function formatValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "—";
  }
  if (Array.isArray(value)) {
    return value.map((item) => formatValue(item)).join(", ");
  }
  if (typeof value === "object") {
    return JSON.stringify(value, null, 2);
  }
  return String(value);
}

function SummaryCard({ title, value, description }: { title: string; value: string; description: string }) {
  return (
    <div className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="text-xs font-medium uppercase tracking-wide text-slate-400">{title}</div>
      <div className="mt-2 text-2xl font-semibold text-white">{value}</div>
      <div className="mt-2 text-xs text-slate-500">{description}</div>
    </div>
  );
}

function DatasetList({
  items,
  selectedDatasetId,
  onSelect,
}: {
  items: DatasetAvailabilityItem[];
  selectedDatasetId: string;
  onSelect: (datasetId: string) => void;
}) {
  return (
    <div className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 flex items-center justify-between">
        <div>
          <div className="text-sm font-semibold text-white">Datasets por CNPJ</div>
          <div className="text-xs text-slate-500">Disponibilidade, formato e reaproveitamento do catálogo.</div>
        </div>
        <div className="text-xs text-slate-400">{items.length} item(ns)</div>
      </div>
      <div className="max-h-[440px] overflow-auto rounded-xl border border-slate-800">
        <table className="min-w-full border-collapse text-xs">
          <thead className="sticky top-0 z-10 bg-slate-950/95">
            <tr>
              <th className="border-b border-slate-800 px-3 py-2 text-left font-medium text-slate-300">Dataset</th>
              <th className="border-b border-slate-800 px-3 py-2 text-left font-medium text-slate-300">Status</th>
              <th className="border-b border-slate-800 px-3 py-2 text-left font-medium text-slate-300">Formato</th>
            </tr>
          </thead>
          <tbody>
            {items.map((item, index) => {
              const active = item.dataset_id === selectedDatasetId;
              return (
                <tr
                  key={item.dataset_id}
                  onClick={() => onSelect(item.dataset_id)}
                  className={`cursor-pointer ${active ? "bg-blue-950/30" : index % 2 === 0 ? "bg-slate-900/20" : "bg-slate-950/30"}`}
                >
                  <td className="border-b border-slate-800 px-3 py-2 text-slate-200">
                    <div className="font-medium">{item.dataset_id}</div>
                    {item.aliases.length > 0 ? (
                      <div className="mt-1 text-[11px] text-slate-500">aliases: {item.aliases.join(", ")}</div>
                    ) : null}
                  </td>
                  <td className="border-b border-slate-800 px-3 py-2 text-slate-300">
                    {item.disponivel ? "disponível" : "ausente"}
                    {item.reutilizado ? <div className="mt-1 text-[11px] text-amber-400">fallback/reutilizado</div> : null}
                  </td>
                  <td className="border-b border-slate-800 px-3 py-2 text-slate-300">{item.formato ?? "—"}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function PreviewTable({
  columns,
  rows,
}: {
  columns: string[];
  rows: Array<Record<string, unknown>>;
}) {
  return (
    <div className="overflow-auto rounded-xl border border-slate-800">
      <table className="min-w-full border-collapse text-xs">
        <thead className="sticky top-0 z-10 bg-slate-950/95">
          <tr>
            {columns.map((column) => (
              <th key={column} className="border-b border-slate-800 px-3 py-2 text-left font-medium text-slate-300">
                {column}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length === 0 ? (
            <tr>
              <td colSpan={Math.max(columns.length, 1)} className="px-3 py-8 text-center text-slate-500">
                Nenhuma linha disponível para prévia.
              </td>
            </tr>
          ) : (
            rows.map((row, rowIndex) => (
              <tr key={rowIndex} className={rowIndex % 2 === 0 ? "bg-slate-900/20" : "bg-slate-950/30"}>
                {columns.map((column) => (
                  <td key={`${rowIndex}-${column}`} className="max-w-[320px] truncate border-b border-slate-800 px-3 py-2 text-slate-300" title={formatValue(row[column])}>
                    {formatValue(row[column])}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

export function CatalogoDatasetsTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);
  const [cnpjInput, setCnpjInput] = useState(selectedCnpj ?? "");
  const [activeCnpj, setActiveCnpj] = useState(selectedCnpj ?? "");
  const [selectedDatasetId, setSelectedDatasetId] = useState("");

  useEffect(() => {
    if (selectedCnpj) {
      setCnpjInput(selectedCnpj);
      setActiveCnpj((current) => current || selectedCnpj);
    }
  }, [selectedCnpj]);

  const summaryQuery = useQuery({
    queryKey: ["fiscal", "catalogo", "summary"],
    queryFn: () => fiscalFeatureApi.getDatasetCatalogSummary(),
  });

  const availabilityQuery = useQuery({
    queryKey: ["fiscal", "catalogo", "availability", activeCnpj || "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getDatasetCatalogForCnpj(activeCnpj),
    enabled: Boolean(activeCnpj),
  });

  const items = availabilityQuery.data?.items ?? [];
  const orderedItems = useMemo(
    () => [...items].sort((a, b) => Number(b.disponivel) - Number(a.disponivel) || a.dataset_id.localeCompare(b.dataset_id)),
    [items],
  );

  useEffect(() => {
    if (!orderedItems.length) {
      setSelectedDatasetId("");
      return;
    }
    if (!selectedDatasetId || !orderedItems.some((item) => item.dataset_id === selectedDatasetId)) {
      const firstAvailable = orderedItems.find((item) => item.disponivel);
      setSelectedDatasetId(firstAvailable?.dataset_id ?? orderedItems[0].dataset_id);
    }
  }, [orderedItems, selectedDatasetId]);

  const inspectionQuery = useQuery({
    queryKey: ["fiscal", "catalogo", "inspect", activeCnpj || "sem-cnpj", selectedDatasetId || "sem-dataset"],
    queryFn: () => fiscalFeatureApi.inspectDatasetCatalog(activeCnpj, selectedDatasetId, 20),
    enabled: Boolean(activeCnpj && selectedDatasetId),
  });

  const disponiveis = orderedItems.filter((item) => item.disponivel).length;
  const ausentes = orderedItems.length - disponiveis;
  const previewColumns = inspectionQuery.data?.columns ?? [];
  const previewRows = inspectionQuery.data?.preview ?? [];
  const aliases = inspectionQuery.data?.aliases ?? [];
  const metadata = inspectionQuery.data?.metadata ?? null;
  const probe = inspectionQuery.data?.probe ?? {};

  return (
    <FiscalPageShell
      title="Catálogo de Datasets"
      subtitle="Inspeção operacional da materialização canônica e disponibilidade por CNPJ."
    >
      <div className="space-y-4">
        <div className="grid gap-4 lg:grid-cols-3">
          <SummaryCard
            title="Datasets catalogados"
            value={String(summaryQuery.data?.total_datasets ?? 0)}
            description="Total de nomes canônicos conhecidos pelo backend."
          />
          <SummaryCard
            title="Aliases"
            value={String(summaryQuery.data?.total_aliases ?? 0)}
            description="Equivalências de nomes legados para nomes canônicos."
          />
          <SummaryCard
            title="Materializados no CNPJ"
            value={activeCnpj ? `${disponiveis}/${orderedItems.length || 0}` : "—"}
            description="Quantidade de datasets localizados para o CNPJ em foco."
          />
        </div>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-3 text-sm font-semibold text-white">Consulta operacional</div>
          <div className="grid gap-3 lg:grid-cols-[1fr_auto_auto]">
            <input
              value={cnpjInput}
              onChange={(event) => setCnpjInput(event.target.value)}
              placeholder="Informe ou confirme o CNPJ"
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            />
            <button
              onClick={() => setActiveCnpj(cnpjInput)}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-4 py-2 text-sm text-slate-200 hover:bg-slate-900/50"
            >
              Inspecionar
            </button>
            <button
              onClick={() => {
                if (selectedCnpj) {
                  setCnpjInput(selectedCnpj);
                  setActiveCnpj(selectedCnpj);
                }
              }}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-4 py-2 text-sm text-slate-200 hover:bg-slate-900/50"
            >
              Usar CNPJ selecionado
            </button>
          </div>
          <div className="mt-3 text-xs text-slate-500">
            CNPJ em foco: <span className="font-mono text-slate-300">{activeCnpj || "não informado"}</span>
            {availabilityQuery.isFetching ? <span className="ml-2 text-blue-300">atualizando…</span> : null}
          </div>
        </section>

        <div className="grid gap-4 xl:grid-cols-[0.95fr_1.05fr]">
          <DatasetList items={orderedItems} selectedDatasetId={selectedDatasetId} onSelect={setSelectedDatasetId} />

          <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
            <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
              <div>
                <div className="text-sm font-semibold text-white">Inspeção do dataset</div>
                <div className="text-xs text-slate-500">Probe, aliases, metadata e prévia de linhas.</div>
              </div>
              <div className="text-xs text-slate-400">{selectedDatasetId || "nenhum dataset selecionado"}</div>
            </div>

            <div className="grid gap-3 md:grid-cols-2">
              <div className="rounded-xl border border-slate-800 bg-slate-950/30 p-3 text-xs text-slate-300">
                <div className="mb-2 font-medium text-slate-200">Resumo</div>
                <div><span className="text-slate-500">status:</span> {formatValue((probe as Record<string, unknown>).status)}</div>
                <div><span className="text-slate-500">rows:</span> {formatValue((probe as Record<string, unknown>).rows)}</div>
                <div><span className="text-slate-500">formato:</span> {formatValue((probe as Record<string, unknown>).format)}</div>
                <div><span className="text-slate-500">caminho:</span> {formatValue(inspectionQuery.data?.caminho)}</div>
                <div><span className="text-slate-500">reutilizado:</span> {formatValue(inspectionQuery.data?.reutilizado)}</div>
              </div>
              <div className="rounded-xl border border-slate-800 bg-slate-950/30 p-3 text-xs text-slate-300">
                <div className="mb-2 font-medium text-slate-200">Aliases e metadata</div>
                <div><span className="text-slate-500">aliases:</span> {aliases.length ? aliases.join(", ") : "—"}</div>
                <div className="mt-2 text-slate-500">metadata:</div>
                <pre className="mt-1 max-h-40 overflow-auto whitespace-pre-wrap break-words text-[11px] text-slate-300">{formatValue(metadata)}</pre>
              </div>
            </div>

            <div className="mt-4">
              <div className="mb-2 text-sm font-semibold text-white">Prévia de linhas</div>
              <PreviewTable columns={previewColumns} rows={previewRows} />
            </div>

            {!activeCnpj ? (
              <div className="mt-3 text-xs text-amber-300">Informe um CNPJ para listar e inspecionar datasets materializados.</div>
            ) : null}
          </section>
        </div>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-2 text-sm font-semibold text-white">Atalhos de leitura</div>
          <div className="grid gap-2 md:grid-cols-2 xl:grid-cols-3">
            <div className="rounded-xl border border-slate-800 bg-slate-950/30 p-3 text-xs text-slate-300">
              <div className="font-medium text-slate-200">Disponíveis</div>
              <div className="mt-1 text-slate-500">{disponiveis} dataset(s) localizados para o CNPJ atual.</div>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/30 p-3 text-xs text-slate-300">
              <div className="font-medium text-slate-200">Ausentes</div>
              <div className="mt-1 text-slate-500">{ausentes} dataset(s) ainda não materializados para o CNPJ atual.</div>
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/30 p-3 text-xs text-slate-300">
              <div className="font-medium text-slate-200">Catálogo materializado</div>
              <div className="mt-1 text-slate-500">{(summaryQuery.data?.materialized_datasets ?? []).slice(0, 6).join(", ") || "—"}</div>
            </div>
          </div>
        </section>
      </div>
    </FiscalPageShell>
  );
}

export default CatalogoDatasetsTab;
