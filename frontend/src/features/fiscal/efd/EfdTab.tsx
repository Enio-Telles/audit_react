import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";

import { DataTable } from "../../../components/table/DataTable";
import { useAppStore } from "../../../store/appStore";
import {
  fiscalFeatureApi,
  type EfdCompareResponse,
} from "../api";
import { abrirFiscalEmNovaAba, lerBootstrapFiscalDaUrl } from "../navigation";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";
import { FiscalPageShell } from "../shared/FiscalPageShell";
import { FiscalRowDetailPanel } from "../shared/FiscalRowDetailPanel";
import { EfdComparisonPanel } from "./components/EfdComparisonPanel";
import { EfdDictionaryPanel } from "./components/EfdDictionaryPanel";
import { EfdRecordSwitcher } from "./components/EfdRecordSwitcher";
import { EfdTreePanel } from "./components/EfdTreePanel";

const ROW_ID_COLUMN = "__efd_row_id";

function deriveRowIdentifier(record: string, row: Record<string, unknown> | null): string | null {
  if (!row) return null;

  const candidatesByRecord: Record<string, string[]> = {
    reg_0000: ["id_arquivo", "arquivo_id", "cnpj"],
    reg_0190: ["cod_unid"],
    reg_0200: ["cod_item"],
    reg_0220: ["cod_item", "unid_conv", "cod_unid_conv"],
    c100: ["chv_nfe", "chave_nfe", "id_doc", "num_doc"],
    c170: ["chv_nfe", "chave_nfe", "id_doc"],
    c190: ["chv_nfe", "chave_nfe", "id_doc", "cfop"],
    c176: ["chv_nfe", "chave_nfe", "id_doc"],
    c197: ["chv_nfe", "chave_nfe", "id_doc", "cod_aj"],
    h005: ["dt_inv"],
    h010: ["cod_item"],
    h020: ["cod_item"],
    k200: ["cod_item"],
  };

  for (const field of candidatesByRecord[record] ?? []) {
    const value = row[field];
    if (value !== null && value !== undefined && String(value) !== "") {
      return String(value);
    }
  }

  for (const field of ["row_id", "id", "id_doc", "chv_nfe", "cod_item"]) {
    const value = row[field];
    if (value !== null && value !== undefined && String(value) !== "") {
      return String(value);
    }
  }

  return null;
}

export function EfdTab() {
  const bootstrap = useMemo(() => lerBootstrapFiscalDaUrl(), []);
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);
  const setActiveTab = useAppStore((state) => state.setActiveTab);
  const [selectedRecord, setSelectedRecord] = useState(
    () => (bootstrap?.abaAtiva === "efd" && bootstrap.record) || "c100",
  );
  const [periodo, setPeriodo] = useState("");
  const [periodoA, setPeriodoA] = useState("");
  const [periodoB, setPeriodoB] = useState("");
  const [page, setPage] = useState(1);
  const [selectedRowKey, setSelectedRowKey] = useState<string | null>(null);
  const [comparison, setComparison] = useState<EfdCompareResponse | null>(null);
  const [compareError, setCompareError] = useState<string | null>(null);

  const summaryQuery = useQuery({
    queryKey: ["fiscal", "efd", "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getEfdResumo(selectedCnpj),
  });

  const recordsQuery = useQuery({
    queryKey: ["fiscal", "efd", "records"],
    queryFn: () => fiscalFeatureApi.getEfdRecords(),
  });

  const dictionaryQuery = useQuery({
    queryKey: ["fiscal", "efd", "dictionary", selectedRecord],
    queryFn: () => fiscalFeatureApi.getEfdDictionary(selectedRecord),
  });

  const manifestQuery = useQuery({
    queryKey: ["fiscal", "efd", "manifest", selectedRecord, selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getEfdManifest(selectedRecord, selectedCnpj),
  });

  const datasetQuery = useQuery({
    queryKey: [
      "fiscal",
      "efd",
      "dataset",
      selectedRecord,
      selectedCnpj ?? "sem-cnpj",
      periodo,
      page,
    ],
    queryFn: () =>
      fiscalFeatureApi.getEfdDatasetRecord(selectedRecord, {
        cnpj: selectedCnpj,
        periodo: periodo || undefined,
        page,
        pageSize: 100,
        preferLayer: "base",
      }),
    enabled: Boolean(selectedCnpj),
  });

  const treeQuery = useQuery({
    queryKey: ["fiscal", "efd", "tree", selectedCnpj ?? "sem-cnpj", periodo],
    queryFn: () =>
      fiscalFeatureApi.getEfdDocumentTree(selectedCnpj!, {
        periodo: periodo || undefined,
        limitDocs: 25,
      }),
    enabled: Boolean(selectedCnpj && selectedRecord === "c100"),
  });

  useEffect(() => {
    setPage(1);
    setSelectedRowKey(null);
    setComparison(null);
    setCompareError(null);
  }, [selectedRecord, selectedCnpj, periodo]);

  const tableRows = useMemo(
    () =>
      (datasetQuery.data?.records ?? []).map((row, index) => ({
        ...row,
        [ROW_ID_COLUMN]: `${page}-${index}`,
      })),
    [datasetQuery.data?.records, page],
  );

  const selectedRow = useMemo(
    () =>
      tableRows.find((row) => String(row[ROW_ID_COLUMN] ?? "") === (selectedRowKey ?? "")) ?? null,
    [selectedRowKey, tableRows],
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

  const rowIdentifier = useMemo(
    () => deriveRowIdentifier(selectedRecord, selectedRowDetail),
    [selectedRecord, selectedRowDetail],
  );

  const rowProvenanceQuery = useQuery({
    queryKey: [
      "fiscal",
      "efd",
      "row-provenance",
      selectedRecord,
      selectedCnpj ?? "sem-cnpj",
      rowIdentifier ?? "sem-linha",
    ],
    queryFn: () =>
      fiscalFeatureApi.getEfdRowProvenance(selectedRecord, {
        rowIdentifier: rowIdentifier!,
        cnpj: selectedCnpj,
        preferLayer: "base",
      }),
    enabled: Boolean(selectedCnpj && rowIdentifier),
  });

  const totalPages = Math.max(
    1,
    Math.ceil((datasetQuery.data?.total ?? 0) / Math.max(datasetQuery.data?.page_size ?? 100, 1)),
  );

  async function handleCompare() {
    if (!selectedCnpj || !periodoA || !periodoB) {
      setCompareError("Informe CNPJ selecionado e dois periodos para comparar.");
      setComparison(null);
      return;
    }

    setCompareError(null);
    try {
      const payload = await fiscalFeatureApi.compareEfdRecord(selectedRecord, {
        cnpj: selectedCnpj,
        periodoA,
        periodoB,
      });
      setComparison(payload);
    } catch (error) {
      setComparison(null);
      setCompareError(error instanceof Error ? error.message : "Falha ao comparar periodos.");
    }
  }

  return (
    <FiscalPageShell
      title="EFD - Escrituracao Fiscal Digital"
      subtitle="Workbench canonico da EFD com registros base, manifest, comparacao entre periodos, arvore documental e proveniencia por linha."
    >
      <div className="space-y-4">
        <FiscalDomainOverview
          data={summaryQuery.data}
          isLoading={summaryQuery.isLoading}
          errorMessage={summaryQuery.error instanceof Error ? summaryQuery.error.message : undefined}
          onOpenShortcut={setActiveTab}
        />

        <EfdRecordSwitcher
          records={recordsQuery.data ?? []}
          value={selectedRecord}
          onChange={setSelectedRecord}
        />

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-3 text-sm font-semibold text-white">Controles EFD</div>
          <div className="grid gap-3 xl:grid-cols-[1fr_1fr_1fr_1fr_auto]">
            <input
              value={periodo}
              onChange={(event) => setPeriodo(event.target.value)}
              placeholder="Periodo (AAAAMM)"
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            />
            <input
              value={periodoA}
              onChange={(event) => setPeriodoA(event.target.value)}
              placeholder="Comparacao A (AAAAMM)"
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            />
            <input
              value={periodoB}
              onChange={(event) => setPeriodoB(event.target.value)}
              placeholder="Comparacao B (AAAAMM)"
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 outline-none focus:border-blue-500"
            />
            <div className="rounded-xl border border-slate-800 bg-slate-950/30 px-3 py-2 text-xs text-slate-400">
              {datasetQuery.isLoading
                ? "Carregando dataset..."
                : `${datasetQuery.data?.dataset_id ?? "sem dataset"} · camada ${datasetQuery.data?.layer ?? "n/d"}`}
            </div>
            <button
              type="button"
              onClick={handleCompare}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 hover:bg-slate-900/50"
            >
              Comparar periodos
            </button>
            <button
              type="button"
              onClick={() =>
                abrirFiscalEmNovaAba({
                  tab: "efd",
                  cnpj: selectedCnpj,
                  record: selectedRecord,
                })
              }
              disabled={!selectedCnpj}
              className="rounded-xl border border-slate-700 bg-slate-950/40 px-3 py-2 text-sm text-slate-200 hover:bg-slate-900/50 disabled:cursor-not-allowed disabled:opacity-40"
            >
              Abrir tabela em nova aba
            </button>
          </div>
          {compareError ? <div className="mt-3 text-sm text-rose-300">{compareError}</div> : null}
        </section>

        <div className="grid gap-4 xl:grid-cols-[1.8fr_1fr]">
          <div className="space-y-4">
            <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
              <div className="mb-3 text-sm font-semibold text-white">Manifest e rastreabilidade</div>
              <pre className="overflow-auto rounded-xl border border-slate-800 bg-slate-950/40 p-3 text-xs text-slate-300">
                {JSON.stringify(manifestQuery.data ?? {}, null, 2)}
              </pre>
            </section>

            <section className="overflow-hidden rounded-2xl border border-slate-700 bg-slate-900/30">
              <div className="border-b border-slate-800 px-4 py-3">
                <div className="flex flex-wrap items-center justify-between gap-2">
                  <div>
                    <div className="text-sm font-semibold text-white">Tabela do registro</div>
                    <div className="text-xs text-slate-500">
                      Leitura canônica do registro selecionado com prioridade para a camada base.
                    </div>
                  </div>
                  <div className="text-xs text-slate-400">
                    {datasetQuery.data
                      ? `${datasetQuery.data.total} linha(s) · pagina ${datasetQuery.data.page} de ${totalPages}`
                      : "Sem dataset carregado"}
                  </div>
                </div>
              </div>

              <DataTable
                columns={datasetQuery.data?.columns ?? []}
                rows={tableRows}
                totalRows={datasetQuery.data?.total ?? 0}
                loading={datasetQuery.isLoading}
                page={page}
                totalPages={totalPages}
                onPageChange={setPage}
                rowKey={ROW_ID_COLUMN}
                selectedRowKeys={selectedRowKeys}
                onRowSelect={(key, checked) => setSelectedRowKey(checked ? key : null)}
                showSelectionCheckboxes={false}
                appearanceKey={`efd_${selectedRecord}`}
                showColumnFilters
              />
            </section>

            {selectedRecord === "c100" ? <EfdTreePanel data={treeQuery.data ?? null} /> : null}

            <FiscalRowDetailPanel
              row={selectedRowDetail}
              title="Detalhe do registro EFD"
              subtitle="Selecione uma linha para inspecionar todos os campos do registro atual."
              emptyMessage="Selecione uma linha da tabela EFD para inspecionar o registro completo."
            />
          </div>

          <div className="space-y-4">
            <EfdDictionaryPanel fields={dictionaryQuery.data?.fields ?? []} />
            <EfdComparisonPanel data={comparison} />
            <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
              <div className="mb-3 text-sm font-semibold text-white">Proveniencia da linha</div>
              <pre className="overflow-auto rounded-xl border border-slate-800 bg-slate-950/40 p-3 text-xs text-slate-300">
                {JSON.stringify(rowProvenanceQuery.data ?? {}, null, 2)}
              </pre>
            </section>
          </div>
        </div>
      </div>
    </FiscalPageShell>
  );
}

export default EfdTab;
