import React, { useEffect, useMemo, useState } from "react";
import {
  fetchEfdCompare,
  fetchEfdDataset,
  fetchEfdDictionary,
  fetchEfdManifest,
  fetchEfdRecords,
  fetchEfdRowProvenance,
  fetchEfdTree,
  type EfdCompareResponse,
  type EfdDatasetResponse,
} from "./api";
import { EfdRecordSwitcher } from "./components/EfdRecordSwitcher";
import { EfdDictionaryPanel } from "./components/EfdDictionaryPanel";
import { EfdComparisonPanel } from "./components/EfdComparisonPanel";
import { EfdTreePanel } from "./components/EfdTreePanel";
import { FiscalDataTable } from "../shared/components/FiscalDataTable";

export default function EfdPage() {
  const [records, setRecords] = useState<Array<{ record: string; title: string; description: string }>>([]);
  const [selectedRecord, setSelectedRecord] = useState("c100");
  const [cnpj, setCnpj] = useState("");
  const [periodo, setPeriodo] = useState("");
  const [periodoA, setPeriodoA] = useState("");
  const [periodoB, setPeriodoB] = useState("");
  const [dataset, setDataset] = useState<EfdDatasetResponse | null>(null);
  const [dictionary, setDictionary] = useState<Array<{ field: string; label: string; description: string }>>([]);
  const [manifest, setManifest] = useState<Record<string, unknown> | null>(null);
  const [comparison, setComparison] = useState<EfdCompareResponse | null>(null);
  const [tree, setTree] = useState<{ doc_key: string; documents: Array<Record<string, unknown>> } | null>(null);
  const [rowProvenance, setRowProvenance] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchEfdRecords().then(setRecords).catch(console.error);
  }, []);

  useEffect(() => {
    fetchEfdDictionary(selectedRecord)
      .then((payload) => setDictionary(payload.fields))
      .catch(console.error);
  }, [selectedRecord]);

  const columns = useMemo(() => dataset?.columns ?? [], [dataset]);

  async function reloadDataset() {
    setLoading(true);
    try {
      const [datasetPayload, manifestPayload] = await Promise.all([
        fetchEfdDataset(selectedRecord, { cnpj, periodo, page: 1, pageSize: 200, preferLayer: "base" }),
        fetchEfdManifest(selectedRecord, cnpj || undefined),
      ]);
      setDataset(datasetPayload);
      setManifest(manifestPayload as unknown as Record<string, unknown>);
      if (selectedRecord === "c100" && cnpj) {
        const treePayload = await fetchEfdTree({ cnpj, periodo: periodo || undefined });
        setTree(treePayload as { doc_key: string; documents: Array<Record<string, unknown>> });
      } else {
        setTree(null);
      }
    } finally {
      setLoading(false);
    }
  }

  async function handleCompare() {
    if (!cnpj || !periodoA || !periodoB) return;
    const payload = await fetchEfdCompare(selectedRecord, { cnpj, periodoA, periodoB });
    setComparison(payload);
  }

  async function handleRowClick(row: Record<string, unknown>) {
    const firstDictionaryField = dictionary[0]?.field;
    const candidateValue =
      row[firstDictionaryField] ??
      row["chv_nfe"] ??
      row["chave_nfe"] ??
      row["cod_item"] ??
      row["num_doc"];

    if (!candidateValue) {
      setRowProvenance(null);
      return;
    }

    const payload = await fetchEfdRowProvenance(selectedRecord, {
      rowIdentifier: String(candidateValue),
      cnpj: cnpj || undefined,
    });
    setRowProvenance(payload as Record<string, unknown>);
  }

  return (
    <div className="space-y-4">
      <div className="rounded border bg-white p-4">
        <div className="mb-3 text-lg font-semibold">EFD</div>
        <div className="grid gap-3 md:grid-cols-5">
          <input className="rounded border px-3 py-2" placeholder="CNPJ" value={cnpj} onChange={(e) => setCnpj(e.target.value)} />
          <input className="rounded border px-3 py-2" placeholder="Período (AAAAMM)" value={periodo} onChange={(e) => setPeriodo(e.target.value)} />
          <input className="rounded border px-3 py-2" placeholder="Comparar A" value={periodoA} onChange={(e) => setPeriodoA(e.target.value)} />
          <input className="rounded border px-3 py-2" placeholder="Comparar B" value={periodoB} onChange={(e) => setPeriodoB(e.target.value)} />
          <div className="flex gap-2">
            <button className="rounded bg-slate-900 px-3 py-2 text-white" onClick={reloadDataset} type="button">
              {loading ? "Carregando..." : "Carregar"}
            </button>
            <button className="rounded border px-3 py-2" onClick={handleCompare} type="button">
              Comparar
            </button>
          </div>
        </div>
      </div>

      <EfdRecordSwitcher
        records={records.map((item) => ({ record: item.record, title: item.title }))}
        value={selectedRecord}
        onChange={setSelectedRecord}
      />

      <div className="grid gap-4 xl:grid-cols-[2fr,1fr]">
        <div className="space-y-4">
          <div className="rounded border bg-white p-3">
            <div className="mb-2 text-sm font-semibold">Manifest / rastreabilidade</div>
            <pre className="overflow-auto rounded bg-slate-50 p-3 text-xs">{JSON.stringify(manifest, null, 2)}</pre>
          </div>

          <FiscalDataTable columns={columns} rows={dataset?.records ?? []} onRowClick={handleRowClick} />

          {selectedRecord === "c100" ? <EfdTreePanel data={tree} /> : null}
        </div>

        <div className="space-y-4">
          <EfdDictionaryPanel fields={dictionary} />
          <EfdComparisonPanel data={comparison} />
          <div className="rounded border bg-white p-3">
            <div className="mb-2 text-sm font-semibold">Proveniência da linha</div>
            <pre className="overflow-auto rounded bg-slate-50 p-3 text-xs">{JSON.stringify(rowProvenance, null, 2)}</pre>
          </div>
        </div>
      </div>
    </div>
  );
}
