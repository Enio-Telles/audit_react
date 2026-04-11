import { useEffect, useMemo, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { cnpjApi, pipelineApi, sqlApi } from "../../api/client";
import { fiscalFeatureApi } from "../../features/fiscal/api";
import { useAppStore } from "../../store/appStore";
import { ExtractionApproachSelector, type ExtractionApproach } from "../pipeline/ExtractionApproachSelector";
import { ExtractionReadinessBanner } from "../pipeline/ExtractionReadinessBanner";
import { ExtrairSelecaoModal } from "../modals/ExtrairSelecaoModal";
import { GerenciarConsultasModal } from "../modals/GerenciarConsultasModal";
import { GerenciarCnpjModal } from "../modals/GerenciarCnpjModal";

const LS_KEY = "fiscalParquet.selectedConsultas";

const inputCls =
  "w-full bg-slate-800 border border-slate-600 rounded px-2 py-1.5 text-sm text-slate-200 placeholder-slate-500 focus:outline-none focus:border-blue-500";
const btnCls =
  "px-3 py-1.5 rounded text-xs font-medium cursor-pointer transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed";

function sanitizeCnpj(value: string | null | undefined): string | null {
  const cleaned = String(value ?? "").replace(/\D/g, "");
  return cleaned || null;
}

export function LeftPanel() {
  const queryClient = useQueryClient();
  const {
    selectedCnpj,
    setSelectedCnpj,
    selectedFile,
    setSelectedFile,
    pipelineWatchCnpj,
    pipelineStatus,
    pipelinePolling,
    startPipelineMonitor,
    updatePipelineStatus,
    selectedConsultas,
    setSelectedConsultas,
    setActiveTab,
  } = useAppStore();

  const [newCnpj, setNewCnpj] = useState("");
  const [dataLimite, setDataLimite] = useState("12/03/2026");
  const [showSqlSelector, setShowSqlSelector] = useState(false);
  const [showExtrairModal, setShowExtrairModal] = useState(false);
  const [showGerenciarModal, setShowGerenciarModal] = useState(false);
  const [selectedApproach, setSelectedApproach] = useState<ExtractionApproach>("full");
  const [gerenciarCnpj, setGerenciarCnpj] = useState<{
    cnpj: string;
    razaoSocial: string | null;
  } | null>(null);

  const { data: cnpjs = [] } = useQuery({
    queryKey: ["cnpjs"],
    queryFn: cnpjApi.list,
  });

  const { data: files = [] } = useQuery({
    queryKey: ["files", selectedCnpj],
    queryFn: () => cnpjApi.listFiles(selectedCnpj!),
    enabled: !!selectedCnpj,
  });

  const { data: sqlFiles = [] } = useQuery({
    queryKey: ["sqlFiles"],
    queryFn: sqlApi.listFiles,
    staleTime: Infinity,
  });

  const catalogCnpj = useMemo(
    () => sanitizeCnpj(newCnpj.trim()) ?? sanitizeCnpj(selectedCnpj),
    [newCnpj, selectedCnpj],
  );

  const catalogAvailabilityQuery = useQuery({
    queryKey: ["dataset-catalog", catalogCnpj],
    queryFn: () => fiscalFeatureApi.getDatasetCatalogForCnpj(catalogCnpj!),
    enabled: Boolean(catalogCnpj),
    staleTime: 30_000,
  });

  useEffect(() => {
    if (sqlFiles.length === 0) return;
    try {
      const raw = localStorage.getItem(LS_KEY);
      if (!raw) return;
      const parsed: unknown = JSON.parse(raw);
      if (!Array.isArray(parsed)) return;
      const validPaths = new Set(sqlFiles.map((f) => f.path));
      const filtered = (parsed as unknown[]).filter(
        (p) => typeof p === "string" && validPaths.has(p as string),
      ) as string[];
      if (filtered.length > 0 && filtered.length < sqlFiles.length) {
        setSelectedConsultas(filtered);
      }
    } catch {
      // ignorar erros de parse
    }
  }, [sqlFiles, setSelectedConsultas]);

  const addMutation = useMutation({
    mutationFn: (cnpj: string) => cnpjApi.add(cnpj),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: ["cnpjs"] }),
  });

  const ensureSelectedCnpj = async () => {
    const typedCnpj = newCnpj.trim();
    if (typedCnpj) {
      const record = await addMutation.mutateAsync(typedCnpj);
      setSelectedCnpj(record.cnpj);
      setNewCnpj("");
      return record.cnpj;
    }
    return selectedCnpj;
  };

  const runPipeline = async (modo: ExtractionApproach) => {
    const cnpj = await ensureSelectedCnpj();
    if (!cnpj) return;

    startPipelineMonitor(cnpj, {
      status: "queued",
      progresso: [],
      erros: [],
      percentual: 0,
      etapas_concluidas: 0,
      total_etapas: 0,
      etapa_atual: "fila",
      item_atual: null,
    });

    await pipelineApi.run({
      cnpj,
      data_limite: modo === "process" ? undefined : dataLimite,
      incluir_extracao: modo !== "process",
      incluir_processamento: modo !== "extract",
      consultas:
        modo !== "process" && selectedConsultas !== null
          ? selectedConsultas
          : undefined,
      tabelas: undefined,
    });
    setSelectedCnpj(cnpj);
  };

  useEffect(() => {
    if (!pipelinePolling || !pipelineWatchCnpj) return;
    const id = setInterval(async () => {
      const s = await pipelineApi.status(pipelineWatchCnpj);
      updatePipelineStatus(s);
      if (s.status === "done" || s.status === "error") {
        queryClient.invalidateQueries({ queryKey: ["files", pipelineWatchCnpj] });
        queryClient.invalidateQueries({ queryKey: ["dataset-catalog", pipelineWatchCnpj] });
      }
    }, 1500);
    return () => clearInterval(id);
  }, [pipelinePolling, pipelineWatchCnpj, queryClient, updatePipelineStatus]);

  const progressValue = pipelineStatus?.percentual ?? 0;
  const etapasConcluidas = pipelineStatus?.etapas_concluidas ?? 0;
  const totalEtapas = pipelineStatus?.total_etapas ?? 0;
  const etapaAtualTexto =
    pipelineStatus?.etapa_atual === "extracao"
      ? "Extraindo tabelas brutas"
      : pipelineStatus?.etapa_atual === "processamento"
        ? "Processando tabelas analiticas"
        : pipelineStatus?.etapa_atual === "fila"
          ? "Aguardando na fila"
          : pipelineStatus?.etapa_atual === "preparacao"
            ? "Preparando execucao"
            : pipelineStatus?.etapa_atual === "concluido"
              ? "Concluido"
              : pipelineStatus?.etapa_atual === "erro"
                ? "Falha no pipeline"
                : "Pipeline";

  const pipelineStatusLabel =
    pipelineStatus?.status === "done"
      ? "Concluido"
      : pipelineStatus?.status === "error"
        ? "Falha no pipeline"
        : pipelineStatus?.item_atual
          ? `${etapaAtualTexto}: ${pipelineStatus.item_atual}`
          : etapaAtualTexto;

  const progressBarCls =
    pipelineStatus?.status === "done"
      ? "bg-emerald-500"
      : pipelineStatus?.status === "error"
        ? "bg-rose-500"
        : "bg-blue-500";

  const consultasResumo = useMemo(() => {
    if (selectedConsultas === null) return `todas (${sqlFiles.length})`;
    return `${selectedConsultas.length} de ${sqlFiles.length}`;
  }, [selectedConsultas, sqlFiles.length]);

  const runLabel =
    selectedApproach === "full"
      ? "Executar extração + processamento"
      : selectedApproach === "extract"
        ? "Executar somente extração"
        : "Executar somente processamento";

  const approachNote =
    selectedApproach === "process"
      ? "Esta abordagem reaproveita o que já está materializado no CNPJ e ignora data limite e consultas SQL."
      : `Esta abordagem vai usar consultas ${consultasResumo} e data limite ${dataLimite}.`;

  const openCatalogTab = () => {
    if (catalogCnpj) setSelectedCnpj(catalogCnpj);
    setActiveTab("catalogo-datasets");
  };

  const sectionCls = "border border-slate-700 rounded p-2 mb-3";
  const sectionTitleCls = "text-xs text-slate-400 font-semibold mb-2 uppercase tracking-wide";

  return (
    <div className="flex flex-col h-full p-2 gap-2 overflow-y-auto" style={{ background: "#0d1f3c", width: 260, minWidth: 260 }}>
      <div className="text-center py-2 border-b border-slate-700">
        <div className="text-sm font-bold text-blue-300">Fiscal Parquet</div>
      </div>

      <div className={sectionCls}>
        <label htmlFor="input-cpf-cnpj" className={sectionTitleCls + " block"}>CPF/CNPJ</label>
        <input
          id="input-cpf-cnpj"
          className={inputCls + " mb-2"}
          placeholder="Digite o CPF ou CNPJ..."
          value={newCnpj}
          onChange={(e) => setNewCnpj(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && newCnpj.trim()) {
              addMutation.mutate(newCnpj.trim());
              setNewCnpj("");
            }
          }}
        />

        <div className="flex items-center gap-2 mb-2">
          <label htmlFor="input-data-limite" className="text-xs text-slate-400">Data limite EFD:</label>
          <input
            id="input-data-limite"
            className="bg-slate-800 border border-slate-600 rounded px-2 py-1 text-xs text-slate-200 focus:outline-none w-28"
            value={dataLimite}
            onChange={(e) => setDataLimite(e.target.value)}
          />
        </div>

        <div className="mb-2 text-[11px] text-slate-500">
          Realidade atual do código: o frontend só dispara três modos reais do pipeline — extrair + processar, somente extrair ou somente processar.
        </div>

        <ExtractionApproachSelector
          selectedApproach={selectedApproach}
          onSelect={setSelectedApproach}
          consultasResumo={consultasResumo}
          dataLimite={dataLimite}
        />

        <div className="mt-2">
          <ExtractionReadinessBanner
            cnpj={catalogCnpj}
            items={catalogAvailabilityQuery.data?.items ?? []}
            loading={catalogAvailabilityQuery.isFetching}
            selectedApproach={selectedApproach}
            onApplySuggestion={setSelectedApproach}
            onOpenCatalog={openCatalogTab}
          />
        </div>

        <div className="mt-2 rounded-xl border border-slate-800 bg-slate-950/30 p-2 text-[11px] text-slate-400">{approachNote}</div>

        <div className="mt-2 grid grid-cols-1 gap-2">
          <button
            className={btnCls + " bg-blue-700 hover:bg-blue-600 text-white"}
            onClick={() => void runPipeline(selectedApproach)}
            disabled={(!selectedCnpj && !newCnpj.trim()) || pipelinePolling || addMutation.isPending}
            aria-busy={pipelinePolling || addMutation.isPending}
          >
            {pipelinePolling || addMutation.isPending ? "Processando..." : runLabel}
          </button>

          <div className="grid grid-cols-2 gap-2">
            <button className={btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"} onClick={() => setShowExtrairModal(true)} disabled={pipelinePolling}>Seleção guiada SQL</button>
            <button className={btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"} onClick={() => setShowGerenciarModal(true)}>Gerenciar consultas</button>
          </div>

          <div className="grid grid-cols-2 gap-1">
            <button
              className={btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"}
              onClick={() => {
                void queryClient.invalidateQueries({ queryKey: ["cnpjs"] });
                if (selectedCnpj) void queryClient.invalidateQueries({ queryKey: ["files", selectedCnpj] });
                if (catalogCnpj) void queryClient.invalidateQueries({ queryKey: ["dataset-catalog", catalogCnpj] });
              }}
            >
              Atualizar lista
            </button>
            <button className={btnCls + " bg-slate-700 hover:bg-slate-600 text-slate-200"} onClick={openCatalogTab}>Abrir catálogo</button>
          </div>
        </div>

        <div className="mt-2 border border-slate-700 rounded text-xs">
          <button
            className="w-full flex items-center justify-between px-2 py-1.5 text-slate-300 hover:bg-slate-700 rounded"
            onClick={() => setShowSqlSelector((s) => !s)}
          >
            <span>{selectedConsultas === null ? `Consultas SQL da extração: todas (${sqlFiles.length})` : `Consultas SQL da extração: ${selectedConsultas.length} de ${sqlFiles.length}`}</span>
            <span className="text-slate-500">{showSqlSelector ? "▲" : "▼"}</span>
          </button>
          {showSqlSelector && (
            <div className="border-t border-slate-700 p-1.5">
              <div className="mb-1 text-[10px] text-slate-500">Essas consultas são usadas pelas abordagens <span className="text-slate-300">Extrair + Processar</span> e <span className="text-slate-300">Somente Extração</span>.</div>
              <div className="flex gap-1 mb-1.5 flex-wrap">
                <button className="px-1.5 py-0.5 rounded bg-blue-800 hover:bg-blue-700 text-blue-100 text-[10px]" onClick={() => setSelectedConsultas(null)}>Todas</button>
                <button className="px-1.5 py-0.5 rounded bg-slate-700 hover:bg-slate-600 text-slate-300 text-[10px]" onClick={() => setSelectedConsultas([])}>Nenhuma</button>
              </div>
              <div className="overflow-y-auto flex flex-col gap-0.5" style={{ maxHeight: 200 }}>
                {sqlFiles.map((f) => {
                  const checked = selectedConsultas === null || selectedConsultas.includes(f.path);
                  return (
                    <label key={f.path} className="flex items-center gap-1.5 px-1 py-0.5 rounded hover:bg-slate-700 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={checked}
                        className="accent-blue-500 shrink-0"
                        onChange={() => {
                          if (selectedConsultas === null) {
                            setSelectedConsultas(sqlFiles.map((x) => x.path).filter((p) => p !== f.path));
                          } else if (checked) {
                            const next = selectedConsultas.filter((p) => p !== f.path);
                            setSelectedConsultas(next.length === 0 ? [] : next);
                          } else {
                            const next = [...selectedConsultas, f.path];
                            setSelectedConsultas(next.length === sqlFiles.length ? null : next);
                          }
                        }}
                      />
                      <span className="text-[10px] text-slate-300 truncate" title={f.path}>{f.path}</span>
                    </label>
                  );
                })}
              </div>
            </div>
          )}
        </div>
      </div>

      {pipelineStatus && pipelineStatus.status !== "idle" && (
        <div className="border border-slate-700 rounded p-2 text-xs">
          <div className={sectionTitleCls}>Pipeline</div>
          <div className="mb-2">
            <div className="mb-1 flex items-center justify-between text-[11px] text-slate-300">
              <span>{pipelineStatusLabel}</span>
              <span>{Math.round(progressValue)}%</span>
            </div>
            <div className="mb-1 text-[10px] text-slate-400">{totalEtapas > 0 ? `${etapasConcluidas} de ${totalEtapas} etapa(s) concluida(s)` : "Aguardando definicao das etapas"}</div>
            <div className="h-2 overflow-hidden rounded-full bg-slate-800">
              <div
                role="progressbar"
                aria-valuenow={Math.round(progressValue)}
                aria-valuemin={0}
                aria-valuemax={100}
                className={`h-full rounded-full transition-all duration-500 ${progressBarCls} ${(pipelineStatus.status === "queued" || pipelineStatus.status === "running") ? "animate-pulse" : ""}`}
                style={{ width: `${progressValue}%` }}
              />
            </div>
          </div>
          <div className="overflow-auto" style={{ maxHeight: 100, background: "#060e1f", padding: 6, borderRadius: 4 }}>
            {pipelineStatus.progresso.slice(-20).map((msg, i) => <div key={i} className="text-slate-300 font-mono">{msg}</div>)}
            {pipelineStatus.progresso.length === 0 && pipelineStatus.erros.length === 0 && <div className="text-slate-400 font-mono">Aguardando atualizacao do pipeline...</div>}
            {pipelineStatus.erros.map((e, i) => <div key={`e${i}`} className="text-red-400 font-mono">{e}</div>)}
          </div>
          <div className="mt-2 flex items-center justify-between gap-2">
            <div className={`text-xs font-semibold ${pipelineStatus.status === "done" ? "text-green-400" : pipelineStatus.status === "error" ? "text-red-400" : "text-yellow-400"}`}>{pipelineStatus.status.toUpperCase()}</div>
            {(pipelineStatus.status === "done" || pipelineStatus.status === "error") && pipelineWatchCnpj ? (
              <button
                className="rounded bg-slate-700 px-2 py-1 text-[10px] font-medium text-slate-200 hover:bg-slate-600"
                onClick={() => {
                  setSelectedCnpj(pipelineWatchCnpj);
                  setActiveTab("catalogo-datasets");
                }}
              >
                Ver catálogo do CNPJ
              </button>
            ) : null}
          </div>
        </div>
      )}

      <div className={sectionCls + " flex-1"}>
        <div className={sectionTitleCls}>CNPJs registrados</div>
        <div className="flex flex-col gap-0.5">
          {cnpjs.map((r) => (
            <div key={r.cnpj} className="flex items-center gap-0.5 group">
              <button
                onClick={() => setSelectedCnpj(r.cnpj)}
                className={`flex-1 text-left px-2 py-1.5 rounded text-xs transition-colors ${selectedCnpj === r.cnpj ? "bg-blue-700 text-white" : "text-slate-300 hover:bg-slate-700"}`}
                title={r.razao_social ? `${r.cnpj} - ${r.razao_social}` : r.cnpj}
              >
                <div className="font-semibold">{r.cnpj}</div>
                <div className="truncate text-[11px] text-slate-400">{r.razao_social ?? "Razão social não disponível"}</div>
              </button>
              <button
                onClick={() => setGerenciarCnpj({ cnpj: r.cnpj, razaoSocial: r.razao_social })}
                title="Gerenciar dados do CNPJ"
                aria-label="Gerenciar dados do CNPJ"
                className="shrink-0 px-1.5 py-1.5 rounded text-slate-500 hover:text-slate-200 hover:bg-slate-700 opacity-0 group-hover:opacity-100 transition-opacity text-[11px]"
              >
                ⚙
              </button>
            </div>
          ))}
        </div>
      </div>

      {selectedCnpj && files.length > 0 && (
        <div className={sectionCls}>
          <div className={sectionTitleCls}>Arquivos Parquet do CNPJ</div>
          <div className="text-xs text-slate-500 mb-1 grid grid-cols-2 gap-1 font-semibold"><span>Arquivo</span><span>Local</span></div>
          <div className="overflow-y-auto" style={{ maxHeight: 200 }}>
            {files.map((f) => (
              <button
                key={f.path}
                onClick={() => setSelectedFile(f)}
                className={`w-full text-left px-1 py-0.5 rounded text-xs truncate ${selectedFile?.path === f.path ? "bg-blue-800 text-blue-200" : "hover:bg-slate-700 text-slate-300"}`}
                title={f.name}
              >
                {f.name}
              </button>
            ))}
          </div>
        </div>
      )}

      <div className="text-xs text-slate-500 py-1 border-t border-slate-700">CNPJ selecionado: {selectedCnpj ?? "—"}</div>

      {showExtrairModal && (
        <ExtrairSelecaoModal
          key={String(showExtrairModal)}
          onClose={() => setShowExtrairModal(false)}
          onConfirm={(sel) => {
            setSelectedConsultas(sel);
            setShowExtrairModal(false);
          }}
          sqlFiles={sqlFiles}
          confirmLabel="Aplicar seleção"
        />
      )}

      <GerenciarConsultasModal isOpen={showGerenciarModal} onClose={() => setShowGerenciarModal(false)} />

      {gerenciarCnpj && (
        <GerenciarCnpjModal
          cnpj={gerenciarCnpj.cnpj}
          razaoSocial={gerenciarCnpj.razaoSocial}
          isOpen={true}
          onClose={() => setGerenciarCnpj(null)}
        />
      )}
    </div>
  );
}
