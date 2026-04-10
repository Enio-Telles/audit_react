import { useEffect, useRef } from "react";
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from "@tanstack/react-query";

import { cnpjApi } from "./api/client";
import { LandingPage } from "./components/LandingPage";
import { LeftPanel } from "./components/layout/LeftPanel";
import { AgregacaoTab } from "./components/tabs/AgregacaoTab";
import { ConsultaSqlTab } from "./components/tabs/ConsultaSqlTab";
import { ConsultaTab } from "./components/tabs/ConsultaTab";
import { ConversaoTab } from "./components/tabs/ConversaoTab";
import { EstoqueTab } from "./components/tabs/EstoqueTab";
import { FisconformeTab } from "./components/tabs/FisconformeTab";
import { LogsTab } from "./components/tabs/LogsTab";
import { AnaliseFiscalTab } from "./features/fiscal/analise/AnaliseFiscalTab";
import { DocumentosFiscaisTab } from "./features/fiscal/documentos_fiscais/DocumentosFiscaisTab";
import { EfdTab } from "./features/fiscal/efd/EfdTab";
import { FiscalizacaoTab } from "./features/fiscal/fiscalizacao/FiscalizacaoTab";
import { DossieTab } from "./features/dossie/components/DossieTab";
import {
  ler_bootstrap_dossie_da_url,
  limpar_bootstrap_dossie_da_url,
} from "./features/dossie/navegacao";
import { useAppStore } from "./store/appStore";

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 30_000 } },
});

const TABS = [
  { id: "dossie", label: "Dossiê" },
  { id: "consulta", label: "Consulta" },
  { id: "sql", label: "Consulta SQL" },
  { id: "efd", label: "EFD" },
  { id: "documentos-fiscais", label: "Documentos Fiscais" },
  { id: "fiscalizacao", label: "Fiscalização" },
  { id: "analise-fiscal", label: "Análise Fiscal" },
  { id: "agregacao", label: "Agregação (legado)" },
  { id: "conversao", label: "Conversão (legado)" },
  { id: "estoque", label: "Estoque (legado)" },
  { id: "logs", label: "Logs" },
];

function MainContent() {
  const {
    activeTab,
    setActiveTab,
    leftPanelVisible,
    toggleLeftPanel,
    selectedFile,
    selectedCnpj,
    setSelectedCnpj,
    appMode,
    setAppMode,
  } = useAppStore();
  const bootstrapAplicadoRef = useRef(false);
  const bootstrapPendente =
    appMode === null && Boolean(ler_bootstrap_dossie_da_url());

  useEffect(() => {
    if (bootstrapAplicadoRef.current) {
      return;
    }

    bootstrapAplicadoRef.current = true;

    const bootstrap = ler_bootstrap_dossie_da_url();
    if (bootstrap) {
      if (bootstrap.modoAplicacao) {
        setAppMode(bootstrap.modoAplicacao);
      }
      if (bootstrap.abaAtiva) {
        setActiveTab(bootstrap.abaAtiva);
      }
      if (bootstrap.cnpjSelecionado) {
        setSelectedCnpj(bootstrap.cnpjSelecionado);
      }
      limpar_bootstrap_dossie_da_url();
    }
  }, [setActiveTab, setAppMode, setSelectedCnpj]);

  const { data: cnpjs = [] } = useQuery({
    queryKey: ["cnpjs"],
    queryFn: cnpjApi.list,
  });

  const selectedRecord =
    cnpjs.find((registro) => registro.cnpj === selectedCnpj) ?? null;

  if (bootstrapPendente) {
    return (
      <div className="h-screen w-screen" style={{ background: "#0a1628" }} />
    );
  }

  if (appMode === null) {
    return <LandingPage onSelect={setAppMode} />;
  }

  if (appMode === "fisconforme") {
    return (
      <div
        className="flex h-screen overflow-hidden"
        style={{ background: "#0a1628" }}
      >
        <div className="flex flex-1 flex-col overflow-hidden">
          <div
            className="flex items-center justify-between border-b border-slate-700 px-3 py-1"
            style={{ background: "#0d1f3c", minHeight: 32 }}
          >
            <span className="text-xs font-semibold text-slate-400">
              Fisconforme — Análise em Lote
            </span>
            <button
              onClick={() => setAppMode(null)}
              className="rounded border border-slate-700 bg-slate-800 px-2 py-1 text-xs text-blue-400 hover:text-blue-200"
            >
              ← Voltar ao Início
            </button>
          </div>
          <div className="flex-1 overflow-hidden">
            <FisconformeTab />
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className="flex h-screen overflow-hidden"
      style={{ background: "#0a1628" }}
    >
      {leftPanelVisible && <LeftPanel />}

      <div className="flex flex-1 flex-col overflow-hidden">
        <div
          className="flex items-center justify-between border-b border-slate-700 px-3 py-1"
          style={{ background: "#0d1f3c", minHeight: 32 }}
        >
          <div className="flex min-w-0 items-center gap-2 overflow-hidden text-xs text-slate-400">
            {selectedCnpj && (
              <>
                <button
                  onClick={() => setActiveTab("dossie")}
                  className={`font-mono shrink-0 rounded px-2 py-0.5 transition-colors ${
                    activeTab === "dossie"
                      ? "bg-blue-600 text-white"
                      : "bg-slate-700 text-slate-300 hover:bg-slate-600"
                  }`}
                  title="Ir para o Dossiê"
                >
                  {selectedCnpj}
                </button>
                {selectedRecord?.razao_social && (
                  <span
                    className="max-w-[280px] truncate text-slate-400"
                    title={selectedRecord.razao_social}
                  >
                    | {selectedRecord.razao_social}
                  </span>
                )}
                {selectedRecord?.nome_fantasia &&
                  selectedRecord.nome_fantasia !==
                    selectedRecord.razao_social && (
                    <span
                      className="max-w-[200px] truncate text-slate-500"
                      title={selectedRecord.nome_fantasia}
                    >
                      ({selectedRecord.nome_fantasia})
                    </span>
                  )}
                {selectedFile && (
                  <span className="shrink-0 truncate text-slate-500">
                    | {selectedFile.name}
                  </span>
                )}
              </>
            )}
          </div>
          <button
            onClick={toggleLeftPanel}
            className="rounded border border-slate-700 bg-slate-800 px-2 py-1 text-xs text-blue-400 hover:text-blue-200"
          >
            {leftPanelVisible
              ? "<< Ocultar Painel Lateral"
              : ">> Mostrar Painel Lateral"}
          </button>
          <button
            onClick={() => setAppMode(null)}
            className="ml-2 rounded border border-slate-700 bg-slate-800 px-2 py-1 text-xs text-slate-400 hover:text-slate-200"
          >
            ← Início
          </button>
        </div>

        <div
          role="tablist"
          className="flex items-center gap-1 border-b border-slate-700 px-3 pt-2"
          style={{ background: "#0a1628" }}
        >
          {TABS.map((tab) => (
            <button
              key={tab.id}
              role="tab"
              aria-selected={activeTab === tab.id}
              aria-controls={`panel-${tab.id}`}
              id={`tab-${tab.id}`}
              onClick={() => setActiveTab(tab.id)}
              className={`rounded-t border-l border-r border-t px-4 py-1.5 text-xs font-medium transition-colors ${
                activeTab === tab.id
                  ? "border-slate-600 text-white"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}
              style={activeTab === tab.id ? { background: "#0f1b33" } : {}}
            >
              {tab.label}
            </button>
          ))}
        </div>

        <div
          role="tabpanel"
          id={`panel-${activeTab}`}
          aria-labelledby={`tab-${activeTab}`}
          className="flex-1 overflow-hidden"
          style={{ background: "#0a1628" }}
        >
          {activeTab === "dossie" && (
            <DossieTab
              cnpj={selectedCnpj}
              razaoSocial={selectedRecord?.razao_social}
            />
          )}
          {activeTab === "consulta" && <ConsultaTab />}
          {activeTab === "sql" && <ConsultaSqlTab />}
          {activeTab === "efd" && <EfdTab />}
          {activeTab === "documentos-fiscais" && <DocumentosFiscaisTab />}
          {activeTab === "fiscalizacao" && <FiscalizacaoTab />}
          {activeTab === "analise-fiscal" && <AnaliseFiscalTab />}
          {activeTab === "agregacao" && <AgregacaoTab />}
          {activeTab === "conversao" && <ConversaoTab />}
          {activeTab === "estoque" && <EstoqueTab />}
          {activeTab === "logs" && <LogsTab />}
        </div>
      </div>
    </div>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <MainContent />
    </QueryClientProvider>
  );
}
