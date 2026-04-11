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
import { DossieTab } from "./features/dossie/components/DossieTab";
import { AgregacaoFiscalTab } from "./features/fiscal/agregacao/AgregacaoFiscalTab";
import { AnaliseFiscalTab } from "./features/fiscal/analise/AnaliseFiscalTab";
import { CatalogoDatasetsTab } from "./features/fiscal/catalogo/CatalogoDatasetsTab";
import { ConversaoFiscalTab } from "./features/fiscal/conversao/ConversaoFiscalTab";
import { DocumentosFiscaisTab } from "./features/fiscal/documentos_fiscais/DocumentosFiscaisTab";
import { EfdTab } from "./features/fiscal/efd/EfdTab";
import { EstoqueFiscalTab } from "./features/fiscal/estoque/EstoqueFiscalTab";
import { FiscalizacaoTab } from "./features/fiscal/fiscalizacao/FiscalizacaoTab";
import { ProdutoMasterTab } from "./features/fiscal/produto_master/ProdutoMasterTab";
import { RessarcimentoTab } from "./features/fiscal/ressarcimento/RessarcimentoTab";
import { lerBootstrapFiscalDaUrl } from "./features/fiscal/navigation";
import { useAppStore } from "./store/appStore";

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 30_000 } },
});

const USER_TABS = [
  { id: "efd", label: "EFD" },
  { id: "documentos-fiscais", label: "Documentos Fiscais" },
  { id: "analise-fiscal", label: "Analise Fiscal" },
];

const MAINTENANCE_TABS = [
  { id: "dossie", label: "Dossie" },
  { id: "consulta", label: "Consulta" },
  { id: "sql", label: "Consulta SQL" },
  { id: "produto-master", label: "Produto Master" },
  { id: "fiscal-agregacao", label: "Agregacao" },
  { id: "fiscal-conversao", label: "Conversao" },
  { id: "fiscal-estoque", label: "Estoque" },
  { id: "fiscalizacao", label: "Fiscalizacao" },
  { id: "ressarcimento", label: "Ressarcimento" },
  { id: "catalogo-datasets", label: "Catalogo Datasets" },
  { id: "agregacao", label: "Agregacao (legado)" },
  { id: "conversao", label: "Conversao (legado)" },
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
    workspaceSection,
    setWorkspaceSection,
  } = useAppStore();
  const bootstrapAplicadoRef = useRef(false);
  const bootstrapPendente =
    appMode === null && Boolean(lerBootstrapFiscalDaUrl());

  useEffect(() => {
    if (bootstrapAplicadoRef.current) {
      return;
    }

    bootstrapAplicadoRef.current = true;

    const bootstrap = lerBootstrapFiscalDaUrl();
    if (bootstrap) {
      if (bootstrap.modoAplicacao) {
        setAppMode(bootstrap.modoAplicacao);
      }
      if (bootstrap.abaAtiva) {
        setActiveTab(bootstrap.abaAtiva);
        if (USER_TABS.some((tab) => tab.id === bootstrap.abaAtiva)) {
          setWorkspaceSection("usuario");
        } else {
          setWorkspaceSection("manutencao");
        }
      }
      if (bootstrap.cnpjSelecionado) {
        setSelectedCnpj(bootstrap.cnpjSelecionado);
      }
    }
  }, [setActiveTab, setAppMode, setSelectedCnpj, setWorkspaceSection]);

  const { data: cnpjs = [] } = useQuery({
    queryKey: ["cnpjs"],
    queryFn: cnpjApi.list,
  });

  const selectedRecord =
    cnpjs.find((registro) => registro.cnpj === selectedCnpj) ?? null;
  const visibleTabs =
    workspaceSection === "usuario" ? USER_TABS : MAINTENANCE_TABS;

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
              Fisconforme - Analise em Lote
            </span>
            <button
              onClick={() => setAppMode(null)}
              className="rounded border border-slate-700 bg-slate-800 px-2 py-1 text-xs text-blue-400 hover:text-blue-200"
            >
              Voltar ao Inicio
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
                  onClick={() => {
                    setWorkspaceSection("manutencao");
                    setActiveTab("dossie");
                  }}
                  className={`shrink-0 rounded px-2 py-0.5 font-mono transition-colors ${
                    activeTab === "dossie"
                      ? "bg-blue-600 text-white"
                      : "bg-slate-700 text-slate-300 hover:bg-slate-600"
                  }`}
                  title="Ir para o Dossie"
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
          <div className="flex items-center gap-2">
            <button
              onClick={toggleLeftPanel}
              className="rounded border border-slate-700 bg-slate-800 px-2 py-1 text-xs text-blue-400 hover:text-blue-200"
            >
              {leftPanelVisible ? "Ocultar painel" : "Mostrar painel"}
            </button>
            <button
              onClick={() => setAppMode(null)}
              className="rounded border border-slate-700 bg-slate-800 px-2 py-1 text-xs text-slate-400 hover:text-slate-200"
            >
              Inicio
            </button>
          </div>
        </div>

        <div
          role="tablist"
          className="flex items-center gap-1 border-b border-slate-700 px-3 pt-2"
          style={{ background: "#0a1628" }}
        >
          <div className="mr-4 flex items-center gap-1">
            <button
              type="button"
              onClick={() => {
                setWorkspaceSection("usuario");
                if (!USER_TABS.some((tab) => tab.id === activeTab)) {
                  setActiveTab("efd");
                }
              }}
              className={`rounded-t border-l border-r border-t px-4 py-1.5 text-xs font-medium transition-colors ${
                workspaceSection === "usuario"
                  ? "border-slate-600 text-white"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}
              style={
                workspaceSection === "usuario"
                  ? { background: "#10223a" }
                  : {}
              }
            >
              Usuario
            </button>
            <button
              type="button"
              onClick={() => {
                setWorkspaceSection("manutencao");
                if (!MAINTENANCE_TABS.some((tab) => tab.id === activeTab)) {
                  setActiveTab("dossie");
                }
              }}
              className={`rounded-t border-l border-r border-t px-4 py-1.5 text-xs font-medium transition-colors ${
                workspaceSection === "manutencao"
                  ? "border-slate-600 text-white"
                  : "border-transparent text-slate-400 hover:text-slate-200"
              }`}
              style={
                workspaceSection === "manutencao"
                  ? { background: "#10223a" }
                  : {}
              }
            >
              Manutencao
            </button>
          </div>

          {visibleTabs.map((tab) => (
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
          {activeTab === "fiscal-agregacao" && <AgregacaoFiscalTab />}
          {activeTab === "produto-master" && <ProdutoMasterTab />}
          {activeTab === "fiscal-conversao" && <ConversaoFiscalTab />}
          {activeTab === "fiscal-estoque" && <EstoqueFiscalTab />}
          {activeTab === "documentos-fiscais" && <DocumentosFiscaisTab />}
          {activeTab === "fiscalizacao" && <FiscalizacaoTab />}
          {activeTab === "analise-fiscal" && <AnaliseFiscalTab />}
          {activeTab === "ressarcimento" && <RessarcimentoTab />}
          {activeTab === "catalogo-datasets" && <CatalogoDatasetsTab />}
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
