import { type ReactNode, useEffect, useRef } from "react";
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
import { EstoqueTab } from "./components/tabs/EstoqueTab";
import { FisconformeTab } from "./components/tabs/FisconformeTab";
import { LogsTab } from "./components/tabs/LogsTab";
import { DossieTab } from "./features/dossie/components/DossieTab";
import {
  ler_bootstrap_dossie_da_url,
  limpar_bootstrap_dossie_da_url,
} from "./features/dossie/navegacao";
import {
  type FiscalDomain,
  useAppStore,
} from "./store/appStore";

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: 1, staleTime: 30_000 } },
});

const DOMAIN_CONFIG: Array<{
  id: FiscalDomain;
  label: string;
  items: Array<{ id: string; label: string }>;
}> = [
  {
    id: "efd",
    label: "EFD",
    items: [
      { id: "bloco-0", label: "Bloco 0 (Cadastros)" },
      { id: "bloco-c", label: "Bloco C (Documentos)" },
      { id: "bloco-h", label: "Bloco H (Inventário)" },
    ],
  },
  {
    id: "documentos-fiscais",
    label: "Documentos Fiscais",
    items: [
      { id: "nfe-emissao-propria", label: "NF-e Emissão Própria" },
      { id: "cte-transportes", label: "CT-e Transportes" },
      { id: "fisconforme", label: "Fisconforme" },
      { id: "sitafe-fronteira", label: "Sitafe / Fronteira" },
    ],
  },
  {
    id: "analise-fiscal",
    label: "Análise Fiscal",
    items: [
      { id: "cruzamento-nfe-efd", label: "Cruzamento NF-e x EFD" },
      { id: "estoque-mensal", label: "Estoque Mensal" },
      { id: "estoque-anual", label: "Estoque Anual" },
      { id: "icms-devido", label: "ICMS devido por competência" },
      {
        id: "produtos-inconsistentes",
        label: "Produtos com inconsistências",
      },
      { id: "ressarcimento-st", label: "Ressarcimento ST" },
    ],
  },
];

const TECH_ITEMS = [
  { id: "configuracao-acervo", label: "Configuração & Acervo" },
  { id: "consulta-sql", label: "Consulta SQL" },
  { id: "logs", label: "Logs" },
];

function SubMenuButton({
  active,
  label,
  onClick,
  compact = false,
}: {
  active: boolean;
  label: string;
  onClick: () => void;
  compact?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      className={`w-full rounded-lg px-3 py-2 text-left text-sm transition-colors ${
        active
          ? "bg-slate-800 text-white"
          : "text-slate-400 hover:bg-slate-800/70 hover:text-white"
      } ${compact ? "text-xs" : ""}`}
    >
      {label}
    </button>
  );
}

function FiscalWorkspaceFrame({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: ReactNode;
}) {
  return (
    <div className="flex h-full flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-sm">
      <div className="border-b border-slate-200 bg-slate-50 px-5 py-4">
        <div className="text-sm font-semibold text-slate-900">{title}</div>
        <div className="mt-1 text-xs text-slate-500">{subtitle}</div>
      </div>
      <div className="min-h-0 flex-1 overflow-hidden">{children}</div>
    </div>
  );
}

function EmptyContextState({ onGoTechnical }: { onGoTechnical: () => void }) {
  return (
    <div className="mx-auto flex h-full max-w-3xl items-center justify-center px-6">
      <div className="w-full rounded-2xl border border-slate-200 bg-white p-8 shadow-sm">
        <div className="text-lg font-semibold text-slate-900">
          Selecione um CNPJ para iniciar a análise
        </div>
        <div className="mt-2 text-sm text-slate-600">
          Nesta primeira etapa da refatoração, a seleção de CNPJ e os controles
          operacionais continuam disponíveis em <strong>Configuração &amp; Acervo</strong>,
          enquanto o shell principal já foi simplificado para os três blocos
          fiscais.
        </div>
        <button
          onClick={onGoTechnical}
          className="mt-5 rounded-lg border border-slate-300 bg-slate-100 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-200"
        >
          Abrir Configuração &amp; Acervo
        </button>
      </div>
    </div>
  );
}

function TechnicalWorkspace({
  activeSubItem,
  setActiveSubItem,
  leftPanelVisible,
  toggleLeftPanel,
}: {
  activeSubItem: string;
  setActiveSubItem: (value: string) => void;
  leftPanelVisible: boolean;
  toggleLeftPanel: () => void;
}) {
  const technicalContent =
    activeSubItem === "consulta-sql" ? (
      <ConsultaSqlTab />
    ) : activeSubItem === "logs" ? (
      <LogsTab />
    ) : (
      <div className="flex h-full min-h-0 gap-4 overflow-hidden p-4">
        {leftPanelVisible ? (
          <div className="overflow-hidden rounded-2xl border border-slate-200">
            <LeftPanel />
          </div>
        ) : null}
        <div className="flex min-w-0 flex-1 flex-col gap-4 overflow-auto rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
          <div>
            <div className="text-sm font-semibold text-slate-900">
              Configuração &amp; Acervo
            </div>
            <div className="mt-1 text-xs text-slate-500">
              Painel técnico preservado para seleção de CNPJ, pipeline,
              materialização, catálogo de datasets e controles operacionais.
            </div>
          </div>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-xs text-slate-600">
              Seleção de CNPJ, data de corte e extração continuam disponíveis no
              painel técnico legado.
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-xs text-slate-600">
              O objetivo desta etapa é isolar a engenharia do fluxo principal do
              auditor, sem perder capacidade operacional.
            </div>
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-xs text-slate-600">
              Use esta área para consultas SQL, inspeção de logs, catálogo e
              manutenção do acervo.
            </div>
          </div>
          <button
            onClick={toggleLeftPanel}
            className="w-fit rounded-lg border border-slate-300 bg-slate-100 px-3 py-2 text-xs font-medium text-slate-700 hover:bg-slate-200"
          >
            {leftPanelVisible ? "Ocultar painel técnico" : "Mostrar painel técnico"}
          </button>
        </div>
      </div>
    );

  return (
    <div className="flex h-full min-h-0 gap-4 p-4">
      <aside className="w-64 shrink-0 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
        <div className="text-xs font-semibold uppercase tracking-wide text-slate-500">
          Manutenção / T.I.
        </div>
        <div className="mt-3 space-y-2">
          {TECH_ITEMS.map((item) => (
            <SubMenuButton
              key={item.id}
              active={activeSubItem === item.id}
              label={item.label}
              compact
              onClick={() => setActiveSubItem(item.id)}
            />
          ))}
        </div>
      </aside>
      <div className="min-w-0 flex-1 overflow-hidden rounded-2xl border border-slate-200 bg-slate-50 shadow-sm">
        {technicalContent}
      </div>
    </div>
  );
}

function MainContent() {
  const {
    activeDomain,
    setActiveDomain,
    activeSubItem,
    setActiveSubItem,
    leftPanelVisible,
    toggleLeftPanel,
    selectedFile,
    selectedCnpj,
    setSelectedCnpj,
    appMode,
    setAppMode,
    activeTab,
    setActiveTab,
    contextMode,
    setContextMode,
    contextPeriodo,
    setContextPeriodo,
    efdCutoffDate,
    setEfdCutoffDate,
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
      if (bootstrap.cnpjSelecionado) {
        setSelectedCnpj(bootstrap.cnpjSelecionado);
        setContextMode("cnpj");
      }
      if (bootstrap.abaAtiva === "dossie") {
        setActiveDomain("analise-fiscal");
        setActiveSubItem("dossie-contextual");
        setActiveTab("dossie");
      }
      limpar_bootstrap_dossie_da_url();
    }
  }, [
    setActiveDomain,
    setActiveSubItem,
    setActiveTab,
    setAppMode,
    setContextMode,
    setSelectedCnpj,
  ]);

  useEffect(() => {
    if (activeTab === "sql") {
      setActiveDomain("configuracao-acervo");
      setActiveSubItem("consulta-sql");
      return;
    }
    if (activeTab === "logs") {
      setActiveDomain("configuracao-acervo");
      setActiveSubItem("logs");
      return;
    }
    if (activeTab === "dossie") {
      setActiveDomain("analise-fiscal");
      setActiveSubItem("dossie-contextual");
      return;
    }
    if (activeTab === "estoque") {
      setActiveDomain("analise-fiscal");
      setActiveSubItem("estoque-mensal");
    }
  }, [activeTab, setActiveDomain, setActiveSubItem]);

  const { data: cnpjs = [] } = useQuery({
    queryKey: ["cnpjs"],
    queryFn: cnpjApi.list,
  });

  const selectedRecord =
    cnpjs.find((registro) => registro.cnpj === selectedCnpj) ?? null;

  const currentDomainConfig = DOMAIN_CONFIG.find(
    (domain) => domain.id === activeDomain,
  );
  const currentSubLabel =
    currentDomainConfig?.items.find((item) => item.id === activeSubItem)?.label ??
    TECH_ITEMS.find((item) => item.id === activeSubItem)?.label ??
    activeSubItem;

  const handleDomainChange = (domain: FiscalDomain) => {
    setActiveDomain(domain);
    if (domain === "configuracao-acervo") {
      setActiveSubItem("configuracao-acervo");
      setActiveTab("consulta");
      return;
    }

    const firstItem = DOMAIN_CONFIG.find((item) => item.id === domain)?.items[0];
    if (firstItem) {
      setActiveSubItem(firstItem.id);
    }

    if (domain === "analise-fiscal") {
      setActiveTab("estoque");
      return;
    }

    setActiveTab("consulta");
  };

  const handleSubItemChange = (domain: FiscalDomain, itemId: string) => {
    setActiveDomain(domain);
    setActiveSubItem(itemId);

    if (itemId === "dossie-contextual") {
      setActiveTab("dossie");
      return;
    }
    if (domain === "configuracao-acervo") {
      setActiveTab(
        itemId === "logs"
          ? "logs"
          : itemId === "consulta-sql"
            ? "sql"
            : "consulta",
      );
      return;
    }
    if (domain === "analise-fiscal" && itemId.startsWith("estoque")) {
      setActiveTab("estoque");
      return;
    }

    setActiveTab("consulta");
  };

  const renderAuditView = () => {
    if (activeDomain !== "configuracao-acervo" && !selectedCnpj) {
      return (
        <EmptyContextState
          onGoTechnical={() => handleDomainChange("configuracao-acervo")}
        />
      );
    }

    if (activeDomain === "configuracao-acervo") {
      return (
        <TechnicalWorkspace
          activeSubItem={activeSubItem}
          setActiveSubItem={(value) =>
            handleSubItemChange("configuracao-acervo", value)
          }
          leftPanelVisible={leftPanelVisible}
          toggleLeftPanel={toggleLeftPanel}
        />
      );
    }

    if (activeSubItem === "dossie-contextual") {
      return (
        <FiscalWorkspaceFrame
          title="Dossiê contextual"
          subtitle="Recurso contextual preservado durante a simplificação do shell principal."
        >
          <DossieTab
            cnpj={selectedCnpj}
            razaoSocial={selectedRecord?.razao_social}
          />
        </FiscalWorkspaceFrame>
      );
    }

    if (activeDomain === "efd") {
      return (
        <FiscalWorkspaceFrame
          title={currentSubLabel}
          subtitle="EFD inicial organizada em Bloco 0, Bloco C e Bloco H, sem Bloco K nesta primeira onda."
        >
          <ConsultaTab />
        </FiscalWorkspaceFrame>
      );
    }

    if (activeDomain === "documentos-fiscais") {
      if (activeSubItem === "fisconforme") {
        return (
          <div className="mx-auto flex h-full max-w-4xl items-center justify-center px-6">
            <div className="w-full rounded-2xl border border-slate-200 bg-white p-8 shadow-sm">
              <div className="text-lg font-semibold text-slate-900">
                Fisconforme continua preservado como modo próprio
              </div>
              <div className="mt-2 text-sm text-slate-600">
                A navegação simplificada por CNPJ já expõe o item no shell novo,
                mas o fluxo operacional completo de lote, acervo e notificações
                continua na trilha dedicada de análise em lote.
              </div>
              <button
                onClick={() => setAppMode("fisconforme")}
                className="mt-5 rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-500"
              >
                Abrir análise em lote / Fisconforme
              </button>
            </div>
          </div>
        );
      }

      return (
        <FiscalWorkspaceFrame
          title={currentSubLabel}
          subtitle="Leitura documental focada em tabela, filtros rápidos e detalhamento complementar por linha."
        >
          <ConsultaTab />
        </FiscalWorkspaceFrame>
      );
    }

    if (activeDomain === "analise-fiscal") {
      if (
        activeSubItem === "estoque-mensal" ||
        activeSubItem === "estoque-anual" ||
        activeSubItem === "icms-devido" ||
        activeSubItem === "produtos-inconsistentes"
      ) {
        return (
          <FiscalWorkspaceFrame
            title={currentSubLabel}
            subtitle="Área analítica em transição para o novo shell, preservando a capacidade tabular legada durante a migração."
          >
            <EstoqueTab />
          </FiscalWorkspaceFrame>
        );
      }

      if (activeSubItem === "ressarcimento-st") {
        return (
          <FiscalWorkspaceFrame
            title={currentSubLabel}
            subtitle="Visão analítica preservada no shell novo enquanto o contrato tabular final é consolidado."
          >
            <AgregacaoTab />
          </FiscalWorkspaceFrame>
        );
      }

      return (
        <FiscalWorkspaceFrame
          title={currentSubLabel}
          subtitle="Workbench analítico orientado a contexto, com foco em cruzamentos e leitura tabular."
        >
          <ConsultaTab />
        </FiscalWorkspaceFrame>
      );
    }

    return null;
  };

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
    <div className="flex h-screen overflow-hidden bg-slate-100">
      <aside className="flex w-72 shrink-0 flex-col border-r border-slate-800 bg-slate-950 text-slate-300 shadow-xl">
        <div className="border-b border-slate-800 px-4 py-4">
          <div className="text-base font-semibold text-white">
            Fiscal Parquet Analyzer
          </div>
          <div className="mt-1 text-xs text-slate-500">
            Workbench fiscal simplificado
          </div>
        </div>

        <div className="flex-1 overflow-auto px-3 py-4">
          <div className="mb-2 px-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500">
            Área Fiscal
          </div>
          <div className="space-y-3">
            {DOMAIN_CONFIG.map((domain) => {
              const isActive = activeDomain === domain.id;
              return (
                <div key={domain.id} className="rounded-xl border border-slate-800 bg-slate-900/50 p-2">
                  <button
                    onClick={() => handleDomainChange(domain.id)}
                    className={`w-full rounded-lg px-3 py-2 text-left text-sm font-medium transition-colors ${
                      isActive
                        ? "bg-slate-800 text-white"
                        : "text-slate-300 hover:bg-slate-800 hover:text-white"
                    }`}
                  >
                    {domain.label}
                  </button>
                  {isActive ? (
                    <div className="mt-2 space-y-1">
                      {domain.items.map((item) => (
                        <SubMenuButton
                          key={item.id}
                          active={activeSubItem === item.id}
                          label={item.label}
                          onClick={() => handleSubItemChange(domain.id, item.id)}
                          compact
                        />
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>

        <div className="border-t border-slate-800 px-3 py-4">
          <div className="mb-2 px-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500">
            Manutenção / T.I.
          </div>
          <SubMenuButton
            active={activeDomain === "configuracao-acervo"}
            label="Configuração & Acervo"
            onClick={() => handleDomainChange("configuracao-acervo")}
            compact
          />
        </div>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <header className="border-b border-slate-200 bg-white px-6 py-3">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <div className="text-xs font-medium uppercase tracking-wide text-slate-400">
                Contexto global
              </div>
              <div className="mt-1 flex flex-wrap items-center gap-2 text-sm text-slate-600">
                <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-medium text-slate-700">
                  {contextMode === "cnpj" ? "CNPJ" : "Lote"}
                </span>
                <span className="font-mono text-slate-900">
                  {selectedCnpj ?? "Nenhum CNPJ selecionado"}
                </span>
                {selectedRecord?.razao_social ? (
                  <span className="max-w-[320px] truncate text-slate-500">
                    — {selectedRecord.razao_social}
                  </span>
                ) : null}
                {selectedFile ? (
                  <span className="max-w-[220px] truncate text-slate-400">
                    arquivo: {selectedFile.name}
                  </span>
                ) : null}
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2">
              <label className="flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                <span>Período</span>
                <input
                  type="month"
                  value={contextPeriodo}
                  onChange={(event) => setContextPeriodo(event.target.value)}
                  className="border-0 bg-transparent p-0 text-xs text-slate-700 outline-none"
                />
              </label>
              <label className="flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-600">
                <span>Corte EFD</span>
                <input
                  type="date"
                  value={efdCutoffDate}
                  onChange={(event) => setEfdCutoffDate(event.target.value)}
                  className="border-0 bg-transparent p-0 text-xs text-slate-700 outline-none"
                />
              </label>
              {selectedCnpj ? (
                <button
                  onClick={() => handleSubItemChange("analise-fiscal", "dossie-contextual")}
                  className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-medium text-slate-700 hover:bg-slate-50"
                >
                  Dossiê contextual
                </button>
              ) : null}
              <button
                onClick={() => setAppMode(null)}
                className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-xs font-medium text-slate-700 hover:bg-slate-50"
              >
                ← Início
              </button>
            </div>
          </div>

          <div className="mt-3 flex flex-wrap items-center gap-2 text-xs text-slate-500">
            <span>{DOMAIN_CONFIG.find((domain) => domain.id === activeDomain)?.label ?? "Configuração & Acervo"}</span>
            <span>›</span>
            <span className="font-medium text-slate-700">{currentSubLabel}</span>
          </div>
        </header>

        <main className="min-h-0 flex-1 overflow-hidden p-4">{renderAuditView()}</main>
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
