import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";

import { useAppStore } from "../../../store/appStore";
import { fiscalFeatureApi } from "../api";
import { FiscalDomainOverview } from "../shared/FiscalDomainOverview";
import { FiscalPageShell } from "../shared/FiscalPageShell";

export function AnaliseFiscalTab() {
  const selectedCnpj = useAppStore((state) => state.selectedCnpj);
  const setActiveTab = useAppStore((state) => state.setActiveTab);
  const setWorkspaceSection = useAppStore((state) => state.setWorkspaceSection);

  const summaryQuery = useQuery({
    queryKey: ["fiscal", "analise", "resumo", selectedCnpj ?? "sem-cnpj"],
    queryFn: () => fiscalFeatureApi.getAnaliseResumo(selectedCnpj),
  });

  const analysisModules = useMemo(
    () => [
      {
        id: "fiscal-estoque",
        title: "Estoque",
        description:
          "Eventos, saldos, inventario declarado e divergencias do estoque fiscal.",
      },
      {
        id: "fiscal-conversao",
        title: "Conversao",
        description:
          "Fatores de conversao, origem do fator final e evidencias de unidade.",
      },
      {
        id: "fiscal-agregacao",
        title: "Agregacao",
        description:
          "Identidade de produto, grupos de merge, conflitos e evidencias.",
      },
      {
        id: "produto-master",
        title: "Produto Master",
        description:
          "Catalogo base, agrupamentos e classificacao fiscal dos produtos.",
      },
      {
        id: "ressarcimento",
        title: "Ressarcimento",
        description:
          "Itens, conciliacao e visao mensal do ressarcimento ST.",
      },
    ],
    [],
  );

  return (
    <FiscalPageShell
      title="Analise Fiscal"
      subtitle="Area do usuario para cruzamentos fiscais e verificacoes complexas, sem poluir a leitura com detalhes tecnicos de manutencao."
    >
      <div className="space-y-4">
        <FiscalDomainOverview
          data={summaryQuery.data}
          isLoading={summaryQuery.isLoading}
          errorMessage={
            summaryQuery.error instanceof Error
              ? summaryQuery.error.message
              : undefined
          }
          onOpenShortcut={setActiveTab}
        />

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-3 text-sm font-semibold text-white">
            Modulos analiticos
          </div>
          <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
            {analysisModules.map((module) => (
              <article
                key={module.id}
                className="rounded-xl border border-slate-800 bg-slate-950/40 p-3"
              >
                <div className="text-sm font-medium text-white">
                  {module.title}
                </div>
                <div className="mt-2 text-xs text-slate-400">
                  {module.description}
                </div>
                <button
                  type="button"
                  onClick={() => {
                    setWorkspaceSection("manutencao");
                    setActiveTab(module.id);
                  }}
                  className="mt-3 rounded-lg border border-slate-700 bg-slate-800 px-3 py-2 text-xs text-slate-200 hover:bg-slate-700"
                >
                  Abrir modulo
                </button>
              </article>
            ))}
          </div>
        </section>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="mb-3 text-sm font-semibold text-white">
            Separacao funcional
          </div>
          <div className="grid gap-3 lg:grid-cols-3">
            <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3 text-xs text-slate-400">
              <div className="font-medium text-slate-200">EFD</div>
              Escrituracao pura. Deve seguir os blocos do Guia Pratico da EFD
              e permanecer separada dos cruzamentos com outros documentos.
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3 text-xs text-slate-400">
              <div className="font-medium text-slate-200">
                Documentos Fiscais
              </div>
              Area de leitura documental para NF-e, NFC-e, CT-e, sinais do
              Fisconforme e fronteira.
            </div>
            <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3 text-xs text-slate-400">
              <div className="font-medium text-slate-200">Analise Fiscal</div>
              Espaco de cruzamentos, verificacoes e conciliacoes. Os modulos
              detalhados ficam separados para preservar clareza de leitura.
            </div>
          </div>
        </section>
      </div>
    </FiscalPageShell>
  );
}

export default AnaliseFiscalTab;
