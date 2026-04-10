import { useAppStore } from "../../../store/appStore";
import { FiscalPageShell } from "../shared/FiscalPageShell";

const atalhosLegados = [
  {
    id: "estoque",
    title: "Cruzamentos",
    description:
      "A aba de Estoque permanece ativa como legado e passa a ser tratada como parte dos cruzamentos fiscais.",
    cta: "Abrir Estoque legado",
  },
  {
    id: "agregacao",
    title: "Verificações de Agregação",
    description:
      "Agregação continua disponível como legado e passa a ocupar a frente de verificações do módulo fiscal.",
    cta: "Abrir Agregação legada",
  },
  {
    id: "conversao",
    title: "Verificações de Conversão",
    description:
      "Conversão continua disponível como legado e passa a compor a camada de verificações estruturais.",
    cta: "Abrir Conversão legada",
  },
] as const;

export function AnaliseFiscalTab() {
  const setActiveTab = useAppStore((state) => state.setActiveTab);

  return (
    <FiscalPageShell
      title="Análise Fiscal"
      subtitle="Cruzamentos, verificações e classificação dos produtos."
    >
      <div className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="text-sm font-semibold text-white">
            Núcleo analítico do novo módulo fiscal
          </div>
          <div className="mt-3 grid gap-3 md:grid-cols-3">
            <div className="rounded-xl border border-slate-700 bg-slate-950/40 p-3">
              <div className="text-sm font-medium text-white">Cruzamentos</div>
              <p className="mt-2 text-sm text-slate-400">
                Relacionamentos entre EFD, documentos fiscais, XML, fronteira, Sitafe e demais
                fontes estruturadas.
              </p>
            </div>
            <div className="rounded-xl border border-slate-700 bg-slate-950/40 p-3">
              <div className="text-sm font-medium text-white">Verificações</div>
              <p className="mt-2 text-sm text-slate-400">
                Regras de consistência, integridade, equivalência de unidades, agregação e
                coerência fiscal.
              </p>
            </div>
            <div className="rounded-xl border border-slate-700 bg-slate-950/40 p-3">
              <div className="text-sm font-medium text-white">
                Classificação dos Produtos
              </div>
              <p className="mt-2 text-sm text-slate-400">
                Catálogo mestre, pendências de classificação, conflitos e enriquecimento do
                cadastro analítico de produtos.
              </p>
            </div>
          </div>
        </section>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="text-sm font-semibold text-white">Compatibilidade temporária</div>
          <p className="mt-2 text-sm text-slate-400">
            Durante a migração, as abas antigas continuam acessíveis. Os atalhos abaixo ajudam a
            navegar enquanto os datasets canônicos e as telas novas são materializados.
          </p>
          <div className="mt-4 space-y-3">
            {atalhosLegados.map((atalho) => (
              <div
                key={atalho.id}
                className="rounded-xl border border-slate-700 bg-slate-950/40 p-3"
              >
                <div className="text-sm font-medium text-white">{atalho.title}</div>
                <p className="mt-2 text-sm text-slate-400">{atalho.description}</p>
                <button
                  onClick={() => setActiveTab(atalho.id)}
                  className="mt-3 rounded-lg border border-slate-600 bg-slate-800 px-3 py-2 text-xs font-medium text-slate-200 hover:bg-slate-700"
                >
                  {atalho.cta}
                </button>
              </div>
            ))}
          </div>
        </section>
      </div>
    </FiscalPageShell>
  );
}

export default AnaliseFiscalTab;
