import { FiscalPageShell } from "../shared/FiscalPageShell";

export function FiscalizacaoTab() {
  return (
    <FiscalPageShell
      title="Fiscalização"
      subtitle="Fronteira, Fisconforme, malhas, chaves e resoluções."
    >
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="text-sm font-semibold text-white">Fronteira</div>
          <p className="mt-2 text-sm text-slate-400">
            Área prevista para consolidação de eventos, passagens, rotas e sinais de fiscalização
            relacionados à fronteira.
          </p>
        </section>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="text-sm font-semibold text-white">Fisconforme</div>
          <p className="mt-2 text-sm text-slate-400">
            O modo de análise em lote continua disponível na entrada específica do sistema.
            Esta aba passa a representar o domínio fiscal consolidado para leitura e auditoria.
          </p>
        </section>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="text-sm font-semibold text-white">Malhas e Pendências</div>
          <p className="mt-2 text-sm text-slate-400">
            Espaço reservado para datasets canônicos de inconsistências, pendências abertas,
            sinais de risco e itens priorizados para investigação.
          </p>
        </section>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
          <div className="text-sm font-semibold text-white">Chaves</div>
          <p className="mt-2 text-sm text-slate-400">
            Estrutura futura para chaves fiscais, rastreamento de documentos e vínculo entre
            fontes distintas do ecossistema fiscal.
          </p>
        </section>

        <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4 md:col-span-2 xl:col-span-1">
          <div className="text-sm font-semibold text-white">Resoluções</div>
          <p className="mt-2 text-sm text-slate-400">
            Área reservada para histórico de resoluções, encaminhamentos e desfechos de ações
            fiscais com rastreabilidade de origem.
          </p>
        </section>
      </div>
    </FiscalPageShell>
  );
}

export default FiscalizacaoTab;
