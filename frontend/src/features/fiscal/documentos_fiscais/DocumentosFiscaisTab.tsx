import { FiscalPageShell } from "../shared/FiscalPageShell";

export function DocumentosFiscaisTab() {
  return (
    <FiscalPageShell
      title="Documentos Fiscais"
      subtitle="NF-e, NFC-e, CT-e, informações complementares e contatos."
    >
      <div className="rounded-2xl border border-dashed border-slate-700 bg-slate-900/20 p-4 text-sm text-slate-400">
        Estrutura inicial criada. Esta área passará a consumir datasets canônicos em Parquet,
        em vez de depender de SQL acoplado diretamente à tela.
      </div>
    </FiscalPageShell>
  );
}

export default DocumentosFiscaisTab;
