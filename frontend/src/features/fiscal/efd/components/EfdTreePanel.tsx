import type { EfdDocumentTreeResponse } from "../../api";

interface EfdTreePanelProps {
  data: EfdDocumentTreeResponse | null;
}

function MiniList({ title, items }: { title: string; items: Record<string, unknown>[] }) {
  return (
    <div className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
      <div className="mb-2 text-xs font-semibold text-slate-300">{title}</div>
      <div className="max-h-56 space-y-2 overflow-auto">
        {items.length === 0 ? (
          <div className="text-xs text-slate-500">Sem registros.</div>
        ) : (
          items.map((item, index) => (
            <pre key={index} className="overflow-auto rounded-lg border border-slate-800 bg-slate-950/60 p-2 text-[11px] text-slate-300">
              {JSON.stringify(item, null, 2)}
            </pre>
          ))
        )}
      </div>
    </div>
  );
}

export function EfdTreePanel({ data }: EfdTreePanelProps) {
  return (
    <section className="rounded-2xl border border-slate-700 bg-slate-900/30 p-4">
      <div className="mb-3 text-sm font-semibold text-white">Arvore documental EFD</div>
      {!data || data.documents.length === 0 ? (
        <div className="rounded-xl border border-slate-800 bg-slate-950/30 px-4 py-6 text-sm text-slate-500">
          Nenhum documento encontrado para o filtro atual.
        </div>
      ) : (
        <div className="space-y-3">
          {data.documents.map((node, index) => (
            <div key={`${index}-${String(node.document[data.doc_key] ?? "sem-chave")}`} className="rounded-xl border border-slate-800 bg-slate-950/40 p-3">
              <div className="mb-2 text-sm font-medium text-white">
                Documento: {String(node.document[data.doc_key] ?? "sem chave")}
              </div>
              <div className="grid gap-3 xl:grid-cols-4">
                <MiniList title="C170 Itens" items={node.items_c170} />
                <MiniList title="C190 Resumo" items={node.summary_c190} />
                <MiniList title="C176 Vinculos" items={node.links_c176} />
                <MiniList title="C197 Ajustes" items={node.adjustments_c197} />
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}

export default EfdTreePanel;
