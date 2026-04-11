import React from "react";

type TreePayload = {
  doc_key: string;
  documents: Array<{
    document: Record<string, unknown>;
    items_c170: Record<string, unknown>[];
    summary_c190: Record<string, unknown>[];
    links_c176: Record<string, unknown>[];
    adjustments_c197: Record<string, unknown>[];
  }>;
};

export function EfdTreePanel({ data }: { data: TreePayload | null }) {
  return (
    <div className="rounded border bg-white p-3">
      <div className="mb-3 text-sm font-semibold">Árvore documental EFD</div>
      {!data || data.documents.length === 0 ? (
        <div className="text-sm text-slate-500">Nenhum documento encontrado para o filtro atual.</div>
      ) : (
        <div className="space-y-3">
          {data.documents.map((node, index) => (
            <div key={`${index}-${String(node.document[data.doc_key])}`} className="rounded border p-3">
              <div className="mb-2 text-sm font-medium">
                Documento: {String(node.document[data.doc_key] ?? "sem chave")}
              </div>
              <div className="grid gap-3 md:grid-cols-4">
                <MiniList title="C170 Itens" items={node.items_c170} />
                <MiniList title="C190 Resumo" items={node.summary_c190} />
                <MiniList title="C176 Vínculos" items={node.links_c176} />
                <MiniList title="C197 Ajustes" items={node.adjustments_c197} />
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function MiniList({ title, items }: { title: string; items: Record<string, unknown>[] }) {
  return (
    <div className="rounded border p-2">
      <div className="mb-2 text-xs font-semibold text-slate-600">{title}</div>
      <div className="max-h-56 overflow-auto space-y-2">
        {items.length === 0 ? <div className="text-xs text-slate-400">Sem registros.</div> : null}
        {items.map((item, idx) => (
          <pre key={idx} className="overflow-auto rounded bg-slate-50 p-2 text-[11px]">
            {JSON.stringify(item, null, 2)}
          </pre>
        ))}
      </div>
    </div>
  );
}
