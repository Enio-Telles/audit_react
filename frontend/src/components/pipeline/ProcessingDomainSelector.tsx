import type { ReactNode } from "react";

export interface ProcessingDomainPreset {
  id: string;
  title: string;
  subtitle: string;
  tables: string[];
  description: string;
}

interface Props {
  selectedTables: string[] | null;
  onChange: (tables: string[] | null) => void;
}

const PRESETS: ProcessingDomainPreset[] = [
  {
    id: "all",
    title: "Pipeline analítico completo",
    subtitle: "Reprocessa toda a camada de transformação disponível.",
    tables: [],
    description: "Usa todas as tabelas de processamento já conhecidas pelo backend.",
  },
  {
    id: "documentos",
    title: "Documentos base",
    subtitle: "Recompõe a consolidação documental primária.",
    tables: ["tb_documentos"],
    description: "Útil quando a base documental foi alterada e você quer reconstruir a consolidação antes das demais camadas.",
  },
  {
    id: "efd",
    title: "EFD enriquecido",
    subtitle: "Regera as camadas derivadas de C170/C176 com apoio do XML.",
    tables: ["c170_xml", "c176_xml"],
    description: "Útil quando você ajustou regras de interpretação do EFD ou do vínculo com XML.",
  },
  {
    id: "produtos",
    title: "Produtos e classificação",
    subtitle: "Reconstrói identidade, agrupamentos e fatores de conversão.",
    tables: ["item_unidades", "itens", "descricao_produtos", "produtos_final", "fontes_produtos", "fatores_conversao"],
    description: "Útil quando mudou a lógica de agrupamento, normalização ou conversão de unidades.",
  },
  {
    id: "estoque",
    title: "Estoque e análises",
    subtitle: "Recalcula movimentação, mensal e anual.",
    tables: ["movimentacao_estoque", "calculos_mensais", "calculos_anuais"],
    description: "Útil quando a base já está pronta e você quer atualizar cruzamentos e saldos finais.",
  },
];

function sameSelection(a: string[] | null, b: string[] | null): boolean {
  if (a === null && b === null) return true;
  if (a === null || b === null) return false;
  if (a.length !== b.length) return false;
  const sa = [...a].sort();
  const sb = [...b].sort();
  return sa.every((item, index) => item === sb[index]);
}

function selectedLabel(tables: string[] | null): ReactNode {
  if (tables === null) return <>todas as tabelas analíticas</>;
  if (tables.length === 0) return <>nenhuma tabela</>;
  return <>{tables.length} tabela(s) selecionada(s)</>;
}

export function ProcessingDomainSelector({ selectedTables, onChange }: Props) {
  return (
    <div className="space-y-2 rounded-xl border border-slate-700 bg-slate-950/30 p-2">
      <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-400">Reprocessamento seletivo por domínio</div>
      <div className="text-[11px] text-slate-500">Disponível no modo <span className="text-slate-300">Somente Processamento</span>. O frontend usa os IDs reais de `tabelas` já suportados pelo backend.</div>
      <div className="grid gap-2">
        {PRESETS.map((preset) => {
          const target = preset.id === "all" ? null : preset.tables;
          const active = sameSelection(selectedTables, target);
          return (
            <button
              key={preset.id}
              type="button"
              onClick={() => onChange(target)}
              className={`rounded-xl border p-3 text-left transition-colors ${active ? "border-blue-500 bg-blue-950/25" : "border-slate-700 bg-slate-900/30 hover:bg-slate-900/50"}`}
            >
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold text-white">{preset.title}</div>
                  <div className="mt-1 text-xs text-slate-400">{preset.subtitle}</div>
                </div>
                <div className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${active ? "bg-blue-500/20 text-blue-200" : "bg-slate-800 text-slate-400"}`}>{active ? "Selecionado" : "Selecionar"}</div>
              </div>
              <div className="mt-2 text-[11px] text-slate-300">{preset.description}</div>
              {preset.tables.length > 0 ? (
                <div className="mt-2 flex flex-wrap gap-1.5">
                  {preset.tables.map((table) => (
                    <span key={table} className="rounded-full border border-slate-700 bg-slate-950/50 px-2 py-0.5 text-[10px] text-slate-300">{table}</span>
                  ))}
                </div>
              ) : (
                <div className="mt-2 text-[10px] text-slate-500">Inclui a cadeia analítica completa.</div>
              )}
            </button>
          );
        })}
      </div>
      <div className="rounded-xl border border-slate-800 bg-slate-950/40 px-3 py-2 text-[11px] text-slate-400">
        <span className="font-semibold uppercase tracking-wide text-slate-500">Resumo:</span> vai reprocessar {selectedLabel(selectedTables)}.
      </div>
    </div>
  );
}

export default ProcessingDomainSelector;
