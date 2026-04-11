import type { DatasetAvailabilityItem } from "../../features/fiscal/types";
import type { ExtractionApproach } from "./ExtractionApproachSelector";

interface Props {
  cnpj: string | null;
  items: DatasetAvailabilityItem[];
  loading?: boolean;
  selectedApproach: ExtractionApproach;
  onApplySuggestion: (approach: ExtractionApproach) => void;
  onOpenCatalog: () => void;
}

const RAW_BASE_IDS = ["tb_documentos", "c170_xml", "c176_xml", "bloco_h", "nfe_base", "nfce_base", "cte_base", "dados_cadastrais", "malhas"] as const;
const ANALYTIC_IDS = ["mov_estoque", "aba_mensal", "aba_anual", "fatores_conversao", "produtos_agrupados", "produtos_final"] as const;

function summarize(ids: readonly string[], items: DatasetAvailabilityItem[]) {
  const available = ids.filter((id) => items.some((item) => item.dataset_id === id && item.disponivel));
  const missing = ids.filter((id) => !available.includes(id));
  return { available, missing };
}

function deriveSuggestion(items: DatasetAvailabilityItem[]) {
  const raw = summarize(RAW_BASE_IDS, items);
  const analytic = summarize(ANALYTIC_IDS, items);

  if (raw.available.length === 0) {
    return {
      approach: "full" as ExtractionApproach,
      tone: "amber" as const,
      title: "Sugestão automática: Extrair + Processar",
      description: "Ainda não há datasets-base suficientes materializados para reaproveitar o processamento com segurança.",
      details: `Base encontrada: nenhuma. Analíticos encontrados: ${analytic.available.length}.`,
    };
  }

  if (raw.available.length > 0 && analytic.available.length === 0) {
    return {
      approach: "process" as ExtractionApproach,
      tone: "blue" as const,
      title: "Sugestão automática: Somente Processamento",
      description: "Há base materializada e ainda não há camadas analíticas suficientes. Faz sentido reprocessar antes de extrair novamente.",
      details: `Base encontrada: ${raw.available.join(", ")}.`,
    };
  }

  if (raw.missing.length >= 4) {
    return {
      approach: "full" as ExtractionApproach,
      tone: "amber" as const,
      title: "Sugestão automática: Extrair + Processar",
      description: "Existe material parcial, mas ainda faltam vários datasets-base. A abordagem completa tende a ser a mais consistente.",
      details: `Base ausente: ${raw.missing.slice(0, 5).join(", ")}${raw.missing.length > 5 ? "..." : ""}.`,
    };
  }

  return {
    approach: "process" as ExtractionApproach,
    tone: "emerald" as const,
    title: "Sugestão automática: Somente Processamento",
    description: "A base materializada já parece suficiente para reaproveitar o pipeline analítico. Use extração completa só quando SQL, período ou fonte tiverem mudado.",
    details: `Base encontrada: ${raw.available.length}. Analíticos encontrados: ${analytic.available.length}.`,
  };
}

function toneClasses(tone: "amber" | "blue" | "emerald") {
  if (tone === "amber") return "border-amber-500/40 bg-amber-950/20 text-amber-100";
  if (tone === "blue") return "border-blue-500/40 bg-blue-950/20 text-blue-100";
  return "border-emerald-500/40 bg-emerald-950/20 text-emerald-100";
}

export function ExtractionReadinessBanner({ cnpj, items, loading = false, selectedApproach, onApplySuggestion, onOpenCatalog }: Props) {
  if (!cnpj) {
    return <div className="rounded-xl border border-slate-800 bg-slate-950/30 p-3 text-[11px] text-slate-500">Informe ou selecione um CNPJ para o frontend sugerir a abordagem mais adequada com base no catálogo materializado.</div>;
  }

  if (loading) {
    return <div className="rounded-xl border border-slate-800 bg-slate-950/30 p-3 text-[11px] text-slate-400">Verificando materialização do catálogo para <span className="font-mono text-slate-200">{cnpj}</span>...</div>;
  }

  const suggestion = deriveSuggestion(items);
  const raw = summarize(RAW_BASE_IDS, items);
  const weakProcess = selectedApproach === "process" && raw.available.length < 2;

  return (
    <div className="space-y-2">
      <div className={`rounded-xl border p-3 text-[11px] ${toneClasses(suggestion.tone)}`}>
        <div className="text-xs font-semibold uppercase tracking-wide">{suggestion.title}</div>
        <div className="mt-1 text-sm font-medium">{suggestion.description}</div>
        <div className="mt-1 opacity-90">{suggestion.details}</div>
        <div className="mt-3 flex flex-wrap gap-2">
          <button type="button" onClick={() => onApplySuggestion(suggestion.approach)} className="rounded-lg border border-white/15 bg-black/10 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-black/20">Usar abordagem sugerida</button>
          <button type="button" onClick={onOpenCatalog} className="rounded-lg border border-white/15 bg-black/10 px-3 py-1.5 text-[11px] font-semibold text-white hover:bg-black/20">Abrir catálogo do CNPJ</button>
        </div>
      </div>

      {weakProcess ? (
        <div className="rounded-xl border border-rose-500/40 bg-rose-950/20 p-3 text-[11px] text-rose-100">
          <div className="text-xs font-semibold uppercase tracking-wide">Atenção ao modo “Somente Processamento”</div>
          <div className="mt-1">O catálogo localizou pouca base materializada para este CNPJ. O processamento isolado pode não produzir o resultado esperado.</div>
          <div className="mt-1 opacity-90">Base encontrada: {raw.available.length ? raw.available.join(", ") : "nenhuma"}.</div>
        </div>
      ) : null}
    </div>
  );
}

export default ExtractionReadinessBanner;
