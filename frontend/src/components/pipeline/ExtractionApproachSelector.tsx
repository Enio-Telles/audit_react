import type { ReactNode } from "react";

export type ExtractionApproach = "full" | "extract" | "process";

interface ApproachCard {
  id: ExtractionApproach;
  title: string;
  subtitle: string;
  example: string;
  details: string[];
}

interface Props {
  selectedApproach: ExtractionApproach;
  onSelect: (approach: ExtractionApproach) => void;
  consultasResumo: string;
  dataLimite: string;
}

const CARDS: ApproachCard[] = [
  {
    id: "full",
    title: "Extrair + Processar",
    subtitle: "Atualiza a base bruta e em seguida recompõe as camadas analíticas.",
    example: "Exemplo: mudou SQL, data limite ou base Oracle e você quer sair já com datasets prontos para análise.",
    details: [
      "Usa consultas SQL selecionadas.",
      "Usa data limite da extração.",
      "Entrega brutos + analíticos no mesmo fluxo.",
    ],
  },
  {
    id: "extract",
    title: "Somente Extração",
    subtitle: "Busca novamente tabelas brutas e materializa Parquet/Delta sem recalcular análises.",
    example: "Exemplo: quer validar o resultado bruto da consulta antes de rodar estoque, cruzamentos e classificações.",
    details: [
      "Usa consultas SQL selecionadas.",
      "Usa data limite da extração.",
      "Não roda a camada analítica depois.",
    ],
  },
  {
    id: "process",
    title: "Somente Processamento",
    subtitle: "Reaproveita arquivos já materializados e recalcula as camadas analíticas.",
    example: "Exemplo: você alterou regras do pipeline e quer reprocessar sem bater de novo no banco.",
    details: [
      "Ignora consultas SQL da extração.",
      "Ignora data limite.",
      "Reusa material já existente no CNPJ.",
    ],
  },
];

function Box({ label, tone }: { label: string; tone: "blue" | "emerald" | "amber" }) {
  const toneCls =
    tone === "blue"
      ? "fill-blue-500/25 stroke-blue-400"
      : tone === "emerald"
        ? "fill-emerald-500/25 stroke-emerald-400"
        : "fill-amber-500/25 stroke-amber-400";
  return (
    <g>
      <rect x="0" y="0" width="82" height="28" rx="8" className={toneCls} strokeWidth="1.5" />
      <text x="41" y="18" textAnchor="middle" className="fill-slate-200 text-[9px] font-medium">
        {label}
      </text>
    </g>
  );
}

function Arrow({ x1, y1, x2, y2 }: { x1: number; y1: number; x2: number; y2: number }) {
  return (
    <g>
      <line x1={x1} y1={y1} x2={x2} y2={y2} stroke="currentColor" strokeWidth="1.5" className="text-slate-500" />
      <path d={`M ${x2 - 6} ${y2 - 4} L ${x2} ${y2} L ${x2 - 6} ${y2 + 4}`} fill="none" stroke="currentColor" strokeWidth="1.5" className="text-slate-500" />
    </g>
  );
}

function ApproachImage({ approach }: { approach: ExtractionApproach }) {
  if (approach === "extract") {
    return (
      <svg viewBox="0 0 260 78" className="h-20 w-full rounded-xl border border-slate-800 bg-slate-950/40 p-2">
        <g transform="translate(6,20)"><Box label="Oracle / SQL" tone="blue" /></g>
        <Arrow x1={92} y1={34} x2={130} y2={34} />
        <g transform="translate(136,20)"><Box label="Parquet / Delta" tone="emerald" /></g>
        <text x="130" y="68" textAnchor="middle" className="fill-slate-500 text-[10px]">Extração bruta</text>
      </svg>
    );
  }

  if (approach === "process") {
    return (
      <svg viewBox="0 0 260 78" className="h-20 w-full rounded-xl border border-slate-800 bg-slate-950/40 p-2">
        <g transform="translate(6,20)"><Box label="Parquet / Delta" tone="emerald" /></g>
        <Arrow x1={92} y1={34} x2={130} y2={34} />
        <g transform="translate(136,20)"><Box label="Análises" tone="amber" /></g>
        <text x="130" y="68" textAnchor="middle" className="fill-slate-500 text-[10px]">Reprocessamento</text>
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 260 78" className="h-20 w-full rounded-xl border border-slate-800 bg-slate-950/40 p-2">
      <g transform="translate(2,20)"><Box label="Oracle / SQL" tone="blue" /></g>
      <Arrow x1={88} y1={34} x2={122} y2={34} />
      <g transform="translate(126,20)"><Box label="Parquet / Delta" tone="emerald" /></g>
      <Arrow x1={212} y1={34} x2={244} y2={34} />
      <g transform="translate(244,20) scale(0.82)"><Box label="Análises" tone="amber" /></g>
      <text x="130" y="68" textAnchor="middle" className="fill-slate-500 text-[10px]">Fluxo completo</text>
    </svg>
  );
}

function DetailList({ items }: { items: string[] }) {
  return (
    <div className="mt-2 space-y-1 text-[11px] text-slate-400">
      {items.map((item) => (
        <div key={item}>• {item}</div>
      ))}
    </div>
  );
}

export function ExtractionApproachSelector({
  selectedApproach,
  onSelect,
  consultasResumo,
  dataLimite,
}: Props) {
  const summary: Record<ExtractionApproach, ReactNode> = {
    full: (
      <>
        Vai usar <span className="text-slate-200">consultas {consultasResumo}</span> e data limite <span className="text-slate-200">{dataLimite}</span>, depois segue para processamento.
      </>
    ),
    extract: (
      <>
        Vai usar <span className="text-slate-200">consultas {consultasResumo}</span> e data limite <span className="text-slate-200">{dataLimite}</span>, mas para antes da camada analítica.
      </>
    ),
    process: (
      <>
        Vai reaproveitar <span className="text-slate-200">material já existente</span> e ignorar data limite e consultas SQL.
      </>
    ),
  };

  return (
    <div className="space-y-2 rounded-xl border border-slate-700 bg-slate-950/30 p-2">
      <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-400">Abordagem de execução</div>
      <div className="grid gap-2">
        {CARDS.map((card) => {
          const active = card.id === selectedApproach;
          return (
            <button
              key={card.id}
              type="button"
              onClick={() => onSelect(card.id)}
              className={`rounded-xl border p-3 text-left transition-colors ${
                active
                  ? "border-blue-500 bg-blue-950/25"
                  : "border-slate-700 bg-slate-900/30 hover:bg-slate-900/50"
              }`}
            >
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-sm font-semibold text-white">{card.title}</div>
                  <div className="mt-1 text-xs text-slate-400">{card.subtitle}</div>
                </div>
                <div className={`rounded-full px-2 py-0.5 text-[10px] font-semibold ${active ? "bg-blue-500/20 text-blue-200" : "bg-slate-800 text-slate-400"}`}>
                  {active ? "Selecionada" : "Selecionar"}
                </div>
              </div>
              <div className="mt-3"><ApproachImage approach={card.id} /></div>
              <div className="mt-2 text-[11px] text-slate-300">{card.example}</div>
              <DetailList items={card.details} />
            </button>
          );
        })}
      </div>
      <div className="rounded-xl border border-slate-800 bg-slate-950/40 px-3 py-2 text-[11px] text-slate-400">
        <span className="font-semibold uppercase tracking-wide text-slate-500">Resumo da seleção:</span>{" "}
        {summary[selectedApproach]}
      </div>
    </div>
  );
}

export default ExtractionApproachSelector;
