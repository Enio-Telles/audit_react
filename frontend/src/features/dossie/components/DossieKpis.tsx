import { useMemo } from "react";
import type { DossieSectionSummary } from "../types";
import { formatarDataAtualizacao } from "../utils/dossie_helpers";

// ⚡ Bolt Optimization: Use cached Intl.NumberFormat instance instead of Number.prototype.toLocaleString()
const intlInteger = new Intl.NumberFormat("pt-BR");

interface DossieKpisProps {
  sections: DossieSectionSummary[];
}

interface KpiCardProps {
  label: string;
  value: string | number;
  sublabel?: string;
  variant?: "default" | "success" | "warning" | "error";
}

function KpiCard({
  label,
  value,
  sublabel,
  variant = "default",
}: KpiCardProps) {
  const variantClasses: Record<string, string> = {
    default: "border-slate-700 bg-slate-900/60",
    success: "border-green-800/60 bg-green-950/30",
    warning: "border-amber-700/60 bg-amber-950/30",
    error: "border-rose-800/60 bg-rose-950/30",
  };
  const valueClasses: Record<string, string> = {
    default: "text-white",
    success: "text-green-300",
    warning: "text-amber-300",
    error: "text-rose-300",
  };

  return (
    <div className={`rounded-xl border p-3 ${variantClasses[variant]}`}>
      <div className="text-[11px] uppercase tracking-wide text-slate-400">
        {label}
      </div>
      <div
        className={`mt-1 text-xl font-bold tabular-nums ${valueClasses[variant]}`}
      >
        {value}
      </div>
      {sublabel && (
        <div className="mt-0.5 text-[11px] text-slate-500">{sublabel}</div>
      )}
    </div>
  );
}

export function DossieKpis({ sections }: DossieKpisProps) {
  const kpis = useMemo(() => {
    // ⚡ Bolt Optimization: Single O(N) pass instead of 5 separate loops & intermediate arrays
    let atualizadas = 0;
    let pendentes = 0;
    let comErro = 0;
    let totalLinhas = 0;
    let comDivergencia = 0;
    let ultimaAtualizacao: string | undefined = undefined;

    for (const s of sections) {
      if (s.status === "cached" || s.status === "fresh") {
        atualizadas++;
      } else if (s.status === "idle") {
        pendentes++;
      } else if (s.status === "error") {
        comErro++;
      }

      totalLinhas += s.rowCount ?? 0;

      if (
        s.alternateStrategyComparison === "divergencia_funcional" ||
        s.alternateStrategyComparison === "divergencia_basica"
      ) {
        comDivergencia++;
      }

      if (s.updatedAt) {
        if (!ultimaAtualizacao || s.updatedAt > ultimaAtualizacao) {
          ultimaAtualizacao = s.updatedAt;
        }
      }
    }

    return {
      total: sections.length,
      atualizadas,
      pendentes,
      comErro,
      totalLinhas,
      comDivergencia,
      ultimaAtualizacao,
    };
  }, [sections]);

  const dataFormatada = formatarDataAtualizacao(kpis.ultimaAtualizacao);

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
      <KpiCard label="Total" value={kpis.total} />
      <KpiCard
        label="Atualizadas"
        value={kpis.atualizadas}
        sublabel={`de ${kpis.total}`}
        variant={
          kpis.atualizadas > 0 && kpis.atualizadas === kpis.total
            ? "success"
            : "default"
        }
      />
      <KpiCard
        label="Pendentes"
        value={kpis.pendentes}
        variant={kpis.pendentes > 0 ? "warning" : "default"}
      />
      <KpiCard
        label="Com erro"
        value={kpis.comErro}
        variant={kpis.comErro > 0 ? "error" : "default"}
      />
      <KpiCard
        label="Linhas carregadas"
        value={intlInteger.format(kpis.totalLinhas)}
      />
      {kpis.comDivergencia > 0 ? (
        <KpiCard
          label="Divergencias"
          value={kpis.comDivergencia}
          variant="warning"
        />
      ) : dataFormatada ? (
        <KpiCard
          label="Ultima atualizacao"
          value={dataFormatada}
          sublabel="secao mais recente"
        />
      ) : (
        <KpiCard label="Divergencias" value={0} />
      )}
    </div>
  );
}
