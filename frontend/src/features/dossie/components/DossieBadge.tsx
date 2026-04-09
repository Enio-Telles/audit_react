interface DossieBadgeProps {
  rotulo: string;
  valor?: string | number | null;
  variante?: "neutra" | "info" | "sucesso" | "alerta" | "erro";
  className?: string;
}

function obter_classes_variante(variante: DossieBadgeProps["variante"]): string {
  switch (variante) {
    case "info":
      return "border-sky-800/70 bg-sky-950/30 text-sky-200";
    case "sucesso":
      return "border-emerald-800/70 bg-emerald-950/30 text-emerald-200";
    case "alerta":
      return "border-amber-800/70 bg-amber-950/30 text-amber-200";
    case "erro":
      return "border-rose-800/70 bg-rose-950/30 text-rose-200";
    default:
      return "border-slate-700 bg-slate-950/70 text-slate-300";
  }
}

export function DossieBadge({
  rotulo,
  valor,
  variante = "neutra",
  className = "",
}: DossieBadgeProps) {
  return (
    <span
      className={`inline-flex items-center gap-1 rounded-full border px-2.5 py-1 text-[11px] ${obter_classes_variante(variante)} ${className}`.trim()}
    >
      <span className="text-slate-400">{rotulo}:</span>
      <span className="font-medium">{valor ?? "-"}</span>
    </span>
  );
}

export default DossieBadge;
