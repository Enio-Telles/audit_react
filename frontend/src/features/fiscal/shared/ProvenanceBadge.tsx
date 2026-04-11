interface ProvenanceBadgeProps {
  /** Dataset identifier e.g. "c170_xml", "bloco_h" */
  datasetId: string;
  /** Data layer: "raw", "base", "curated", "mart", "legado" */
  camada?: string;
  /** Optional source path for tooltip */
  sourcePath?: string;
}

const CAMADA_STYLES: Record<string, { border: string; bg: string; text: string }> = {
  raw: { border: "border-emerald-700/60", bg: "bg-emerald-950/30", text: "text-emerald-300" },
  base: { border: "border-blue-700/60", bg: "bg-blue-950/30", text: "text-blue-300" },
  curated: { border: "border-violet-700/60", bg: "bg-violet-950/30", text: "text-violet-300" },
  mart: { border: "border-amber-700/60", bg: "bg-amber-950/30", text: "text-amber-300" },
  legado: { border: "border-slate-600", bg: "bg-slate-800", text: "text-slate-300" },
};

const DEFAULT_STYLE = CAMADA_STYLES.legado;

export function ProvenanceBadge({ datasetId, camada, sourcePath }: ProvenanceBadgeProps) {
  const layer = camada?.toLowerCase() ?? "legado";
  const style = CAMADA_STYLES[layer] ?? DEFAULT_STYLE;

  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[11px] font-medium tracking-wide ${style.border} ${style.bg} ${style.text}`}
      title={
        sourcePath
          ? `Origem: ${sourcePath}\nCamada: ${layer}\nDataset: ${datasetId}`
          : `Camada: ${layer} · Dataset: ${datasetId}`
      }
    >
      <span className="inline-block h-1.5 w-1.5 rounded-full bg-current opacity-70" />
      <span className="uppercase">{layer}</span>
      <span className="opacity-50">·</span>
      <span className="font-mono">{datasetId}</span>
    </span>
  );
}

export default ProvenanceBadge;
