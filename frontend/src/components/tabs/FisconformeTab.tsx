import { useEffect, useMemo, useRef, useState } from "react";
import type { InputHTMLAttributes, ReactNode } from "react";

import { fisconformeApi } from "../../api/client";
import { abrir_dossie_em_nova_aba } from "../../features/dossie/navegacao";
import type {
  AuditorConfig,
  FisconformeConsultaResult,
  FisconformeDsfRecord,
  FisconformeDsfSummary,
  GerarNotificacaoRequest,
  MalhaRecord,
} from "../../api/types";

type Step = "home" | "input" | "results" | "auditor";
type ConsultaMode = "single" | "lote";
type FiltroResultados = "todos" | "com_pendencia" | "sem_pendencia" | "com_erro";

interface ResumoCnpjsConsulta {
  totalInformado: number;
  totalValidos: number;
  totalInvalidos: number;
  totalDuplicados: number;
  cnpjsValidos: string[];
  cnpjsInvalidos: string[];
}

interface FisconformeDraft {
  id: string | null;
  dsf: string;
  referencia: string;
  mode: ConsultaMode;
  cnpjSingle: string;
  cnpjsText: string;
  dataInicio: string;
  dataFim: string;
  forcar: boolean;
  outputDir: string;
  auditorData: AuditorConfig;
  pdfStoredAvailable: boolean;
  pdfStoredName: string;
}

const EMPTY_AUDITOR_FORM: AuditorConfig = {
  auditor: "",
  cargo_titulo: "",
  matricula: "",
  contato: "",
  orgao_origem: "",
};

const EMPTY_DRAFT: FisconformeDraft = {
  id: null,
  dsf: "",
  referencia: "",
  mode: "lote",
  cnpjSingle: "",
  cnpjsText: "",
  dataInicio: "01/2021",
  dataFim: "12/2025",
  forcar: false,
  outputDir: "",
  auditorData: EMPTY_AUDITOR_FORM,
  pdfStoredAvailable: false,
  pdfStoredName: "",
};

const MONTH_LABELS = [
  "jan",
  "fev",
  "mar",
  "abr",
  "mai",
  "jun",
  "jul",
  "ago",
  "set",
  "out",
  "nov",
  "dez",
];

// ⚡ Bolt Optimization: Use cached Intl.DateTimeFormat instance instead of Date.prototype.toLocaleString()
// This avoids repeatedly allocating locale data and parsing options on every render, improving performance.
const intlDateTime = new Intl.DateTimeFormat("pt-BR", {
  dateStyle: "short",
  timeStyle: "medium",
});

function Card({
  children,
  className = "",
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={`rounded-2xl border border-slate-700/80 bg-slate-900/50 p-5 shadow-[0_18px_60px_rgba(2,6,23,0.28)] backdrop-blur ${className}`}
    >
      {children}
    </div>
  );
}

function Label({ children }: { children: ReactNode }) {
  return (
    <label className="mb-1 block text-xs font-medium text-slate-400">
      {children}
    </label>
  );
}

function Input(props: InputHTMLAttributes<HTMLInputElement>) {
  return (
    <input
      {...props}
      className={`w-full rounded-lg border border-slate-700 bg-slate-800/80 px-3 py-2 text-xs text-white outline-none transition-colors placeholder:text-slate-500 focus:border-sky-500 ${props.className ?? ""}`}
    />
  );
}

function SecondaryBtn({
  onClick,
  disabled,
  children,
  className = "",
}: {
  onClick: () => void;
  disabled?: boolean;
  children: ReactNode;
  className?: string;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`rounded-lg border border-slate-600 bg-slate-800/80 px-4 py-2 text-xs font-medium text-slate-200 transition-colors hover:border-slate-500 hover:text-white disabled:cursor-not-allowed disabled:opacity-40 ${className}`}
    >
      {children}
    </button>
  );
}

function PrimaryBtn({
  onClick,
  disabled,
  loading,
  children,
  className = "",
}: {
  onClick: () => void;
  disabled?: boolean;
  loading?: boolean;
  children: ReactNode;
  className?: string;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled || loading}
      className={`rounded-lg bg-sky-600 px-4 py-2 text-xs font-semibold text-white transition-colors hover:bg-sky-500 disabled:cursor-not-allowed disabled:opacity-40 ${className}`}
    >
      {loading ? "Aguarde..." : children}
    </button>
  );
}

function formatReference(value: string): string {
  if (!value) return "sem referência";

  let month = 0;
  let year = "";

  if (/^\d{4}-\d{2}$/.test(value)) {
    const [parsedYear, parsedMonth] = value.split("-");
    year = parsedYear;
    month = Number(parsedMonth);
  } else if (/^\d{2}\/\d{4}$/.test(value)) {
    const [parsedMonth, parsedYear] = value.split("/");
    year = parsedYear;
    month = Number(parsedMonth);
  } else {
    return value;
  }

  if (!year || month < 1 || month > 12) return value;
  return `${MONTH_LABELS[month - 1]}/${year}`;
}

function formatDateTime(value: string): string {
  if (!value) return "-";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return intlDateTime.format(parsed);
}

function buildDsfHeadline(
  item: Pick<FisconformeDsfSummary, "dsf" | "referencia" | "auditor">,
): string {
  const prefix = item.dsf ? `DSF n. ${item.dsf}` : "DSF sem número";
  const reference = formatReference(item.referencia);
  const auditor = item.auditor || "Auditor não informado";
  return `${prefix} (${reference}) - ${auditor}`;
}

function normalizeDraftCnpjs(draft: FisconformeDraft): string[] {
  if (draft.mode === "single") {
    return draft.cnpjSingle.trim() ? [draft.cnpjSingle.trim()] : [];
  }

  return draft.cnpjsText
    .split(/[\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function normalizar_cnpj_lote(cnpj: string): string {
  return cnpj.replace(/\D/g, "");
}

function validar_cnpj_lote(cnpj: string): boolean {
  return normalizar_cnpj_lote(cnpj).length === 14;
}

function resumir_cnpjs_do_rascunho(draft: FisconformeDraft): ResumoCnpjsConsulta {
  const entradas = normalizeDraftCnpjs(draft).map(normalizar_cnpj_lote).filter(Boolean);
  const contagemPorCnpj = new Map<string, number>();

  for (const cnpj of entradas) {
    contagemPorCnpj.set(cnpj, (contagemPorCnpj.get(cnpj) ?? 0) + 1);
  }

  const cnpjsValidos = Array.from(new Set(entradas.filter(validar_cnpj_lote)));
  const cnpjsInvalidos = Array.from(new Set(entradas.filter((cnpj) => !validar_cnpj_lote(cnpj))));
  const totalDuplicados = Array.from(contagemPorCnpj.values()).reduce((acumulado, quantidade) => {
    return quantidade > 1 ? acumulado + (quantidade - 1) : acumulado;
  }, 0);

  return {
    totalInformado: entradas.length,
    totalValidos: cnpjsValidos.length,
    totalInvalidos: cnpjsInvalidos.length,
    totalDuplicados,
    cnpjsValidos,
    cnpjsInvalidos,
  };
}

function obter_resultados_filtrados(
  resultados: FisconformeConsultaResult[],
  filtro: FiltroResultados,
): FisconformeConsultaResult[] {
  if (filtro === "com_erro") {
    return resultados.filter((item) => Boolean(item.error));
  }

  if (filtro === "com_pendencia") {
    return resultados.filter((item) => !item.error && (item.malhas?.length ?? 0) > 0);
  }

  if (filtro === "sem_pendencia") {
    return resultados.filter((item) => !item.error && (item.malhas?.length ?? 0) === 0);
  }

  return resultados;
}

async function fileToBase64(file: File | null): Promise<string | undefined> {
  if (!file) return undefined;

  return new Promise<string>((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const dataUrl = String(reader.result || "");
      resolve(dataUrl.split(",")[1] ?? "");
    };
    reader.onerror = () => reject(reader.error);
    reader.readAsDataURL(file);
  });
}

function downloadBlob(blob: Blob, fileName: string) {
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = fileName;
  anchor.click();
  URL.revokeObjectURL(url);
}

function extractFileName(
  contentDisposition: string | undefined,
  fallback: string,
): string {
  if (!contentDisposition) return fallback;
  const match = contentDisposition.match(/filename="?([^"]+)"?/i);
  return match?.[1] ?? fallback;
}

function buildDraftFromRecord(
  record: FisconformeDsfRecord,
  auditorFallback: AuditorConfig,
): FisconformeDraft {
  const cnpjs = record.cnpjs ?? [];
  const isSingle = cnpjs.length <= 1;

  return {
    id: record.id,
    dsf: record.dsf,
    referencia: record.referencia,
    mode: isSingle ? "single" : "lote",
    cnpjSingle: isSingle ? (cnpjs[0] ?? "") : "",
    cnpjsText: isSingle ? "" : cnpjs.join("\n"),
    dataInicio: record.data_inicio,
    dataFim: record.data_fim,
    forcar: record.forcar_atualizacao,
    outputDir: record.output_dir,
    auditorData: {
      auditor: record.auditor || auditorFallback.auditor,
      cargo_titulo: record.cargo_titulo || auditorFallback.cargo_titulo,
      matricula: record.matricula || auditorFallback.matricula,
      contato: record.contato || auditorFallback.contato,
      orgao_origem: record.orgao_origem || auditorFallback.orgao_origem,
    },
    pdfStoredAvailable: record.pdf_disponivel,
    pdfStoredName: record.pdf_file_name,
  };
}

function HomeStep({
  dsfs,
  loading,
  error,
  onRefresh,
  onNew,
  onOpen,
}: {
  dsfs: FisconformeDsfSummary[];
  loading: boolean;
  error: string;
  onRefresh: () => void;
  onNew: () => void;
  onOpen: (id: string) => void;
}) {
  const totals = useMemo(() => {
    const cnpjs = dsfs.reduce((sum, item) => sum + item.cnpjs_count, 0);
    return { dsfs: dsfs.length, cnpjs };
  }, [dsfs]);

  return (
    <div className="mx-auto flex h-full w-full max-w-6xl flex-col gap-6 overflow-y-auto px-6 py-8">
      <Card className="overflow-hidden bg-[radial-gradient(circle_at_top_left,_rgba(56,189,248,0.18),_transparent_36%),linear-gradient(135deg,rgba(14,99,156,0.22),rgba(15,23,42,0.18))]">
        <div className="grid gap-6 lg:grid-cols-[1.3fr_0.7fr]">
          <div className="flex flex-col gap-4">
            <div className="inline-flex w-fit items-center gap-2 rounded-full border border-sky-500/20 bg-sky-500/10 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-sky-200">
              Fisconforme não Atendido
              <span className="rounded-full bg-slate-900/60 px-2 py-0.5 text-[10px] text-slate-300">
                atividade de Acervo
              </span>
            </div>

            <div>
              <h2 className="text-3xl font-semibold tracking-tight text-white">
                Central de DSFs e notificações TXT
              </h2>
              <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-300">
                Organize o acervo por DSF, reabra consultas com os campos já
                preenchidos e mantenha o destino das notificações definido antes
                de entrar nas etapas de Consulta, Resultados e Para Notificações.
              </p>
            </div>

            <div className="flex flex-wrap gap-2 text-[11px] text-slate-300">
              {["1. Consulta", "2. Resultados", "3. Para Notificações"].map(
                (item) => (
                  <span
                    key={item}
                    className="rounded-full border border-slate-700 bg-slate-900/60 px-3 py-1"
                  >
                    {item}
                  </span>
                ),
              )}
            </div>

            <div className="flex flex-wrap gap-3">
              <PrimaryBtn onClick={onNew}>Abrir novo acervo</PrimaryBtn>
              <SecondaryBtn onClick={onRefresh}>
                Atualizar lista de DSFs
              </SecondaryBtn>
            </div>
          </div>

          <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1">
            <Card className="border-sky-500/20 bg-slate-950/30">
              <div className="text-[11px] uppercase tracking-[0.18em] text-slate-400">
                DSFs cadastradas
              </div>
              <div className="mt-3 text-3xl font-semibold text-white">
                {totals.dsfs}
              </div>
              <div className="mt-1 text-xs text-slate-400">
                prontas para reabertura com os dados preenchidos
              </div>
            </Card>

            <Card className="border-emerald-500/20 bg-slate-950/30">
              <div className="text-[11px] uppercase tracking-[0.18em] text-slate-400">
                CNPJs mapeados
              </div>
              <div className="mt-3 text-3xl font-semibold text-white">
                {totals.cnpjs}
              </div>
              <div className="mt-1 text-xs text-slate-400">
                vinculados ao acervo atual
              </div>
            </Card>
          </div>
        </div>
      </Card>

      <Card>
        <div className="mb-4 flex items-center justify-between gap-3">
          <div>
            <h3 className="text-sm font-semibold text-white">
              Acervo de DSFs existentes
            </h3>
            <p className="mt-1 text-xs text-slate-400">
              Abra uma DSF já cadastrada para continuar com os campos
              preenchidos.
            </p>
          </div>
          <div className="text-xs text-slate-500">
            {loading ? "Carregando..." : `${dsfs.length} registro(s)`}
          </div>
        </div>

        {error && (
          <div className="mb-4 rounded-lg bg-red-950/40 px-3 py-2 text-xs text-red-300">
            {error}
          </div>
        )}

        {!loading && dsfs.length === 0 && (
          <div className="rounded-xl border border-dashed border-slate-700 bg-slate-950/30 px-4 py-6 text-sm text-slate-400">
            Nenhuma DSF salva ainda. Use o card acima para iniciar o primeiro
            acervo.
          </div>
        )}

        <div className="flex flex-col gap-3">
          {dsfs.map((item) => (
            <div
              key={item.id}
              className="rounded-xl border border-slate-800 bg-slate-950/30 p-4 transition-colors hover:border-slate-700"
            >
              <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div className="min-w-0 flex-1">
                  <div className="text-sm font-semibold text-white">
                    {buildDsfHeadline(item)}
                  </div>
                  <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-slate-400">
                    <span className="rounded-full bg-slate-800/80 px-2.5 py-1">
                      {item.cnpjs_count} CNPJ(s)
                    </span>
                    <span className="rounded-full bg-slate-800/80 px-2.5 py-1">
                      período {item.data_inicio} a {item.data_fim}
                    </span>
                    <span className="rounded-full bg-slate-800/80 px-2.5 py-1">
                      {item.pdf_disponivel ? "PDF DSF salvo" : "Sem PDF salvo"}
                    </span>
                  </div>

                  <div className="mt-3 grid gap-2 text-xs text-slate-400 md:grid-cols-2">
                    <div>
                      <span className="text-slate-500">Pasta de saída:</span>{" "}
                      <span className="text-slate-300">
                        {item.output_dir || "download pelo navegador"}
                      </span>
                    </div>
                    <div>
                      <span className="text-slate-500">Atualizado em:</span>{" "}
                      <span className="text-slate-300">
                        {formatDateTime(item.updated_at)}
                      </span>
                    </div>
                  </div>
                </div>

                <div className="flex shrink-0 gap-2">
                  <SecondaryBtn onClick={() => onOpen(item.id)}>
                    Editar
                  </SecondaryBtn>
                  <PrimaryBtn onClick={() => onOpen(item.id)}>Abrir</PrimaryBtn>
                </div>
              </div>
            </div>
          ))}
        </div>
      </Card>
    </div>
  );
}

function CnpjInputStep({
  draft,
  pdfFile,
  onBackHome,
  onDraftChange,
  onAuditorChange,
  onPdfChange,
  onSaveAuditor,
  onSaveDraft,
  onConsultar,
}: {
  draft: FisconformeDraft;
  pdfFile: File | null;
  onBackHome: () => void;
  onDraftChange: <K extends keyof FisconformeDraft>(
    field: K,
    value: FisconformeDraft[K],
  ) => void;
  onAuditorChange: (field: keyof AuditorConfig, value: string) => void;
  onPdfChange: (file: File | null) => void;
  onSaveAuditor: () => Promise<void>;
  onSaveDraft: () => Promise<void>;
  onConsultar: () => Promise<void>;
}) {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [salvandoAuditor, setSalvandoAuditor] = useState(false);
  const [feedback, setFeedback] = useState("");
  const [error, setError] = useState("");
  const resumoCnpjs = useMemo(() => resumir_cnpjs_do_rascunho(draft), [draft]);

  async function handleSave() {
    setSaving(true);
    setError("");
    setFeedback("");
    try {
      await onSaveDraft();
      setFeedback("Acervo salvo com sucesso.");
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setError(message);
    } finally {
      setSaving(false);
    }
  }

  async function handleConsultar() {
    setLoading(true);
    setError("");
    setFeedback("");
    try {
      await onConsultar();
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setError(message);
    } finally {
      setLoading(false);
    }
  }

  async function handleSalvarAuditor() {
    setSalvandoAuditor(true);
    setError("");
    setFeedback("");
    try {
      await onSaveAuditor();
      setFeedback("Dados do auditor salvos e vinculados à DSF atual.");
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setError(message);
    } finally {
      setSalvandoAuditor(false);
    }
  }

  return (
    <div className="mx-auto flex h-full w-full max-w-4xl flex-col gap-6 overflow-y-auto px-6 py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-white">1. Consulta</h2>
          <p className="mt-1 text-xs text-slate-400">
            Monte o acervo da DSF, defina a referência e consulte os CNPJs
            vinculados.
          </p>
        </div>
        <SecondaryBtn onClick={onBackHome}>Voltar para os acervos</SecondaryBtn>
      </div>

      <Card className="bg-[radial-gradient(circle_at_top_left,_rgba(56,189,248,0.12),_transparent_28%),rgba(15,23,42,0.72)]">
        <div className="grid gap-4 md:grid-cols-2">
          <div>
            <Label>Número da DSF</Label>
            <Input
              value={draft.dsf}
              onChange={(event) => onDraftChange("dsf", event.target.value)}
              placeholder="Ex: 1204"
            />
          </div>
          <div>
            <Label>Data de referência</Label>
            <Input
              type="month"
              value={draft.referencia}
              onChange={(event) =>
                onDraftChange("referencia", event.target.value)
              }
            />
          </div>
          <div className="md:col-span-2">
            <Label>Pasta de saída das notificações</Label>
            <Input
              value={draft.outputDir}
              onChange={(event) =>
                onDraftChange("outputDir", event.target.value)
              }
              placeholder="Ex: C:\\Notificacoes\\Fisconforme\\DSF_1204"
            />
            <div className="mt-1 text-[11px] text-slate-500">
              Se preenchido, o backend salva os TXT e o ZIP diretamente nesse
              caminho local.
            </div>
          </div>
          <div className="md:col-span-2">
            <Label>Arquivo da DSF (PDF)</Label>
            <div className="flex flex-wrap items-center gap-2">
              <SecondaryBtn onClick={() => fileInputRef.current?.click()}>
                {pdfFile ? "Trocar PDF" : "Selecionar PDF"}
              </SecondaryBtn>
              <input
                ref={fileInputRef}
                type="file"
                accept=".pdf"
                className="hidden"
                onChange={(event) =>
                  onPdfChange(event.target.files?.[0] ?? null)
                }
              />
              {pdfFile && (
                <span className="rounded-full bg-slate-800/80 px-3 py-1 text-xs text-slate-200">
                  {pdfFile.name}
                </span>
              )}
              {!pdfFile && draft.pdfStoredAvailable && (
                <span className="rounded-full bg-emerald-900/30 px-3 py-1 text-xs text-emerald-300">
                  PDF salvo no acervo:{" "}
                  {draft.pdfStoredName || "documento da DSF"}
                </span>
              )}
              {(pdfFile || draft.pdfStoredAvailable) && (
                <button
                  type="button"
                  onClick={() => onPdfChange(null)}
                  className="text-xs text-slate-400 transition-colors hover:text-white"
                >
                  limpar seleção atual
                </button>
              )}
            </div>
          </div>
        </div>
      </Card>

      <Card>
        <div className="mb-3 text-xs font-semibold text-slate-400">
          Dados do auditor vinculados à DSF
        </div>
        <div className="grid gap-3 md:grid-cols-2">
          <div className="md:col-span-2">
            <Label>Nome do Auditor</Label>
            <Input
              value={draft.auditorData.auditor}
              onChange={(event) =>
                onAuditorChange("auditor", event.target.value)
              }
              placeholder="Nome completo do auditor"
            />
          </div>
          <div>
            <Label>Cargo / Título</Label>
            <Input
              value={draft.auditorData.cargo_titulo}
              onChange={(event) =>
                onAuditorChange("cargo_titulo", event.target.value)
              }
              placeholder="Ex: Auditor"
            />
          </div>
          <div>
            <Label>Matrícula</Label>
            <Input
              value={draft.auditorData.matricula}
              onChange={(event) =>
                onAuditorChange("matricula", event.target.value)
              }
              placeholder="Número da matrícula"
            />
          </div>
          <div>
            <Label>Contato</Label>
            <Input
              value={draft.auditorData.contato}
              onChange={(event) =>
                onAuditorChange("contato", event.target.value)
              }
              placeholder="E-mail ou telefone"
            />
          </div>
          <div>
            <Label>Órgão de origem</Label>
            <Input
              value={draft.auditorData.orgao_origem}
              onChange={(event) =>
                onAuditorChange("orgao_origem", event.target.value)
              }
              placeholder="Ex: SEFIN / RO"
            />
          </div>
        </div>

        <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
          <div className="text-xs text-slate-500">
            Estes dados ficam salvos como padrão do auditor e também na DSF atual.
          </div>
          <PrimaryBtn
            onClick={handleSalvarAuditor}
            loading={salvandoAuditor}
            disabled={saving || loading}
          >
            Salvar dados do auditor
          </PrimaryBtn>
        </div>
      </Card>

      <Card>
        <div className="mb-3 text-xs font-semibold text-slate-400">
          CNPJs da análise em lote
        </div>

        <div className="mb-4 flex gap-2">
          {(["single", "lote"] as const).map((mode) => (
            <button
              key={mode}
              onClick={() => onDraftChange("mode", mode)}
              className={`rounded-full px-3 py-1 text-xs transition-colors ${
                draft.mode === mode
                  ? "bg-sky-500/20 text-sky-200"
                  : "bg-slate-800/80 text-slate-400 hover:text-white"
              }`}
            >
              {mode === "single" ? "CNPJ único" : "Lote de CNPJs"}
            </button>
          ))}
        </div>

        {draft.mode === "single" ? (
          <div className="mb-4">
            <Label>CNPJ</Label>
            <Input
              value={draft.cnpjSingle}
              onChange={(event) =>
                onDraftChange("cnpjSingle", event.target.value)
              }
              placeholder="12.345.678/0001-90"
            />
          </div>
        ) : (
          <div className="mb-4">
            <Label>CNPJs</Label>
            <textarea
              value={draft.cnpjsText}
              onChange={(event) =>
                onDraftChange("cnpjsText", event.target.value)
              }
              rows={8}
              className="w-full resize-y rounded-lg border border-slate-700 bg-slate-800/80 px-3 py-2 text-xs text-white outline-none transition-colors placeholder:text-slate-500 focus:border-sky-500"
              placeholder={"12.345.678/0001-90\n98.765.432/0001-10"}
            />
          </div>
        )}

        <div className="grid gap-3 md:grid-cols-2">
          <div>
            <Label>Período início (MM/AAAA)</Label>
            <Input
              value={draft.dataInicio}
              onChange={(event) =>
                onDraftChange("dataInicio", event.target.value)
              }
              placeholder="01/2021"
            />
          </div>
          <div>
            <Label>Período fim (MM/AAAA)</Label>
            <Input
              value={draft.dataFim}
              onChange={(event) => onDraftChange("dataFim", event.target.value)}
              placeholder="12/2025"
            />
          </div>
        </div>

        <label className="mt-4 flex items-center gap-2 text-xs text-slate-400">
          <input
            type="checkbox"
            checked={draft.forcar}
            onChange={(event) => onDraftChange("forcar", event.target.checked)}
            className="rounded"
          />
          Forçar atualização e ignorar o cache da consulta
        </label>

        <div className="mt-4 grid gap-3 md:grid-cols-4">
          <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">
              Informados
            </div>
            <div className="mt-2 text-xl font-semibold text-white">
              {resumoCnpjs.totalInformado}
            </div>
          </div>
          <div className="rounded-lg border border-emerald-900/60 bg-emerald-950/20 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-emerald-400/80">
              Válidos
            </div>
            <div className="mt-2 text-xl font-semibold text-white">
              {resumoCnpjs.totalValidos}
            </div>
          </div>
          <div className="rounded-lg border border-amber-900/60 bg-amber-950/20 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-amber-400/80">
              Duplicados
            </div>
            <div className="mt-2 text-xl font-semibold text-white">
              {resumoCnpjs.totalDuplicados}
            </div>
          </div>
          <div className="rounded-lg border border-rose-900/60 bg-rose-950/20 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-rose-400/80">
              Inválidos
            </div>
            <div className="mt-2 text-xl font-semibold text-white">
              {resumoCnpjs.totalInvalidos}
            </div>
          </div>
        </div>

        {resumoCnpjs.cnpjsInvalidos.length > 0 && (
          <div className="mt-4 rounded-lg bg-rose-950/30 px-3 py-2 text-xs text-rose-300">
            CNPJs inválidos ignorados na consulta: {resumoCnpjs.cnpjsInvalidos.join(", ")}
          </div>
        )}

        {feedback && (
          <div className="mt-4 rounded-lg bg-emerald-950/30 px-3 py-2 text-xs text-emerald-300">
            {feedback}
          </div>
        )}
        {error && (
          <div className="mt-4 rounded-lg bg-red-950/40 px-3 py-2 text-xs text-red-300">
            {error}
          </div>
        )}

        <div className="mt-5 flex flex-wrap justify-end gap-3">
          <SecondaryBtn onClick={handleSave} disabled={saving || loading}>
            {saving ? "Salvando..." : "Salvar no acervo"}
          </SecondaryBtn>
          <PrimaryBtn onClick={handleConsultar} loading={loading}>
            Consultar
          </PrimaryBtn>
        </div>
      </Card>
    </div>
  );
}

function ResultsStep({
  draft,
  results,
  expandedCnpj,
  filtroResultados,
  onBackHome,
  onBackInput,
  onChangeFiltroResultados,
  onToggleExpandedCnpj,
  onParaNotificacoes,
}: {
  draft: FisconformeDraft;
  results: FisconformeConsultaResult[];
  expandedCnpj: string | null;
  filtroResultados: FiltroResultados;
  onBackHome: () => void;
  onBackInput: () => void;
  onChangeFiltroResultados: (filtro: FiltroResultados) => void;
  onToggleExpandedCnpj: (cnpj: string) => void;
  onParaNotificacoes: () => void;
}) {
  // ⚡ Bolt: Optimize array aggregations by using a single .reduce() pass instead of multiple .filter().length and .reduce() calls, reducing O(4N) to O(N) traversals.
  const { totalMalhas, totalErros, totalComPendencia, totalSemPendencia } = results.reduce(
    (acc, item) => {
      const malhasCount = item.malhas?.length ?? 0;
      acc.totalMalhas += malhasCount;

      if (item.error) {
        acc.totalErros += 1;
      } else if (malhasCount > 0) {
        acc.totalComPendencia += 1;
      } else {
        acc.totalSemPendencia += 1;
      }

      return acc;
    },
    { totalMalhas: 0, totalErros: 0, totalComPendencia: 0, totalSemPendencia: 0 }
  );
  const resultadosFiltrados = useMemo(
    () => obter_resultados_filtrados(results, filtroResultados),
    [filtroResultados, results],
  );

  return (
    <div className="mx-auto flex h-full w-full max-w-6xl flex-col gap-6 overflow-y-auto px-6 py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">
            {buildDsfHeadline({
              dsf: draft.dsf,
              referencia: draft.referencia,
              auditor: draft.auditorData.auditor,
            })}
          </div>
          <h2 className="mt-2 text-lg font-semibold text-white">
            2. Resultados
          </h2>
        </div>
        <div className="flex flex-wrap gap-2">
          <SecondaryBtn onClick={onBackHome}>Acervos</SecondaryBtn>
          <SecondaryBtn onClick={onBackInput}>Editar consulta</SecondaryBtn>
          <PrimaryBtn onClick={onParaNotificacoes}>Para Notificações</PrimaryBtn>
        </div>
      </div>

      <div className="grid gap-3 md:grid-cols-4">
        <Card className="p-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Consultados</div>
          <div className="mt-2 text-2xl font-semibold text-white">{results.length}</div>
        </Card>
        <Card className="border-amber-900/60 bg-amber-950/10 p-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-amber-400/80">Com pendência</div>
          <div className="mt-2 text-2xl font-semibold text-white">{totalComPendencia}</div>
        </Card>
        <Card className="border-emerald-900/60 bg-emerald-950/10 p-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-emerald-400/80">Sem pendência</div>
          <div className="mt-2 text-2xl font-semibold text-white">{totalSemPendencia}</div>
        </Card>
        <Card className="border-rose-900/60 bg-rose-950/10 p-4">
          <div className="text-[11px] uppercase tracking-[0.18em] text-rose-400/80">Com erro</div>
          <div className="mt-2 text-2xl font-semibold text-white">{totalErros}</div>
        </Card>
      </div>

      <Card>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex flex-wrap gap-4 text-xs text-slate-400">
            <span>
              <span className="font-semibold text-white">{totalMalhas}</span>{" "}
              pendência(s)
            </span>
            <span>
              <span className="font-semibold text-white">
                {results.filter((item) => item.from_cache).length}
              </span>{" "}
              carregados do cache
            </span>
          </div>
          <div className="flex flex-wrap gap-2">
            {([
              ["todos", "Todos"],
              ["com_pendencia", "Com pendência"],
              ["sem_pendencia", "Sem pendência"],
              ["com_erro", "Com erro"],
            ] as const).map(([filtro, rotulo]) => (
              <button
                key={filtro}
                onClick={() => onChangeFiltroResultados(filtro)}
                className={`rounded-full px-3 py-1 text-xs transition-colors ${
                  filtroResultados === filtro
                    ? "bg-sky-500/20 text-sky-200"
                    : "bg-slate-800/80 text-slate-400 hover:text-white"
                }`}
              >
                {rotulo}
              </button>
            ))}
          </div>
        </div>
      </Card>

      <div className="flex flex-col gap-3">
        {resultadosFiltrados.length === 0 ? (
          <Card>
            <div className="text-sm text-slate-400">
              Nenhum CNPJ corresponde ao filtro selecionado.
            </div>
          </Card>
        ) : (
          resultadosFiltrados.map((result) => (
            <ResultCard
              key={result.cnpj}
              result={result}
              expanded={expandedCnpj === result.cnpj}
              onToggle={() => onToggleExpandedCnpj(result.cnpj)}
            />
          ))
        )}
      </div>
    </div>
  );
}

function ResultCard({
  result,
  expanded,
  onToggle,
}: {
  result: FisconformeConsultaResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  const dc = result.dados_cadastrais as Record<string, unknown> | null;
  const malhas = (result.malhas ?? []) as MalhaRecord[];

  const razaoSocial = String(
    dc?.RAZAO_SOCIAL ?? dc?.NOME ?? dc?.Nome ?? dc?.razao_social ?? "",
  );

  const isUrl = (value: string): boolean =>
    Boolean(
      value && (value.startsWith("http://") || value.startsWith("https://")),
    );

  return (
    <Card className={result.error ? "border-red-800/60" : ""}>
      <div className="flex items-start justify-between gap-3">
        <button
          onClick={onToggle}
          className="min-w-0 flex-1 text-left"
        >
          <div className="flex flex-wrap items-center gap-3">
            <span className="font-mono text-xs font-semibold text-white">
              {result.cnpj}
            </span>
            {razaoSocial && (
              <span className="truncate text-xs text-slate-300">
                {razaoSocial}
              </span>
            )}
            {result.from_cache && (
              <span className="rounded-full bg-slate-800 px-2 py-0.5 text-[11px] text-slate-400">
                cache
              </span>
            )}
          </div>
        </button>

        <div className="flex shrink-0 items-center gap-3">
          <SecondaryBtn onClick={() => abrir_dossie_em_nova_aba(result.cnpj)}>
            Abrir Dossiê
          </SecondaryBtn>
          {result.error ? (
            <span className="text-xs text-red-400">{result.error}</span>
          ) : (
            <span
              className={`text-xs font-medium ${
                malhas.length > 0 ? "text-amber-400" : "text-emerald-400"
              }`}
            >
              {malhas.length > 0
                ? `${malhas.length} pendência(s)`
                : "Sem pendências"}
            </span>
          )}
          <button onClick={onToggle} className="text-xs text-slate-500">
            {expanded ? "▲" : "▼"}
          </button>
        </div>
      </div>

      {expanded && !result.error && (
        <div className="mt-4 flex flex-col gap-4 border-t border-slate-800 pt-4">
          {dc && (
            <div>
              <div className="mb-2 text-xs font-semibold text-slate-400">
                Dados cadastrais
              </div>
              <div className="grid gap-x-6 gap-y-2 md:grid-cols-2">
                {Object.entries(dc)
                  .filter(
                    ([key]) => !["cached_at", "_FROM_PARQUET"].includes(key),
                  )
                  .map(([key, value]) => {
                    const rendered = String(value ?? "");
                    return (
                      <div key={key} className="flex gap-3 text-xs">
                        <span className="w-40 shrink-0 text-slate-500">
                          {key}
                        </span>
                        {isUrl(rendered) ? (
                          <a
                            href={rendered}
                            target="_blank"
                            rel="noreferrer"
                            className="truncate text-sky-400 underline"
                          >
                            {rendered}
                          </a>
                        ) : (
                          <span className="truncate text-slate-200">
                            {rendered}
                          </span>
                        )}
                      </div>
                    );
                  })}
              </div>
            </div>
          )}

          {malhas.length > 0 ? (
            <div>
              <div className="mb-2 text-xs font-semibold text-slate-400">
                Pendências fiscais ({malhas.length})
              </div>
              <div className="overflow-x-auto">
                <table className="w-full border-collapse text-xs">
                  <thead>
                    <tr className="border-b border-slate-800 text-slate-400">
                      <th className="py-1 pr-3 text-left">ID Pend.</th>
                      <th className="py-1 pr-3 text-left">ID Notif.</th>
                      <th className="py-1 pr-3 text-left">Malha ID</th>
                      <th className="py-1 pr-3 text-left">Título</th>
                      <th className="py-1 pr-3 text-left">Período</th>
                      <th className="py-1 pr-3 text-left">Status Pend.</th>
                      <th className="py-1 pr-3 text-left">Status Notif.</th>
                      <th className="py-1 text-left">Ciência</th>
                    </tr>
                  </thead>
                  <tbody>
                    {malhas.map((malha, index) => {
                      const value = (lower: string, upper: string) =>
                        String(malha[lower] ?? malha[upper] ?? "-");
                      const statusPendencia = String(
                        malha.status_pendencia ?? malha.STATUS_PENDENCIA ?? "",
                      );

                      return (
                        <tr
                          key={`${result.cnpj}-${index}`}
                          className="border-b border-slate-900 text-slate-300 hover:bg-slate-800/40"
                        >
                          <td className="py-1 pr-3">
                            {value("id_pendencia", "ID_PENDENCIA")}
                          </td>
                          <td className="py-1 pr-3 text-slate-400">
                            {value("id_notificacao", "ID_NOTIFICACAO")}
                          </td>
                          <td className="py-1 pr-3 text-slate-400">
                            {value("malhas_id", "MALHAS_ID")}
                          </td>
                          <td className="py-1 pr-3 text-slate-200">
                            {value("titulo_malha", "TITULO_MALHA")}
                          </td>
                          <td className="py-1 pr-3">
                            {value("periodo", "PERIODO")}
                          </td>
                          <td
                            className={`py-1 pr-3 ${
                              statusPendencia.toLowerCase().includes("pendente")
                                ? "text-amber-400"
                                : "text-slate-300"
                            }`}
                          >
                            {statusPendencia || "-"}
                          </td>
                          <td className="py-1 pr-3">
                            {value("status_notificacao", "STATUS_NOTIFICACAO")}
                          </td>
                          <td className="py-1 text-slate-400">
                            {value(
                              "data_ciencia_consolidada",
                              "DATA_CIENCIA_CONSOLIDADA",
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>
          ) : (
            <div className="rounded-lg bg-emerald-950/20 px-3 py-2 text-xs text-emerald-300">
              Nenhuma pendência fiscal encontrada para o período informado.
            </div>
          )}
        </div>
      )}
    </Card>
  );
}

function DadosAuditorStep({
  draft,
  results,
  pdfFile,
  onBackHome,
  onBackInput,
  onBackResults,
}: {
  draft: FisconformeDraft;
  results: FisconformeConsultaResult[];
  pdfFile: File | null;
  onBackHome: () => void;
  onBackInput: () => void;
  onBackResults: () => void;
}) {
  const [gerando, setGerando] = useState<string | null>(null);
  const [gerandoDocx, setGerandoDocx] = useState<string | null>(null);
  const [gerandoLote, setGerandoLote] = useState(false);
  const [statusPerfil, setStatusPerfil] = useState("");
  const [erroLote, setErroLote] = useState("");
  const [erros, setErros] = useState<Record<string, string>>({});

  const activeResults = useMemo(
    () => results.filter((item) => !item.error),
    [results],
  );
  const checklistPronto = [
    { id: "dsf", label: "DSF preenchida", pronto: Boolean(draft.dsf.trim()) },
    {
      id: "auditor",
      label: "Auditor preenchido",
      pronto: Boolean(draft.auditorData.auditor.trim()),
    },
    {
      id: "pdf",
      label: "PDF disponível ou dispensado",
      pronto: Boolean(pdfFile || draft.pdfStoredAvailable || !draft.pdfStoredName),
    },
    {
      id: "saida",
      label: "Saída definida ou download no navegador",
      pronto: true,
    },
  ];

  async function montarPayloadBase() {
    const pdf_base64 = await fileToBase64(pdfFile);
    return {
      dsf: draft.dsf,
      dsf_id: draft.id ?? undefined,
      auditor: draft.auditorData.auditor,
      cargo_titulo: draft.auditorData.cargo_titulo,
      matricula: draft.auditorData.matricula,
      contato: draft.auditorData.contato,
      orgao_origem: draft.auditorData.orgao_origem,
      output_dir: draft.outputDir || undefined,
      pdf_base64,
    };
  }

  async function handleGerar(result: FisconformeConsultaResult) {
    setGerando(result.cnpj);
    setErros((current) => ({ ...current, [result.cnpj]: "" }));
    setStatusPerfil("");

    try {
      const payload: GerarNotificacaoRequest = {
        cnpj: result.cnpj,
        ...(await montarPayloadBase()),
      };
      const response = await fisconformeApi.gerarNotificacao(payload);
      downloadBlob(
        new Blob([response.conteudo], { type: "text/html;charset=utf-8" }),
        response.nome_arquivo,
      );
      if (response.salvo_em) {
        setStatusPerfil(
          `Arquivo salvo em ${response.salvo_em} e baixado no navegador.`,
        );
      }
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setErros((current) => ({ ...current, [result.cnpj]: message }));
    } finally {
      setGerando(null);
    }
  }

  async function handleGerarDocx(result: FisconformeConsultaResult) {
    setGerandoDocx(result.cnpj);
    setErros((current) => ({ ...current, [result.cnpj]: "" }));
    setStatusPerfil("");

    try {
      const payload: GerarNotificacaoRequest = {
        cnpj: result.cnpj,
        ...(await montarPayloadBase()),
      };
      const response = await fisconformeApi.gerarDocx(payload);
      const fileName = extractFileName(
        response.headers["content-disposition"],
        `notificacao_${result.cnpj}.docx`,
      );
      downloadBlob(
        new Blob([response.data], {
          type: "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        }),
        fileName,
      );
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setErros((current) => ({ ...current, [result.cnpj]: message }));
    } finally {
      setGerandoDocx(null);
    }
  }

  async function handleGerarLote() {
    setErroLote("");
    setStatusPerfil("");
    setGerandoLote(true);

    try {
      const response = await fisconformeApi.gerarNotificacoesLote({
        cnpjs: activeResults.map((item) => item.cnpj),
        ...(await montarPayloadBase()),
      });
      const fileName = extractFileName(
        response.headers["content-disposition"],
        `notificacoes_fisconforme_${activeResults.length}.zip`,
      );
      downloadBlob(
        new Blob([response.data], { type: "application/zip" }),
        fileName,
      );

      const savedTo = response.headers["x-saved-to"];
      const savedCount = response.headers["x-saved-count"];
      if (savedTo) {
        setStatusPerfil(
          `ZIP baixado no navegador e ${savedCount ?? activeResults.length} notificações salvas em ${savedTo}.`,
        );
      }
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setErroLote(message);
    } finally {
      setGerandoLote(false);
    }
  }

  return (
    <div className="mx-auto flex h-full w-full max-w-4xl flex-col gap-6 overflow-y-auto px-6 py-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">
            {buildDsfHeadline({
              dsf: draft.dsf,
              referencia: draft.referencia,
              auditor: draft.auditorData.auditor,
            })}
          </div>
          <h2 className="mt-2 text-lg font-semibold text-white">
            3. Para Notificações
          </h2>
        </div>
        <div className="flex flex-wrap gap-2">
          <SecondaryBtn onClick={onBackHome}>Acervos</SecondaryBtn>
          <SecondaryBtn onClick={onBackInput}>Consulta</SecondaryBtn>
          <SecondaryBtn onClick={onBackResults}>Resultados</SecondaryBtn>
        </div>
      </div>

      <Card>
        <div className="mb-4 flex items-center justify-between gap-3">
          <div>
            <div className="text-xs font-semibold text-slate-400">
              Resumo usado nas notificações
            </div>
            <p className="mt-1 text-xs text-slate-500">
              Esta etapa não edita os dados do auditor. Se precisar ajustar os
              campos da DSF, volte para a etapa 1. Consulta.
            </p>
          </div>
          <div className="rounded-full bg-slate-800/70 px-3 py-1 text-[11px] text-slate-300">
            saída: {draft.outputDir || "download pelo navegador"}
          </div>
        </div>

        <div className="grid gap-3 md:grid-cols-2">
          <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Auditor</div>
            <div className="mt-2 text-sm text-white">{draft.auditorData.auditor || "não preenchido"}</div>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Cargo / Título</div>
            <div className="mt-2 text-sm text-white">{draft.auditorData.cargo_titulo || "não preenchido"}</div>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Matrícula</div>
            <div className="mt-2 text-sm text-white">{draft.auditorData.matricula || "não preenchido"}</div>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Contato</div>
            <div className="mt-2 text-sm text-white">{draft.auditorData.contato || "não preenchido"}</div>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">Órgão de origem</div>
            <div className="mt-2 text-sm text-white">{draft.auditorData.orgao_origem || "não preenchido"}</div>
          </div>
          <div className="rounded-lg border border-slate-800 bg-slate-950/40 px-3 py-3">
            <div className="text-[11px] uppercase tracking-[0.18em] text-slate-500">PDF da DSF</div>
            <div className="mt-2 text-sm text-white">
              {pdfFile?.name || draft.pdfStoredName || "download pelo navegador"}
            </div>
          </div>
        </div>

        {statusPerfil && (
          <div
            className={`mt-4 rounded-lg px-3 py-2 text-xs ${
              statusPerfil.startsWith("Erro")
                ? "bg-red-950/40 text-red-300"
                : "bg-emerald-950/30 text-emerald-300"
            }`}
          >
            {statusPerfil}
          </div>
        )}
      </Card>

      <Card>
        <div className="mb-3 text-xs font-semibold text-slate-400">
          Checklist de prontidão
        </div>
        <div className="grid gap-3 md:grid-cols-2">
          {checklistPronto.map((item) => (
            <div
              key={item.id}
              className={`rounded-lg border px-3 py-3 text-sm ${
                item.pronto
                  ? "border-emerald-900/60 bg-emerald-950/10 text-emerald-300"
                  : "border-amber-900/60 bg-amber-950/10 text-amber-300"
              }`}
            >
              {item.pronto ? "Pronto" : "Pendente"}: {item.label}
            </div>
          ))}
        </div>
      </Card>

      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-xs font-semibold text-slate-400">
            Preenchimento das notificações
          </div>
          <div className="mt-1 text-xs text-slate-500">
            Gere um TXT por CNPJ ou todas as notificações de uma vez só.
          </div>
        </div>
        {activeResults.length > 0 && (
          <PrimaryBtn
            onClick={handleGerarLote}
            loading={gerandoLote}
            disabled={!draft.auditorData.auditor || gerando !== null}
          >
            {`Gerar as ${activeResults.length} notificações`}
          </PrimaryBtn>
        )}
      </div>

      {erroLote && (
        <div className="rounded-lg bg-red-950/40 px-3 py-2 text-xs text-red-300">
          {erroLote}
        </div>
      )}

      {activeResults.length === 0 && (
        <Card>
          <div className="text-sm text-slate-400">
            Nenhum resultado disponível para geração de notificação.
          </div>
        </Card>
      )}

      <div className="flex flex-col gap-4">
        {activeResults.map((result) => {
          const dc = result.dados_cadastrais as Record<string, unknown> | null;

          const getValue = (...keys: string[]): string => {
            for (const key of keys) {
              const value = dc?.[key];
              if (value !== undefined && value !== null && value !== "") {
                return String(value);
              }
            }
            return "-";
          };

          const razaoSocial = getValue(
            "RAZAO_SOCIAL",
            "NOME",
            "Nome",
            "razao_social",
          );
          const ie = getValue("IE", "INSCRICAO_ESTADUAL", "ie");

          const placeholders: Array<{
            key: string;
            value: string;
            source: "sql" | "auditor" | "auto";
          }> = [
            { key: "{{RAZAO_SOCIAL}}", value: razaoSocial, source: "sql" },
            { key: "{{CNPJ}}", value: result.cnpj, source: "sql" },
            { key: "{{IE}}", value: ie, source: "sql" },
            {
              key: "{{DSF}}",
              value: draft.dsf || "(não informado)",
              source: "auditor",
            },
            {
              key: "{{AUDITOR}}",
              value: draft.auditorData.auditor || "(não preenchido)",
              source: "auditor",
            },
            {
              key: "{{CARGO_TITULO}}",
              value: draft.auditorData.cargo_titulo || "(não preenchido)",
              source: "auditor",
            },
            {
              key: "{{MATRICULA}}",
              value: draft.auditorData.matricula || "(não preenchido)",
              source: "auditor",
            },
            {
              key: "{{CONTATO}}",
              value: draft.auditorData.contato || "(não preenchido)",
              source: "auditor",
            },
            {
              key: "{{ORGAO_ORIGEM}}",
              value: draft.auditorData.orgao_origem || "(não preenchido)",
              source: "auditor",
            },
            {
              key: "{{TABELA}}",
              value: `${result.malhas?.length ?? 0} pendência(s) - gerada automaticamente`,
              source: "auto",
            },
            {
              key: "{{DSF_IMAGENS}}",
              value:
                pdfFile?.name ||
                draft.pdfStoredName ||
                (draft.pdfStoredAvailable
                  ? "PDF da DSF salvo no acervo"
                  : "(arquivo não selecionado)"),
              source: "auto",
            },
          ];

          return (
            <Card key={result.cnpj}>
              <div className="mb-3 flex flex-wrap items-center justify-between gap-3">
                <div className="flex flex-wrap items-center gap-3">
                  <span className="font-mono text-xs font-semibold text-white">
                    {result.cnpj}
                  </span>
                  {razaoSocial !== "-" && (
                    <span className="text-xs text-slate-300">{razaoSocial}</span>
                  )}
                </div>
                <SecondaryBtn onClick={() => abrir_dossie_em_nova_aba(result.cnpj)}>
                  Abrir Dossiê
                </SecondaryBtn>
              </div>

              <div className="mb-4">
                <div className="mb-2 text-xs text-slate-500">
                  Mapeamento de placeholders
                </div>
                <div className="overflow-hidden rounded-lg border border-slate-800">
                  {placeholders.map(({ key, value, source }) => {
                    const missing = value.startsWith("(");
                    return (
                      <div
                        key={key}
                        className="flex items-baseline gap-2 border-b border-slate-900 px-3 py-2 text-xs last:border-b-0 hover:bg-slate-800/30"
                      >
                        <span className="w-36 shrink-0 font-mono text-sky-400">
                          {key}
                        </span>
                        <span
                          className={`flex-1 truncate ${
                            missing
                              ? "italic text-slate-500"
                              : source === "sql"
                                ? "text-emerald-300"
                                : source === "auditor"
                                  ? "text-amber-300"
                                  : "text-slate-400"
                          }`}
                        >
                          {value}
                        </span>
                        <span className="shrink-0 text-[11px] text-slate-600">
                          {source === "sql"
                            ? "consulta"
                            : source === "auditor"
                              ? "auditor"
                              : "auto"}
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>

              {erros[result.cnpj] && (
                <div className="mb-3 rounded-lg bg-red-950/40 px-3 py-2 text-xs text-red-300">
                  {erros[result.cnpj]}
                </div>
              )}

              <div className="flex justify-end gap-3">
                <SecondaryBtn
                  onClick={() => handleGerarDocx(result)}
                  disabled={
                    !draft.auditorData.auditor ||
                    gerando !== null ||
                    gerandoDocx !== null ||
                    gerandoLote
                  }
                >
                  {gerandoDocx === result.cnpj
                    ? "Gerando..."
                    : "Gerar notificação Word"}
                </SecondaryBtn>
                <PrimaryBtn
                  onClick={() => handleGerar(result)}
                  loading={gerando === result.cnpj}
                  disabled={
                    !draft.auditorData.auditor ||
                    gerando !== null ||
                    gerandoDocx !== null ||
                    gerandoLote
                  }
                >
                  Gerar notificação TXT
                </PrimaryBtn>
              </div>
            </Card>
          );
        })}
      </div>
    </div>
  );
}

export function FisconformeTab() {
  const [step, setStep] = useState<Step>("home");
  const [results, setResults] = useState<FisconformeConsultaResult[]>([]);
  const [draft, setDraft] = useState<FisconformeDraft>(EMPTY_DRAFT);
  const [pdfFile, setPdfFile] = useState<File | null>(null);
  const [expandedResultCnpj, setExpandedResultCnpj] = useState<string | null>(null);
  const [filtroResultados, setFiltroResultados] = useState<FiltroResultados>("todos");
  const [auditorDefaults, setAuditorDefaults] =
    useState<AuditorConfig>(EMPTY_AUDITOR_FORM);
  const [dsfs, setDsfs] = useState<FisconformeDsfSummary[]>([]);
  const [loadingHome, setLoadingHome] = useState(true);
  const [homeError, setHomeError] = useState("");

  useEffect(() => {
    void bootstrap();
  }, []);

  async function bootstrap() {
    setLoadingHome(true);
    setHomeError("");
    try {
      const [config, acervo] = await Promise.all([
        fisconformeApi.getAuditorConfig().catch(() => EMPTY_AUDITOR_FORM),
        fisconformeApi.listDsfs(),
      ]);

      setAuditorDefaults({ ...EMPTY_AUDITOR_FORM, ...config });
      setDsfs(acervo);
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setHomeError(`Erro ao carregar o acervo de DSFs: ${message}`);
    } finally {
      setLoadingHome(false);
    }
  }

  async function refreshDsfs() {
    setHomeError("");
    try {
      const acervo = await fisconformeApi.listDsfs();
      setDsfs(acervo);
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setHomeError(`Erro ao atualizar as DSFs: ${message}`);
    }
  }

  function updateDraft<K extends keyof FisconformeDraft>(
    field: K,
    value: FisconformeDraft[K],
  ) {
    setDraft((current) => ({ ...current, [field]: value }));
  }

  function updateAuditor(field: keyof AuditorConfig, value: string) {
    setDraft((current) => ({
      ...current,
      auditorData: {
        ...current.auditorData,
        [field]: value,
      },
    }));
  }

  function resetForNewDsf() {
    setResults([]);
    setPdfFile(null);
    setExpandedResultCnpj(null);
    setFiltroResultados("todos");
    setDraft({
      ...EMPTY_DRAFT,
      auditorData: auditorDefaults,
    });
    setStep("input");
  }

  async function openDsf(id: string) {
    setHomeError("");
    try {
      const record = await fisconformeApi.getDsf(id);
      setResults([]);
      setPdfFile(null);
      setExpandedResultCnpj(null);
      setFiltroResultados("todos");
      setDraft(buildDraftFromRecord(record, auditorDefaults));
      setStep("input");
    } catch (exc: unknown) {
      const message = exc instanceof Error ? exc.message : String(exc);
      setHomeError(`Erro ao abrir a DSF selecionada: ${message}`);
    }
  }

  async function persistCurrentDsf() {
    if (!draft.dsf.trim()) {
      throw new Error("Informe o número da DSF antes de salvar ou consultar.");
    }
    if (!draft.referencia.trim()) {
      throw new Error("Informe a data de referência da DSF.");
    }

    const saved = await fisconformeApi.salvarDsf({
      id: draft.id ?? undefined,
      dsf: draft.dsf,
      referencia: draft.referencia,
      cnpjs: normalizeDraftCnpjs(draft),
      data_inicio: draft.dataInicio,
      data_fim: draft.dataFim,
      forcar_atualizacao: draft.forcar,
      auditor: draft.auditorData.auditor,
      cargo_titulo: draft.auditorData.cargo_titulo,
      matricula: draft.auditorData.matricula,
      contato: draft.auditorData.contato,
      orgao_origem: draft.auditorData.orgao_origem,
      output_dir: draft.outputDir,
      pdf_file_name: pdfFile?.name || draft.pdfStoredName,
      pdf_base64: pdfFile ? await fileToBase64(pdfFile) : undefined,
    });

    setDraft((current) => ({
      ...current,
      id: saved.id,
      dataInicio: saved.data_inicio,
      dataFim: saved.data_fim,
      outputDir: saved.output_dir,
      pdfStoredAvailable: saved.pdf_disponivel,
      pdfStoredName: saved.pdf_file_name,
      auditorData: {
        auditor: saved.auditor,
        cargo_titulo: saved.cargo_titulo,
        matricula: saved.matricula,
        contato: saved.contato,
        orgao_origem: saved.orgao_origem,
      },
    }));

    await refreshDsfs();
    return saved;
  }

  async function handleConsultar() {
    await persistCurrentDsf();

    const cnpjs = normalizeDraftCnpjs(draft);
    if (!cnpjs.length) {
      throw new Error("Informe ao menos um CNPJ para consultar.");
    }

    if (draft.mode === "single") {
      const result = await fisconformeApi.consultaCadastral(
        cnpjs[0],
        draft.dataInicio,
        draft.dataFim,
        draft.forcar,
      );
      setResults([result]);
      setExpandedResultCnpj(result.cnpj);
    } else {
      const response = await fisconformeApi.consultaLote(
        cnpjs,
        draft.dataInicio,
        draft.dataFim,
        draft.forcar,
      );
      setResults(response.resultados);
      setExpandedResultCnpj(response.resultados.length === 1 ? (response.resultados[0]?.cnpj ?? null) : null);
    }

    setStep("results");
  }

  async function handleSaveAuditor() {
    await fisconformeApi.salvarAuditorConfig(draft.auditorData);
    await persistCurrentDsf();
  }

  const stepLabels: Record<Exclude<Step, "home">, string> = {
    input: "1. Consulta",
    results: "2. Resultados",
    auditor: "3. Para Notificações",
  };

  return (
    <div className="flex h-full flex-col overflow-hidden bg-[#0a1628]">
      {step !== "home" && (
        <div
          className="flex items-center gap-1 border-b border-slate-800 px-6 py-3"
          style={{ background: "#0d1f3c" }}
        >
          {(
            ["input", "results", "auditor"] as Array<Exclude<Step, "home">>
          ).map((currentStep) => {
            const order = { input: 0, results: 1, auditor: 2 };
            const isActive = step === currentStep;
            const isDone =
              order[currentStep] < order[step as keyof typeof order];

            return (
              <button
                key={currentStep}
                onClick={() => {
                  if (isActive || isDone) {
                    setStep(currentStep);
                  }
                }}
                className={`rounded-full px-3 py-1 text-xs transition-colors ${
                  isActive
                    ? "bg-sky-600 text-white"
                    : isDone
                      ? "text-sky-300"
                      : "text-slate-600"
                }`}
              >
                {stepLabels[currentStep]}
              </button>
            );
          })}
        </div>
      )}

      <div className="flex-1 overflow-hidden">
        {step === "home" && (
          <HomeStep
            dsfs={dsfs}
            loading={loadingHome}
            error={homeError}
            onRefresh={() => {
              void bootstrap();
            }}
            onNew={resetForNewDsf}
            onOpen={(id) => {
              void openDsf(id);
            }}
          />
        )}

        {step === "input" && (
          <CnpjInputStep
            draft={draft}
            pdfFile={pdfFile}
            onBackHome={() => setStep("home")}
            onDraftChange={updateDraft}
            onAuditorChange={updateAuditor}
            onPdfChange={setPdfFile}
            onSaveAuditor={handleSaveAuditor}
            onSaveDraft={async () => { await persistCurrentDsf(); }}
            onConsultar={handleConsultar}
          />
        )}

        {step === "results" && (
          <ResultsStep
            draft={draft}
            results={results}
            expandedCnpj={expandedResultCnpj}
            filtroResultados={filtroResultados}
            onBackHome={() => setStep("home")}
            onBackInput={() => setStep("input")}
            onChangeFiltroResultados={setFiltroResultados}
            onToggleExpandedCnpj={(cnpj) =>
              setExpandedResultCnpj((atual) => (atual === cnpj ? null : cnpj))
            }
            onParaNotificacoes={() => setStep("auditor")}
          />
        )}

        {step === "auditor" && (
          <DadosAuditorStep
            draft={draft}
            results={results}
            pdfFile={pdfFile}
            onBackHome={() => setStep("home")}
            onBackInput={() => setStep("input")}
            onBackResults={() => setStep("results")}
          />
        )}
      </div>
    </div>
  );
}
