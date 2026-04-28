import { useMemo, useState } from "react";

import type { DossieSectionData } from "../types";
import type { DossieViewMode } from "../utils/dossie_helpers";
import { VerticalTable } from "../../../components/table/VerticalTable";

interface DossieContatoDetalheProps {
  dados: DossieSectionData;
  viewMode?: DossieViewMode;
}

type RegistroContato = Record<string, unknown>;
type GrupoAgenda = "empresa" | "socios" | "contadores";

interface EvidenciaContato {
  fonte: string;
  valor: string;
  tabelaOrigem: string | null;
  origemDado: string | null;
}

interface EntidadeAgenda {
  chave: string;
  grupo: GrupoAgenda;
  nome: string;
  documento: string;
  vinculos: string[];
  situacoes: string[];
  fontes: string[];
  tabelasOrigem: string[];
  telefones: EvidenciaContato[];
  emails: EvidenciaContato[];
  enderecos: EvidenciaContato[];
}

interface ResumoGrupoAgenda {
  grupo: GrupoAgenda;
  titulo: string;
  totalEntidades: number;
  totalTelefones: number;
  totalEmails: number;
  totalEnderecos: number;
  totalConflitos: number;
  totalSemContato: number;
}

const TITULOS_GRUPO: Record<GrupoAgenda, string> = {
  empresa: "Empresa",
  socios: "Sócios",
  contadores: "Contadores",
};

const SUBTITULOS_GRUPO: Record<GrupoAgenda, string> = {
  empresa: "Matriz, filiais e dados cadastrais da empresa",
  socios: "Sócios atuais e anteriores vinculados ao CNPJ",
  contadores: "Contadores e escritórios contábeis vinculados",
};

const ICONES_GRUPO: Record<GrupoAgenda, string> = {
  empresa: "🏢",
  socios: "👥",
  contadores: "📋",
};

const ROTULOS_FONTE: Record<string, string> = {
  "dados_cadastrais.sql": "Cadastro BI",
  "dossie_historico_fac.sql": "FAC atual",
  "dossie_contador.sql": "Historico de contador",
  "dossie_rascunho_fac_contador.sql": "Rascunho FAC",
  "dossie_req_inscricao_contador.sql": "Requerimento",
  "dossie_historico_socios.sql": "Historico de socios",
  "dossie_filiais_raiz.sql": "Filiais da raiz",
  "NFe.sql": "NFe",
  "NFCe.sql": "NFCe",
};

function obter_texto(valor: unknown): string | null {
  if (valor === null || valor === undefined) {
    return null;
  }

  const texto = String(valor).trim();
  return texto || null;
}

function normalizar_valor(valor: string): string {
  return valor.trim().toLowerCase();
}

function quebrar_blocos_formatados(valor: string | null): string[] {
  if (!valor) {
    return [];
  }

  return valor
    .split("|")
    .map((item) => item.trim())
    .filter(Boolean);
}

function rotular_fonte(origemDado: string | null): string {
  if (!origemDado) {
    return "Fonte nao informada";
  }

  return ROTULOS_FONTE[origemDado] ?? origemDado;
}

function identificar_grupo_agenda(
  tipoVinculo: string | null,
): GrupoAgenda | null {
  if (!tipoVinculo) {
    return null;
  }

  if (
    tipoVinculo.startsWith("EMPRESA_") ||
    tipoVinculo === "MATRIZ_RAIZ" ||
    tipoVinculo === "FILIAL_RAIZ" ||
    tipoVinculo === "EMAIL_NFE"
  ) {
    return "empresa";
  }

  if (tipoVinculo.startsWith("SOCIO_")) {
    return "socios";
  }

  if (tipoVinculo.startsWith("CONTADOR_")) {
    return "contadores";
  }

  return null;
}

function rotular_vinculo(tipoVinculo: string | null): string {
  switch (tipoVinculo) {
    case "EMPRESA_PRINCIPAL":
      return "Principal";
    case "EMPRESA_FAC_ATUAL":
      return "FAC atual";
    case "MATRIZ_RAIZ":
      return "Matriz";
    case "FILIAL_RAIZ":
      return "Filial";
    case "SOCIO_ATUAL":
      return "Atual";
    case "SOCIO_ANTIGO":
      return "Antigo";
    case "CONTADOR_EMPRESA":
      return "Contador";
    case "EMAIL_NFE":
      return "Doc. fiscal";
    default:
      return tipoVinculo ?? "—";
  }
}

function montar_chave_entidade(
  registro: RegistroContato,
  grupo: GrupoAgenda,
): string {
  const documento =
    obter_texto(registro.cpf_cnpj_referencia) ??
    obter_texto(registro.cnpj_consultado) ??
    "sem-documento";
  const nome =
    obter_texto(registro.nome_referencia) ??
    rotular_vinculo(obter_texto(registro.tipo_vinculo));
  const tipoVinculo = obter_texto(registro.tipo_vinculo) ?? "SEM_VINCULO";

  if (grupo === "empresa") {
    return `${grupo}:${documento}`;
  }

  return `${grupo}:${documento}:${nome}:${tipoVinculo}`;
}

function criar_entidade_agenda(
  registro: RegistroContato,
  grupo: GrupoAgenda,
): EntidadeAgenda {
  const tipoVinculo = obter_texto(registro.tipo_vinculo);
  const documento =
    obter_texto(registro.cpf_cnpj_referencia) ??
    obter_texto(registro.cnpj_consultado) ??
    "Sem documento";
  const nome =
    obter_texto(registro.nome_referencia) ??
    (grupo === "empresa" ? "Empresa consultada" : rotular_vinculo(tipoVinculo));

  return {
    chave: montar_chave_entidade(registro, grupo),
    grupo,
    nome,
    documento,
    vinculos: tipoVinculo ? [tipoVinculo] : [],
    situacoes: [],
    fontes: [],
    tabelasOrigem: [],
    telefones: [],
    emails: [],
    enderecos: [],
  };
}

function adicionar_item_distinto(
  destino: string[],
  valor: string | null,
): void {
  if (!valor || destino.includes(valor)) {
    return;
  }

  destino.push(valor);
}

function adicionar_evidencia(
  destino: EvidenciaContato[],
  fonte: string,
  valor: string | null,
  tabelaOrigem: string | null,
  origemDado: string | null,
): void {
  if (!valor) {
    return;
  }

  const chave = `${fonte}::${valor}::${tabelaOrigem ?? ""}`;
  const jaExiste = destino.some(
    (item) =>
      `${item.fonte}::${item.valor}::${item.tabelaOrigem ?? ""}` === chave,
  );

  if (jaExiste) {
    return;
  }

  destino.push({
    fonte,
    valor,
    tabelaOrigem,
    origemDado,
  });
}

function adicionar_evidencias_por_bloco(
  destino: EvidenciaContato[],
  blocos: string[],
  fontePadrao: string,
  tabelaOrigem: string | null,
  origemDado: string | null,
): void {
  for (const bloco of blocos) {
    const separador = bloco.indexOf(":");
    const fonte =
      separador >= 0 ? bloco.slice(0, separador).trim() : fontePadrao;
    const valoresBrutos = separador >= 0 ? bloco.slice(separador + 1) : bloco;
    const valores = valoresBrutos
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);

    for (const valor of valores) {
      adicionar_evidencia(
        destino,
        fonte || fontePadrao,
        valor,
        tabelaOrigem,
        origemDado,
      );
    }
  }
}

function consolidar_agenda_por_entidade(
  registros: RegistroContato[],
): Record<GrupoAgenda, EntidadeAgenda[]> {
  const entidades = new Map<string, EntidadeAgenda>();

  for (const registro of registros) {
    const tipoVinculo = obter_texto(registro.tipo_vinculo);
    const grupo = identificar_grupo_agenda(tipoVinculo);

    if (!grupo) {
      continue;
    }

    const chave = montar_chave_entidade(registro, grupo);
    const entidadeExistente = entidades.get(chave);
    const entidade =
      entidadeExistente ?? criar_entidade_agenda(registro, grupo);
    const origemDado = obter_texto(registro.origem_dado);
    const tabelaOrigem = obter_texto(registro.tabela_origem);
    const fontePadrao = rotular_fonte(origemDado);
    const nomeReferencia = obter_texto(registro.nome_referencia);

    if (
      nomeReferencia &&
      entidade.nome !== nomeReferencia &&
      !nomeReferencia.toLowerCase().includes("documento fiscal")
    ) {
      entidade.nome = nomeReferencia;
    }

    adicionar_item_distinto(entidade.vinculos, tipoVinculo);
    adicionar_item_distinto(
      entidade.situacoes,
      obter_texto(registro.situacao_cadastral),
    );
    adicionar_item_distinto(entidade.fontes, fontePadrao);

    for (const fonteComplementar of quebrar_blocos_formatados(
      obter_texto(registro.fontes_contato),
    )) {
      adicionar_item_distinto(entidade.fontes, fonteComplementar);
    }

    if (tabelaOrigem) {
      for (const tabela of tabelaOrigem
        .split(";")
        .map((item) => item.trim())
        .filter(Boolean)) {
        adicionar_item_distinto(entidade.tabelasOrigem, tabela);
      }
    }

    const blocosTelefones = quebrar_blocos_formatados(
      obter_texto(registro.telefones_por_fonte),
    );
    const blocosEmails = quebrar_blocos_formatados(
      obter_texto(registro.emails_por_fonte),
    );

    if (blocosTelefones.length > 0) {
      adicionar_evidencias_por_bloco(
        entidade.telefones,
        blocosTelefones,
        fontePadrao,
        tabelaOrigem,
        origemDado,
      );
    }
    adicionar_evidencia(
      entidade.telefones,
      fontePadrao,
      obter_texto(registro.telefone),
      tabelaOrigem,
      origemDado,
    );
    for (const telefoneObservado of (
      obter_texto(registro.telefone_nfe_nfce) ?? ""
    ).split(",")) {
      adicionar_evidencia(
        entidade.telefones,
        "NFe/NFCe reconciliado",
        obter_texto(telefoneObservado),
        tabelaOrigem,
        origemDado,
      );
    }

    if (blocosEmails.length > 0) {
      adicionar_evidencias_por_bloco(
        entidade.emails,
        blocosEmails,
        fontePadrao,
        tabelaOrigem,
        origemDado,
      );
    }
    adicionar_evidencia(
      entidade.emails,
      fontePadrao,
      obter_texto(registro.email),
      tabelaOrigem,
      origemDado,
    );
    adicionar_evidencia(
      entidade.enderecos,
      fontePadrao,
      obter_texto(registro.endereco),
      tabelaOrigem,
      origemDado,
    );

    entidades.set(chave, entidade);
  }

  const grupos: Record<GrupoAgenda, EntidadeAgenda[]> = {
    empresa: [],
    socios: [],
    contadores: [],
  };

  for (const entidade of entidades.values()) {
    grupos[entidade.grupo].push(entidade);
  }

  for (const grupo of Object.keys(grupos) as GrupoAgenda[]) {
    grupos[grupo].sort((entidadeA, entidadeB) => {
      return (
        entidadeA.nome.localeCompare(entidadeB.nome) ||
        entidadeA.documento.localeCompare(entidadeB.documento)
      );
    });
  }

  return grupos;
}

function contar_valores_distintos(evidencias: EvidenciaContato[]): number {
  return new Set(evidencias.map((item) => normalizar_valor(item.valor))).size;
}

function possui_confirmacao_multifonte(
  evidencias: EvidenciaContato[],
): boolean {
  const fontesPorValor = new Map<string, Set<string>>();

  for (const evidencia of evidencias) {
    const chave = normalizar_valor(evidencia.valor);
    const fontes = fontesPorValor.get(chave) ?? new Set<string>();
    fontes.add(evidencia.fonte);
    fontesPorValor.set(chave, fontes);
  }

  return [...fontesPorValor.values()].some((fontes) => fontes.size > 1);
}

function calcular_status_entidade(entidade: EntidadeAgenda): string {
  const totalCategoriasComContato = [
    entidade.telefones.length > 0,
    entidade.emails.length > 0,
    entidade.enderecos.length > 0,
  ].filter(Boolean).length;

  if (totalCategoriasComContato === 0) {
    return "sem contato";
  }

  if (
    contar_valores_distintos(entidade.telefones) > 1 ||
    contar_valores_distintos(entidade.emails) > 1 ||
    contar_valores_distintos(entidade.enderecos) > 1
  ) {
    return "divergente";
  }

  if (
    possui_confirmacao_multifonte(entidade.telefones) ||
    possui_confirmacao_multifonte(entidade.emails) ||
    possui_confirmacao_multifonte(entidade.enderecos) ||
    totalCategoriasComContato === 3
  ) {
    return "confirmado";
  }

  return "parcial";
}

function resumir_grupos(
  grupos: Record<GrupoAgenda, EntidadeAgenda[]>,
): ResumoGrupoAgenda[] {
  return (Object.keys(grupos) as GrupoAgenda[]).map((grupo) => {
    const entidades = grupos[grupo];

    // ⚡ Bolt: Consolidate 5 O(N) loops and redundant status calculations into a single pass
    let totalTelefones = 0;
    let totalEmails = 0;
    let totalEnderecos = 0;
    let totalConflitos = 0;
    let totalSemContato = 0;

    for (const entidade of entidades) {
      totalTelefones += entidade.telefones.length;
      totalEmails += entidade.emails.length;
      totalEnderecos += entidade.enderecos.length;

      const status = calcular_status_entidade(entidade);
      if (status === "divergente") {
        totalConflitos++;
      } else if (status === "sem contato") {
        totalSemContato++;
      }
    }

    return {
      grupo,
      titulo: TITULOS_GRUPO[grupo],
      totalEntidades: entidades.length,
      totalTelefones,
      totalEmails,
      totalEnderecos,
      totalConflitos,
      totalSemContato,
    };
  });
}




function classe_badge_status(status: string): string {
  switch (status) {
    case "confirmado":
      return "border-emerald-700/60 bg-emerald-950/30 text-emerald-200";
    case "divergente":
      return "border-amber-700/60 bg-amber-950/30 text-amber-200";
    case "sem contato":
      return "border-slate-700 bg-slate-950/40 text-slate-400";
    default:
      return "border-cyan-700/60 bg-cyan-950/30 text-cyan-200";
  }
}

function renderizar_evidencias_compactas(
  evidencias: EvidenciaContato[],
): JSX.Element {
  if (evidencias.length === 0) {
    return <span className="text-slate-600">—</span>;
  }

  return (
    <div className="flex flex-col gap-1">
      {evidencias.map((evidencia) => (
        <div
          key={`${evidencia.fonte}-${evidencia.valor}`}
          className="flex items-baseline gap-1.5"
        >
          <span className="text-xs text-slate-200">{evidencia.valor}</span>
          <span className="text-[10px] text-slate-600">{evidencia.fonte}</span>
        </div>
      ))}
    </div>
  );
}

function TabelaContatoVertical({
  entidades,
}: {
  entidades: EntidadeAgenda[];
}): JSX.Element {
  if (entidades.length === 0) {
    return (
      <div className="rounded-xl border border-dashed border-slate-800 bg-slate-950/30 px-4 py-8 text-center text-sm text-slate-500">
        Nenhuma entidade encontrada neste grupo.
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-6 p-4">
      {entidades.map((entidade) => {
        const columns = [
          { header: "Nome / Razão Social", accessorKey: "nome" as const },
          { header: "Documento", accessorKey: "documento" as const },
          {
            header: "Tipos de Vínculos",
            accessorKey: "vinculos" as const,
            cell: (_: unknown, row: EntidadeAgenda) => (
              <div className="flex flex-wrap gap-1.5">
                {row.vinculos.map((v) => (
                  <span
                    key={v}
                    className="rounded bg-slate-800/80 px-2 py-0.5 text-[10px] text-slate-300"
                  >
                    {rotular_vinculo(v)}
                  </span>
                ))}
              </div>
            ),
          },
          {
            header: "Situação Cadastral",
            accessorKey: "situacoes" as const,
            cell: (_: unknown, row: EntidadeAgenda) => (
              <div className="flex flex-col gap-1 text-xs">
                {row.situacoes.map((s) => (
                  <div key={s}>{s}</div>
                ))}
                {row.situacoes.length === 0 && <span className="text-slate-600">—</span>}
              </div>
            ),
          },
          {
            header: "Status da Localização",
            accessorKey: "chave" as const,
            cell: (_: unknown, row: EntidadeAgenda) => {
              const status = calcular_status_entidade(row);
              return (
                <span
                  className={`inline-block rounded border px-2 py-0.5 text-[10px] uppercase font-semibold tracking-wider ${classe_badge_status(
                    status
                  )}`}
                >
                  {status}
                </span>
              );
            },
          },
          {
            header: "Endereços Registrados",
            accessorKey: "enderecos" as const,
            cell: (_: unknown, row: EntidadeAgenda) =>
              renderizar_evidencias_compactas(row.enderecos),
          },
          {
            header: "E-mails Detectados",
            accessorKey: "emails" as const,
            cell: (_: unknown, row: EntidadeAgenda) =>
              renderizar_evidencias_compactas(row.emails),
          },
          {
            header: "Telefones Detectados",
            accessorKey: "telefones" as const,
            cell: (_: unknown, row: EntidadeAgenda) =>
              renderizar_evidencias_compactas(row.telefones),
          },
        ];

        return (
          <VerticalTable
            key={entidade.chave}
            data={entidade}
            columns={columns}
          />
        );
      })}
    </div>
  );
}

function TabelaContatoGrupo({
  entidades,
  viewMode,
}: {
  entidades: EntidadeAgenda[];
  viewMode: DossieViewMode;
}): JSX.Element {
  if (entidades.length === 0) {
    return (
      <div className="rounded-xl border border-dashed border-slate-800 bg-slate-950/30 px-4 py-8 text-center text-sm text-slate-500">
        Nenhuma entidade encontrada neste grupo.
      </div>
    );
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full border-separate border-spacing-0 text-left">
        <thead>
          <tr className="text-[10px] uppercase tracking-[0.16em] text-slate-500">
            <th className="border-b border-slate-800 px-3 py-2.5 font-medium">
              Entidade
            </th>
            <th className="border-b border-slate-800 px-3 py-2.5 font-medium">
              Vínculo
            </th>
            <th className="border-b border-slate-800 px-3 py-2.5 font-medium text-center">
              Status
            </th>
            <th className="border-b border-slate-800 px-3 py-2.5 font-medium">
              Telefones
            </th>
            <th className="border-b border-slate-800 px-3 py-2.5 font-medium">
              Emails
            </th>
            <th className="border-b border-slate-800 px-3 py-2.5 font-medium">
              Endereços
            </th>
            {viewMode === "auditoria" && (
              <th className="border-b border-slate-800 px-3 py-2.5 font-medium">
                Fontes
              </th>
            )}
          </tr>
        </thead>
        <tbody>
          {entidades.map((entidade) => {
            const status = calcular_status_entidade(entidade);

            return (
              <tr
                key={entidade.chave}
                className="align-top text-xs transition-colors hover:bg-slate-800/30"
              >
                <td className="border-b border-slate-900/60 px-3 py-2.5">
                  <div className="font-medium text-slate-100">
                    {entidade.nome}
                  </div>
                  <div className="mt-0.5 font-mono text-[10px] text-slate-500">
                    {entidade.documento}
                  </div>
                  {entidade.situacoes.length > 0 && (
                    <div className="mt-0.5 text-[10px] text-slate-600">
                      {entidade.situacoes.join(" · ")}
                    </div>
                  )}
                </td>
                <td className="border-b border-slate-900/60 px-3 py-2.5">
                  <div className="flex flex-wrap gap-1">
                    {entidade.vinculos.map((vinculo) => (
                      <span
                        key={vinculo}
                        className="inline-flex rounded-md border border-slate-700/60 bg-slate-900/60 px-1.5 py-0.5 text-[10px] text-slate-300"
                      >
                        {rotular_vinculo(vinculo)}
                      </span>
                    ))}
                  </div>
                </td>
                <td className="border-b border-slate-900/60 px-3 py-2.5 text-center">
                  <span
                    className={`inline-flex rounded-full border px-2 py-0.5 text-[10px] font-medium uppercase tracking-wide ${classe_badge_status(status)}`}
                  >
                    {status}
                  </span>
                </td>
                <td className="border-b border-slate-900/60 px-3 py-2.5">
                  {renderizar_evidencias_compactas(entidade.telefones)}
                </td>
                <td className="border-b border-slate-900/60 px-3 py-2.5">
                  {renderizar_evidencias_compactas(entidade.emails)}
                </td>
                <td className="border-b border-slate-900/60 px-3 py-2.5">
                  {renderizar_evidencias_compactas(entidade.enderecos)}
                </td>
                {viewMode === "auditoria" && (
                  <td className="border-b border-slate-900/60 px-3 py-2.5">
                    <div className="flex flex-wrap gap-1">
                      {entidade.fontes.map((fonte) => (
                        <span
                          key={fonte}
                          className="rounded-md border border-cyan-800/40 bg-cyan-950/20 px-1.5 py-0.5 text-[10px] text-cyan-300"
                        >
                          {fonte}
                        </span>
                      ))}
                    </div>
                  </td>
                )}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function KpiCompacto({
  label,
  valor,
  cor,
}: {
  label: string;
  valor: number;
  cor: string;
}) {
  return (
    <div className="flex items-baseline gap-1.5">
      <span className={`text-base font-semibold tabular-nums ${cor}`}>
        {valor}
      </span>
      <span className="text-[10px] text-slate-500">{label}</span>
    </div>
  );
}

export function DossieContatoDetalhe({
  dados,
  viewMode = "auditoria",
}: DossieContatoDetalheProps) {
  const [abaAtiva, setAbaAtiva] = useState<GrupoAgenda>("empresa");
  const grupos = useMemo(
    () => consolidar_agenda_por_entidade(dados.rows),
    [dados.rows],
  );
  const resumo = useMemo(() => resumir_grupos(grupos), [grupos]);
  const resumoAbaAtiva = resumo.find((item) => item.grupo === abaAtiva);

  return (
    <div className="space-y-3">
      {/* Tab bar with inline KPIs */}
      <div className="flex items-end gap-0.5 border-b border-slate-800">
        {(["empresa", "socios", "contadores"] as const).map((grupo) => {
          const item = resumo.find((r) => r.grupo === grupo);
          const isActive = abaAtiva === grupo;

          return (
            <button
              key={grupo}
              type="button"
              onClick={() => setAbaAtiva(grupo)}
              className={`group relative flex items-center gap-2 rounded-t-lg px-4 py-2.5 text-xs font-medium transition-all ${
                isActive
                  ? "border border-b-0 border-slate-700 bg-slate-900/80 text-white"
                  : "border border-transparent text-slate-400 hover:bg-slate-800/40 hover:text-slate-200"
              }`}
            >
              <span className="text-sm">{ICONES_GRUPO[grupo]}</span>
              <span>{TITULOS_GRUPO[grupo]}</span>
              {item && (
                <span
                  className={`rounded-full px-1.5 py-0.5 text-[10px] tabular-nums ${
                    isActive
                      ? "bg-sky-500/20 text-sky-200"
                      : "bg-slate-800/60 text-slate-500"
                  }`}
                >
                  {item.totalEntidades}
                </span>
              )}
              {isActive && (
                <div className="absolute -bottom-px left-0 right-0 h-px bg-slate-900/80" />
              )}
            </button>
          );
        })}
      </div>

      {/* Summary strip for the active tab */}
      {resumoAbaAtiva && (
        <div className="flex flex-wrap items-center gap-x-5 gap-y-1 rounded-xl border border-slate-800 bg-slate-950/40 px-4 py-2.5">
          <div className="flex items-center gap-1.5 text-xs text-slate-400">
            <span>{ICONES_GRUPO[abaAtiva]}</span>
            <span className="font-medium text-slate-200">
              {SUBTITULOS_GRUPO[abaAtiva]}
            </span>
          </div>
          <div className="ml-auto flex flex-wrap items-center gap-4">
            <KpiCompacto
              label="entidades"
              valor={resumoAbaAtiva.totalEntidades}
              cor="text-white"
            />
            <KpiCompacto
              label="telefones"
              valor={resumoAbaAtiva.totalTelefones}
              cor="text-cyan-300"
            />
            <KpiCompacto
              label="emails"
              valor={resumoAbaAtiva.totalEmails}
              cor="text-emerald-300"
            />
            <KpiCompacto
              label="endereços"
              valor={resumoAbaAtiva.totalEnderecos}
              cor="text-amber-200"
            />
            {resumoAbaAtiva.totalConflitos > 0 && (
              <KpiCompacto
                label="conflitos"
                valor={resumoAbaAtiva.totalConflitos}
                cor="text-amber-400"
              />
            )}
            {resumoAbaAtiva.totalSemContato > 0 && (
              <KpiCompacto
                label="sem contato"
                valor={resumoAbaAtiva.totalSemContato}
                cor="text-slate-500"
              />
            )}
          </div>
        </div>
      )}

      {/* Active tab content */}
      <div className={`rounded-xl border border-slate-800 ${abaAtiva === 'empresa' ? 'bg-transparent border-none' : 'bg-slate-900/50'}`}>
        {abaAtiva === "empresa" ? (
          <TabelaContatoVertical entidades={grupos[abaAtiva]} />
        ) : (
          <TabelaContatoGrupo
            entidades={grupos[abaAtiva]}
            viewMode={viewMode}
          />
        )}
      </div>
    </div>
  );
}

export default DossieContatoDetalhe;
