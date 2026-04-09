import { useMemo } from "react";
import type { DossieSectionData } from "../types";

interface DossieContatoDetalheProps {
  dados: DossieSectionData;
}

type RegistroContato = Record<string, unknown>;

const ORDEM_GRUPOS = [
  "EMPRESA_PRINCIPAL",
  "MATRIZ_RAIZ",
  "FILIAL_RAIZ",
  "CONTADOR_EMPRESA",
  "SOCIO_ATUAL",
  "EMAIL_NFE",
];

function obter_texto(valor: unknown): string | null {
  if (valor === null || valor === undefined) {
    return null;
  }

  const texto = String(valor).trim();
  return texto || null;
}

function possui_contato_preenchido(registro: RegistroContato): boolean {
  return Boolean(
    obter_texto(registro.telefone) ||
      obter_texto(registro.telefone_nfe_nfce) ||
      obter_texto(registro.email),
  );
}

function possui_telefone_observado(registro: RegistroContato): boolean {
  return Boolean(obter_texto(registro.telefone_nfe_nfce));
}

function obter_alertas_registro(registro: RegistroContato): string[] {
  const alertas: string[] = [];
  const tipoVinculo = obter_texto(registro.tipo_vinculo);

  if ((tipoVinculo === "FILIAL_RAIZ" || tipoVinculo === "MATRIZ_RAIZ") && !possui_contato_preenchido(registro)) {
    alertas.push("Filial sem contato cadastral");
  }

  if (tipoVinculo === "CONTADOR_EMPRESA" && !possui_contato_preenchido(registro)) {
    alertas.push("Contador sem contato cadastral");
  }

  if (tipoVinculo === "CONTADOR_EMPRESA" && possui_telefone_observado(registro)) {
    alertas.push("Telefone observado em NFe/NFCe");
  }

  return alertas;
}

function agrupar_por_vinculo(registros: RegistroContato[]): Array<[string, RegistroContato[]]> {
  const grupos = new Map<string, RegistroContato[]>();

  for (const registro of registros) {
    const tipoVinculo = obter_texto(registro.tipo_vinculo) ?? "SEM_VINCULO";
    const grupoAtual = grupos.get(tipoVinculo) ?? [];
    grupoAtual.push(registro);
    grupos.set(tipoVinculo, grupoAtual);
  }

  return [...grupos.entries()].sort(([grupoA], [grupoB]) => {
    const indiceA = ORDEM_GRUPOS.indexOf(grupoA);
    const indiceB = ORDEM_GRUPOS.indexOf(grupoB);
    const ordemA = indiceA >= 0 ? indiceA : Number.MAX_SAFE_INTEGER;
    const ordemB = indiceB >= 0 ? indiceB : Number.MAX_SAFE_INTEGER;
    return ordemA - ordemB || grupoA.localeCompare(grupoB);
  });
}

function obter_titulo_grupo(tipoVinculo: string): string {
  switch (tipoVinculo) {
    case "EMPRESA_PRINCIPAL":
      return "Empresa principal";
    case "MATRIZ_RAIZ":
      return "Matriz da raiz";
    case "FILIAL_RAIZ":
      return "Filiais da mesma raiz";
    case "CONTADOR_EMPRESA":
      return "Contador da empresa";
    case "SOCIO_ATUAL":
      return "Socios atuais";
    case "EMAIL_NFE":
      return "Emails observados em documentos";
    default:
      return tipoVinculo;
  }
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

function classe_cartao_registro(registro: RegistroContato): string {
  const tipoVinculo = obter_texto(registro.tipo_vinculo);

  if (tipoVinculo === "EMPRESA_PRINCIPAL") {
    return "border-cyan-700/70 bg-cyan-950/30";
  }

  if (!possui_contato_preenchido(registro) && tipoVinculo !== "SOCIO_ATUAL") {
    return "border-amber-700/70 bg-amber-950/20";
  }

  return "border-slate-700 bg-slate-950/40";
}

export function DossieContatoDetalhe({ dados }: DossieContatoDetalheProps) {
  // Evita reagrupar e recalcular toda a estrutura visual quando o dataset da secao nao mudou.
  const grupos = useMemo(() => agrupar_por_vinculo(dados.rows), [dados.rows]);

  return (
    <div className="space-y-4">
      {grupos.map(([tipoVinculo, registros]) => (
        <section key={tipoVinculo} className="rounded-2xl border border-slate-800 bg-slate-900/60 p-4">
          <div className="mb-3 flex items-center justify-between gap-3">
            <div>
              <h3 className="text-sm font-semibold text-white">{obter_titulo_grupo(tipoVinculo)}</h3>
              <p className="text-xs text-slate-400">
                {registros.length} {registros.length === 1 ? "registro" : "registros"}
              </p>
            </div>
            <div className="rounded-full border border-slate-700 px-2 py-1 text-[11px] text-slate-300">{tipoVinculo}</div>
          </div>

          <div className="grid gap-3 lg:grid-cols-2">
            {registros.map((registro, indice) => {
              const nome = obter_texto(registro.nome_referencia) ?? "Sem nome";
              const documento = obter_texto(registro.cpf_cnpj_referencia) ?? "Sem documento";
              const telefone = obter_texto(registro.telefone);
              const telefoneObservado = obter_texto(registro.telefone_nfe_nfce);
              const email = obter_texto(registro.email);
              const emailsPorFonte = quebrar_blocos_formatados(obter_texto(registro.emails_por_fonte));
              const telefonesPorFonte = quebrar_blocos_formatados(obter_texto(registro.telefones_por_fonte));
              const fontesContato = quebrar_blocos_formatados(obter_texto(registro.fontes_contato));
              const endereco = obter_texto(registro.endereco);
              const origem = obter_texto(registro.origem_dado);
              const tabelaOrigem = obter_texto(registro.tabela_origem);
              const situacao = obter_texto(registro.situacao_cadastral);
              const indicador = obter_texto(registro.indicador_matriz_filial);
              const alertas = obter_alertas_registro(registro);

              return (
                <article key={`${tipoVinculo}-${indice}-${documento}`} className={`rounded-xl border p-4 ${classe_cartao_registro(registro)}`}>
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <h4 className="text-sm font-semibold text-white">{nome}</h4>
                      <div className="mt-1 text-xs text-slate-400">{documento}</div>
                    </div>
                    {indicador && <span className="rounded-full border border-slate-700 px-2 py-0.5 text-[10px] text-slate-300">{indicador}</span>}
                  </div>

                  {alertas.length > 0 && (
                    <div className="mt-3 flex flex-wrap gap-2">
                      {alertas.map((alerta) => (
                        <span
                          key={alerta}
                          className="rounded-full border border-amber-700/70 bg-amber-950/30 px-2 py-1 text-[10px] font-medium text-amber-200"
                        >
                          {alerta}
                        </span>
                      ))}
                    </div>
                  )}

                  <div className="mt-4 grid gap-2 text-xs text-slate-300">
                    <div>
                      <span className="text-slate-500">Telefone cadastral:</span> {telefone ?? "nao informado"}
                    </div>
                    <div>
                      <span className="text-slate-500">Telefone observado em NFe/NFCe:</span> {telefoneObservado ?? "nao observado"}
                    </div>
                    <div>
                      <span className="text-slate-500">Email:</span> {email ?? "nao informado"}
                    </div>
                    {telefonesPorFonte.length > 0 && (
                      <div>
                        <div className="text-slate-500">Telefones por fonte:</div>
                        <div className="mt-1 flex flex-wrap gap-2">
                          {telefonesPorFonte.map((bloco) => (
                            <span key={bloco} className="rounded-full border border-slate-700 px-2 py-1 text-[10px] text-slate-200">
                              {bloco}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                    {emailsPorFonte.length > 0 && (
                      <div>
                        <div className="text-slate-500">Emails por fonte:</div>
                        <div className="mt-1 flex flex-wrap gap-2">
                          {emailsPorFonte.map((bloco) => (
                            <span key={bloco} className="rounded-full border border-slate-700 px-2 py-1 text-[10px] text-slate-200">
                              {bloco}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                    {fontesContato.length > 0 && (
                      <div>
                        <div className="text-slate-500">Fontes consolidadas:</div>
                        <div className="mt-1 flex flex-wrap gap-2">
                          {fontesContato.map((bloco) => (
                            <span key={bloco} className="rounded-full border border-cyan-800/60 bg-cyan-950/30 px-2 py-1 text-[10px] text-cyan-200">
                              {bloco}
                            </span>
                          ))}
                        </div>
                      </div>
                    )}
                    <div>
                      <span className="text-slate-500">Endereco:</span> {endereco ?? "nao informado"}
                    </div>
                    <div>
                      <span className="text-slate-500">Situacao:</span> {situacao ?? "nao informada"}
                    </div>
                    <div>
                      <span className="text-slate-500">Origem:</span> {origem ?? "nao informada"}
                    </div>
                    <div>
                      <span className="text-slate-500">Tabela/view de origem:</span> {tabelaOrigem ?? "nao informada"}
                    </div>
                  </div>
                </article>
              );
            })}
          </div>
        </section>
      ))}
    </div>
  );
}

export default DossieContatoDetalhe;
