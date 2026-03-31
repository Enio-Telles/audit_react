import { useEffect, useMemo, useState } from "react";
import { Link, useLocation } from "wouter";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";
import { useSelecaoOperacional } from "@/contexts/SelecaoOperacionalContext";
import { useAlvosAnalise, useCadastro, useSistema } from "@/hooks/useAuditApi";
import { classificarStatusOracle } from "@/lib/oracle";
import type { AlvoAnalise, ResultadoConsultaCadastral, SelecaoOperacional, SistemaStatus } from "@/types/audit";
import { ArrowRight, Building2, Database, FileText, IdCard, Loader2, Search, Shield } from "lucide-react";
import { toast } from "sonner";

type ModoEntrada = "cnpj" | "cpf" | "lote";

function normalizarDigitos(valor: string): string {
  return valor.replace(/\D/g, "");
}

function normalizarCpf(valor: string): string {
  return normalizarDigitos(valor).slice(0, 11);
}

function formatarCpf(cpf: string): string {
  const digits = normalizarCpf(cpf);
  if (digits.length <= 3) return digits;
  if (digits.length <= 6) return `${digits.slice(0, 3)}.${digits.slice(3)}`;
  if (digits.length <= 9) return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6)}`;
  return `${digits.slice(0, 3)}.${digits.slice(3, 6)}.${digits.slice(6, 9)}-${digits.slice(9)}`;
}

function formatarDocumento(documento: string): string {
  const digits = normalizarDigitos(documento);
  if (digits.length === 11) return formatarCpf(digits);
  if (digits.length === 14) return formatarCnpj(digits);
  return documento;
}

function formatarDataHora(valor: string | null): string {
  if (!valor) return "Sem atividade registrada";
  const data = new Date(valor);
  if (Number.isNaN(data.getTime())) return valor;
  return new Intl.DateTimeFormat("pt-BR", { dateStyle: "short", timeStyle: "short" }).format(data);
}

function resolverVariantePipeline(statusPipeline: AlvoAnalise["status_pipeline"]) {
  if (statusPipeline === "completo") return "default" as const;
  if (statusPipeline === "parcial") return "secondary" as const;
  return "outline" as const;
}

function resolverRotuloPipeline(alvo: AlvoAnalise): string {
  if (alvo.status_pipeline === "completo") return `Pipeline completo (${alvo.total_tabelas_ok}/${alvo.total_tabelas_esperadas})`;
  if (alvo.status_pipeline === "parcial") return `Pipeline parcial (${alvo.total_tabelas_ok}/${alvo.total_tabelas_esperadas})`;
  return "Sem pipeline materializado";
}

function extrairDocumentosLote(texto: string): string[] {
  return Array.from(
    new Set(
      texto
        .split(/[\s,;]+/)
        .map((item) => normalizarDigitos(item))
        .filter(Boolean),
    ),
  );
}

function LinhaAlvo({
  alvo,
  aoSelecionar,
}: {
  alvo: AlvoAnalise;
  aoSelecionar: (documento: string) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => aoSelecionar(alvo.cnpj)}
      className="w-full rounded-lg border border-border/80 bg-card p-3 text-left transition-colors hover:bg-accent/40"
    >
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0 space-y-1">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-mono text-xs font-semibold tracking-wide text-primary">{formatarCnpj(alvo.cnpj)}</span>
            <Badge variant={resolverVariantePipeline(alvo.status_pipeline)}>{resolverRotuloPipeline(alvo)}</Badge>
          </div>
          <p className="text-sm font-semibold text-foreground">{alvo.contribuinte ?? "Contribuinte sem nome consolidado"}</p>
          <div className="flex flex-wrap gap-x-4 gap-y-1 text-xs text-muted-foreground">
            <span>IE: {alvo.ie || "-"}</span>
            <span>Parquets: {alvo.total_parquets}</span>
            <span>Atualizado: {formatarDataHora(alvo.atualizado_em)}</span>
          </div>
        </div>
        <div className="flex shrink-0 items-center text-xs text-muted-foreground">
          Selecionar
          <ArrowRight className="ml-2 h-4 w-4" />
        </div>
      </div>
    </button>
  );
}

function construirResultadoStorage(alvo: AlvoAnalise): ResultadoConsultaCadastral {
  return {
    status: "ok",
    tipo_documento: "cnpj",
    documento_consultado: alvo.cnpj,
    origem: "storage",
    encontrado: true,
    mensagem: null,
    registros: [
      {
        documento: alvo.cnpj,
        ie: alvo.ie,
        nome: alvo.contribuinte,
        nome_fantasia: null,
        endereco: null,
        municipio: null,
        uf: null,
        regime_pagamento: null,
        situacao_ie: null,
        data_inicio_atividade: null,
        data_ultima_situacao: null,
        periodo_atividade: null,
        url_redesim: alvo.ie ? `https://portalcontribuinte.sefin.ro.gov.br/Publico/parametropublica.jsp?NuDevedor=${alvo.ie}` : null,
      },
    ],
  };
}

export default function Home() {
  const [, navegar] = useLocation();
  const { cnpjAtivo, definirCnpjAtivo } = useCnpj();
  const { selecaoOperacional, definirSelecaoOperacional, limparSelecaoOperacional } = useSelecaoOperacional();
  const { data: catalogo, loading: carregandoAlvos, error: erroAlvos, listar } = useAlvosAnalise();
  const { carregarStatus } = useSistema();
  const { consultar, loading: consultandoCadastro } = useCadastro();
  const [statusSistema, setStatusSistema] = useState<SistemaStatus | null>(null);
  const [modoAtivo, setModoAtivo] = useState<ModoEntrada>("cnpj");
  const [entradaCnpj, setEntradaCnpj] = useState("");
  const [entradaCpf, setEntradaCpf] = useState("");
  const [entradaLote, setEntradaLote] = useState("");

  useEffect(() => {
    setEntradaCnpj(formatarCnpj(cnpjAtivo));
  }, [cnpjAtivo]);

  useEffect(() => {
    listar().catch(() => undefined);
    carregarStatus().then(setStatusSistema).catch(() => undefined);
  }, [carregarStatus, listar]);

  const alvos = catalogo?.alvos ?? [];
  const resumo = catalogo?.resumo;
  const badgeOracle = classificarStatusOracle(statusSistema);
  const cnpjDigitado = normalizarDigitos(entradaCnpj).slice(0, 14);
  const cpfDigitado = normalizarCpf(entradaCpf);
  const documentosLote = useMemo(() => extrairDocumentosLote(entradaLote), [entradaLote]);

  const mapaCnpj = useMemo(() => new Map(alvos.map((alvo) => [alvo.cnpj, alvo])), [alvos]);
  const mapaCpf = useMemo(() => {
    const mapa = new Map<string, AlvoAnalise[]>();
    for (const alvo of alvos) {
      for (const cpf of alvo.cpfs_vinculados) {
        mapa.set(cpf, [...(mapa.get(cpf) ?? []), alvo]);
      }
    }
    return mapa;
  }, [alvos]);

  const alvosFiltradosPorCnpj = useMemo(() => {
    const termoBusca = entradaCnpj.trim().toLowerCase();
    if (!termoBusca) return alvos.slice(0, 8);
    return alvos.filter((alvo) => {
      const nome = (alvo.contribuinte ?? "").toLowerCase();
      return alvo.cnpj.includes(cnpjDigitado) || nome.includes(termoBusca);
    });
  }, [alvos, cnpjDigitado, entradaCnpj]);

  const alvosFiltradosPorCpf = useMemo(
    () => (cpfDigitado ? mapaCpf.get(cpfDigitado) ?? [] : []),
    [cpfDigitado, mapaCpf],
  );

  const irParaFluxoCnpj = (rota: string, cnpj: string) => {
    definirCnpjAtivo(cnpj);
    navegar(rota);
  };

  const montarSelecao = (
    tipoSelecao: ModoEntrada,
    documentosOrigem: string[],
    resultadosLocais: ResultadoConsultaCadastral[],
    resultadosOracle: ResultadoConsultaCadastral[],
  ): SelecaoOperacional => {
    const mapaResultados = new Map<string, ResultadoConsultaCadastral>();
    for (const resultado of [...resultadosLocais, ...resultadosOracle]) {
      mapaResultados.set(resultado.documento_consultado, resultado);
    }
    const resultadosCadastrais = Array.from(mapaResultados.values());
    const cnpjsResolvidos = Array.from(
      new Set(
        resultadosCadastrais.flatMap((resultado) => {
          if (!resultado.encontrado) return [];
          return resultado.registros
            .map((registro) => registro.documento)
            .filter((documento) => documento.length === 14);
        }),
      ),
    );

    return {
      tipo_selecao: tipoSelecao,
      documento_principal: documentosOrigem[0] ?? cnpjsResolvidos[0] ?? "",
      documentos_origem: documentosOrigem,
      cnpjs_resolvidos: cnpjsResolvidos,
      resultados_cadastrais: resultadosCadastrais,
    };
  };

  const resolverSelecao = async (documentos: string[], tipoSelecao: ModoEntrada) => {
    const documentosNormalizados = Array.from(new Set(documentos.map((item) => normalizarDigitos(item)).filter(Boolean)));
    if (documentosNormalizados.length === 0) {
      toast.error("Informe ao menos um documento valido");
      return;
    }

    const resultadosLocais: ResultadoConsultaCadastral[] = [];
    const documentosPendentesOracle = new Set<string>();

    for (const documento of documentosNormalizados) {
      if (documento.length === 14) {
        const alvo = mapaCnpj.get(documento);
        if (alvo) {
          resultadosLocais.push(construirResultadoStorage(alvo));
        } else {
          documentosPendentesOracle.add(documento);
        }
        continue;
      }

      if (documento.length === 11) {
        const correspondencias = mapaCpf.get(documento) ?? [];
        if (correspondencias.length > 0) {
          resultadosLocais.push(...correspondencias.map((alvo) => construirResultadoStorage(alvo)));
        } else {
          documentosPendentesOracle.add(documento);
        }
        continue;
      }
    }

    let resultadosOracle: ResultadoConsultaCadastral[] = [];

    if (documentosPendentesOracle.size > 0) {
      try {
        const resposta = await consultar(Array.from(documentosPendentesOracle));
        resultadosOracle = resposta.resultados.filter((item) => item.status === "ok");
      } catch (erro) {
        toast.error("Nao foi possivel consultar os documentos pendentes no Oracle", {
          description: (erro as Error).message,
        });
        if (resultadosLocais.length === 0) {
          return;
        }
      }
    }

    const novaSelecao = montarSelecao(tipoSelecao, documentosNormalizados, resultadosLocais, resultadosOracle);
    definirSelecaoOperacional(novaSelecao);

    if (novaSelecao.cnpjs_resolvidos.length === 1) {
      definirCnpjAtivo(novaSelecao.cnpjs_resolvidos[0]);
    }

    if (novaSelecao.resultados_cadastrais.length === 0) {
      toast.warning("Nenhum documento foi resolvido com dados locais ou Oracle");
      return;
    }

    toast.success("Selecao operacional atualizada", {
      description:
        novaSelecao.cnpjs_resolvidos.length > 0
          ? `${novaSelecao.cnpjs_resolvidos.length} CNPJ(s) resolvido(s)`
          : "Somente ficha cadastral do documento atual disponivel",
    });
  };

  const acoesDisponiveis = useMemo(() => {
    const totalCnpjs = selecaoOperacional?.cnpjs_resolvidos.length ?? 0;
    return {
      podeAbrirCadastral: Boolean(selecaoOperacional?.resultados_cadastrais.length || cnpjAtivo),
      podeAbrirFluxoCnpj: totalCnpjs === 1,
      podeAbrirRelatorios: totalCnpjs > 0,
      cnpjUnico: totalCnpjs === 1 ? selecaoOperacional?.cnpjs_resolvidos[0] ?? cnpjAtivo : cnpjAtivo,
    };
  }, [cnpjAtivo, selecaoOperacional]);

  const cardsSelecao = selecaoOperacional?.resultados_cadastrais ?? [];

  return (
    <div className="min-h-screen bg-background">
      <div className="mx-auto flex min-h-screen max-w-7xl flex-col gap-6 px-6 py-8">
        <Card className="overflow-hidden border-border/80">
          <CardContent className="grid gap-6 p-0 lg:grid-cols-[1.3fr_0.7fr]">
            <div className="space-y-6 bg-[linear-gradient(135deg,rgba(15,23,42,0.98),rgba(30,64,175,0.92))] p-6 text-primary-foreground">
              <div className="flex items-center gap-3">
                <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-white/10">
                  <Shield className="h-5 w-5" />
                </div>
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.24em] text-white/70">Audit React</p>
                  <h1 className="text-2xl font-bold tracking-tight">Entrada operacional da auditoria</h1>
                </div>
              </div>

              <div className="max-w-3xl space-y-3">
                <p className="text-sm leading-6 text-white/85">
                  Selecione CNPJ, CPF ou um lote de documentos. O sistema resolve primeiro pelo storage local e consulta o Oracle somente quando ainda nao houver materializacao local.
                </p>
                <div className="flex flex-wrap gap-2 text-xs">
                  <Badge variant="secondary" className="border-0 bg-white/12 text-white">
                    API {statusSistema?.api ?? "carregando"}
                  </Badge>
                  <Badge
                    variant={badgeOracle.variante}
                    className={
                      badgeOracle.variante === "default"
                        ? "bg-white text-slate-950"
                        : badgeOracle.variante === "destructive"
                          ? "bg-red-500/90 text-white"
                          : "border-white/25 bg-transparent text-white"
                    }
                  >
                    {badgeOracle.texto}
                  </Badge>
                  <Badge variant="secondary" className="border-0 bg-white/12 text-white">
                    CNPJs no storage: {resumo?.total_cnpjs ?? 0}
                  </Badge>
                </div>
              </div>

              {selecaoOperacional?.resultados_cadastrais.length ? (
                <div className="rounded-2xl border border-white/15 bg-white/6 p-4">
                  <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                    <div>
                      <p className="text-xs uppercase tracking-[0.2em] text-white/55">Selecao ativa</p>
                      <p className="mt-1 font-mono text-base font-semibold">
                        {formatarDocumento(selecaoOperacional.documento_principal)}
                      </p>
                      <p className="text-sm text-white/75">
                        {selecaoOperacional.cnpjs_resolvidos.length} CNPJ(s) resolvido(s) na sessao atual.
                      </p>
                    </div>
                    <Button variant="secondary" onClick={limparSelecaoOperacional}>
                      Limpar selecao
                    </Button>
                  </div>
                </div>
              ) : null}
            </div>

            <div className="grid gap-3 p-6">
              <Card className="border-border/70">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-semibold">Etapa 1</CardTitle>
                  <CardDescription>Informe o documento ou conjunto de documentos da analise.</CardDescription>
                </CardHeader>
              </Card>
              <Card className="border-border/70">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-semibold">Etapa 2</CardTitle>
                  <CardDescription>Resolucao hibrida: storage primeiro, Oracle sob demanda depois.</CardDescription>
                </CardHeader>
              </Card>
              <Card className="border-border/70">
                <CardHeader className="pb-3">
                  <CardTitle className="text-sm font-semibold">Etapa 3</CardTitle>
                  <CardDescription>Escolha o modulo de destino conforme o resultado da resolucao.</CardDescription>
                </CardHeader>
              </Card>
            </div>
          </CardContent>
        </Card>

        <div className="grid gap-6 xl:grid-cols-[1.1fr_0.9fr]">
          <Card className="border-border/80">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-base">
                <Search className="h-4 w-4 text-primary" />
                Etapa 1 - Selecionar documentos
              </CardTitle>
              <CardDescription>A resolucao local e preferencial. O Oracle entra somente para documentos ainda nao materializados.</CardDescription>
            </CardHeader>
            <CardContent>
              <Tabs value={modoAtivo} onValueChange={(valor) => setModoAtivo(valor as ModoEntrada)} className="gap-4">
                <TabsList className="grid w-full grid-cols-3">
                  <TabsTrigger value="cnpj">CNPJ</TabsTrigger>
                  <TabsTrigger value="cpf">CPF</TabsTrigger>
                  <TabsTrigger value="lote">Lote</TabsTrigger>
                </TabsList>

                <TabsContent value="cnpj" className="space-y-4">
                  <Input
                    value={entradaCnpj}
                    onChange={(event) => setEntradaCnpj(event.target.value)}
                    placeholder="00.000.000/0000-00 ou nome do contribuinte"
                    className="font-mono"
                  />
                  <div className="flex gap-2">
                    <Button onClick={() => resolverSelecao([cnpjDigitado || entradaCnpj], "cnpj")} disabled={consultandoCadastro}>
                      {consultandoCadastro ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                      Resolver selecao
                    </Button>
                  </div>
                  <ScrollArea className="h-[320px] rounded-lg border border-border/80 p-3">
                    <div className="space-y-3">
                      {alvosFiltradosPorCnpj.map((alvo) => (
                        <LinhaAlvo key={alvo.cnpj} alvo={alvo} aoSelecionar={(documento) => resolverSelecao([documento], "cnpj")} />
                      ))}
                      {!carregandoAlvos && alvosFiltradosPorCnpj.length === 0 ? (
                        <p className="text-sm text-muted-foreground">Nenhum CNPJ encontrado com esse filtro.</p>
                      ) : null}
                    </div>
                  </ScrollArea>
                </TabsContent>

                <TabsContent value="cpf" className="space-y-4">
                  <Input
                    value={entradaCpf}
                    onChange={(event) => setEntradaCpf(event.target.value)}
                    placeholder="000.000.000-00"
                    className="font-mono"
                  />
                  <div className="flex gap-2">
                    <Button onClick={() => resolverSelecao([cpfDigitado || entradaCpf], "cpf")} disabled={consultandoCadastro}>
                      {consultandoCadastro ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                      Resolver CPF
                    </Button>
                  </div>
                  <div className="space-y-3">
                    {alvosFiltradosPorCpf.map((alvo) => (
                      <LinhaAlvo key={`${cpfDigitado}-${alvo.cnpj}`} alvo={alvo} aoSelecionar={(documento) => resolverSelecao([documento], "cnpj")} />
                    ))}
                    {cpfDigitado.length < 11 ? (
                      <p className="text-sm text-muted-foreground">Informe os 11 digitos do CPF para buscar vinculos locais ou consultar o Oracle.</p>
                    ) : null}
                    {cpfDigitado.length === 11 && alvosFiltradosPorCpf.length === 0 ? (
                      <p className="text-sm text-muted-foreground">Sem vinculo local para esse CPF. O Oracle sera usado somente se voce resolver o documento.</p>
                    ) : null}
                  </div>
                </TabsContent>

                <TabsContent value="lote" className="space-y-4">
                  <Textarea
                    value={entradaLote}
                    onChange={(event) => setEntradaLote(event.target.value)}
                    placeholder="Cole CNPJs ou CPFs separados por espaco, virgula ou quebra de linha"
                    className="min-h-32 font-mono text-sm"
                  />
                  <div className="flex gap-2">
                    <Button onClick={() => resolverSelecao(documentosLote, "lote")} disabled={consultandoCadastro}>
                      {consultandoCadastro ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                      Resolver lote
                    </Button>
                  </div>
                  <p className="text-xs text-muted-foreground">
                    O lote prioriza o que ja existe no storage e consulta o Oracle apenas para os documentos restantes.
                  </p>
                </TabsContent>
              </Tabs>
            </CardContent>
          </Card>

          <Card className="border-border/80">
            <CardHeader>
              <CardTitle className="text-base">Etapa 2 - Resultado da resolucao</CardTitle>
              <CardDescription>
                Resultado consolidado da selecao atual. Nenhuma navegacao depende de mock ou sucesso ficticio.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              {erroAlvos ? (
                <div className="rounded-lg border border-destructive/30 bg-destructive/5 p-4 text-sm text-destructive">
                  Falha ao carregar catalogo de alvos: {erroAlvos}
                </div>
              ) : null}

              {cardsSelecao.length === 0 ? (
                <div className="rounded-lg border border-dashed border-border p-4 text-sm text-muted-foreground">
                  Nenhuma selecao operacional ativa.
                </div>
              ) : (
                cardsSelecao.map((resultado) => (
                  <div key={`${resultado.origem}-${resultado.documento_consultado}`} className="rounded-lg border border-border/80 bg-card p-3">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-mono text-xs font-semibold text-primary">
                        {formatarDocumento(resultado.documento_consultado)}
                      </span>
                      <Badge variant={resultado.encontrado ? "default" : "outline"}>{resultado.origem}</Badge>
                      <Badge variant="outline">{resultado.tipo_documento.toUpperCase()}</Badge>
                    </div>
                    <p className="mt-2 text-sm font-semibold">{resultado.registros[0]?.nome ?? resultado.mensagem ?? "Documento sem dados"}</p>
                    {resultado.registros[0]?.ie ? (
                      <p className="mt-1 text-xs text-muted-foreground">IE {resultado.registros[0].ie}</p>
                    ) : null}
                  </div>
                ))
              )}
            </CardContent>
          </Card>
        </div>

        <Card className="border-border/80">
          <CardHeader>
            <CardTitle className="text-base">Etapa 3 - Escolher o destino</CardTitle>
            <CardDescription>Os atalhos abaixo respeitam o tipo de selecao e a quantidade de CNPJs efetivamente resolvidos.</CardDescription>
          </CardHeader>
          <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-5">
            <Button asChild variant="outline" className="h-auto justify-between py-4" disabled={!acoesDisponiveis.podeAbrirCadastral}>
              <Link href="/dados-cadastrais">
                <span className="text-left">
                  <span className="block text-sm font-semibold">Dados cadastrais</span>
                  <span className="block text-xs text-muted-foreground">Ficha principal do documento em analise</span>
                </span>
                <IdCard className="h-4 w-4" />
              </Link>
            </Button>

            <Button
              variant="outline"
              className="h-auto justify-between py-4"
              disabled={!acoesDisponiveis.podeAbrirFluxoCnpj}
              onClick={() => acoesDisponiveis.cnpjUnico && irParaFluxoCnpj("/dashboard", acoesDisponiveis.cnpjUnico)}
            >
              <span className="text-left">
                <span className="block text-sm font-semibold">Dashboard</span>
                <span className="block text-xs text-muted-foreground">Fluxo disponivel apenas com CNPJ unico</span>
              </span>
              <Building2 className="h-4 w-4" />
            </Button>

            <Button
              variant="outline"
              className="h-auto justify-between py-4"
              disabled={!acoesDisponiveis.podeAbrirFluxoCnpj}
              onClick={() => acoesDisponiveis.cnpjUnico && irParaFluxoCnpj("/extracao", acoesDisponiveis.cnpjUnico)}
            >
              <span className="text-left">
                <span className="block text-sm font-semibold">Extracao</span>
                <span className="block text-xs text-muted-foreground">Consulta Oracle e pipeline por CNPJ</span>
              </span>
              <Database className="h-4 w-4" />
            </Button>

            <Button
              variant="outline"
              className="h-auto justify-between py-4"
              disabled={!acoesDisponiveis.podeAbrirFluxoCnpj}
              onClick={() => acoesDisponiveis.cnpjUnico && irParaFluxoCnpj("/consulta", acoesDisponiveis.cnpjUnico)}
            >
              <span className="text-left">
                <span className="block text-sm font-semibold">Consulta</span>
                <span className="block text-xs text-muted-foreground">Tabelas analiticas do CNPJ ativo</span>
              </span>
              <Search className="h-4 w-4" />
            </Button>

            <Button
              variant="outline"
              className="h-auto justify-between py-4"
              disabled={!acoesDisponiveis.podeAbrirRelatorios}
              onClick={() => navegar("/relatorios")}
            >
              <span className="text-left">
                <span className="block text-sm font-semibold">Relatorios</span>
                <span className="block text-xs text-muted-foreground">Aproveita o conjunto resolvido nesta sessao</span>
              </span>
              <FileText className="h-4 w-4" />
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
