import { useEffect, useMemo, useState } from "react";
import { Link } from "wouter";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";
import { useSelecaoOperacional } from "@/contexts/SelecaoOperacionalContext";
import { useCadastro } from "@/hooks/useAuditApi";
import type { DadosCadastrais, ResultadoConsultaCadastral } from "@/types/audit";
import { ArrowRight, Building2, FileSearch, RefreshCw } from "lucide-react";
import { toast } from "sonner";

function formatarDocumento(documento: string): string {
  const digitos = documento.replace(/\D/g, "");
  if (digitos.length === 11) {
    return digitos.replace(/(\d{3})(\d{3})(\d{3})(\d{2})/, "$1.$2.$3-$4");
  }
  if (digitos.length === 14) {
    return formatarCnpj(digitos);
  }
  return documento;
}

function formatarData(valor: string | null): string {
  if (!valor) return "-";
  const data = new Date(valor);
  if (Number.isNaN(data.getTime())) return valor;
  return new Intl.DateTimeFormat("pt-BR").format(data);
}

function possuiFichaCompleta(registro: DadosCadastrais | undefined): boolean {
  if (!registro) return false;
  return Boolean(registro.municipio || registro.regime_pagamento || registro.situacao_ie || registro.data_inicio_atividade);
}

function BlocoFicha({ rotulo, valor, mono = false }: { rotulo: string; valor: string | null | undefined; mono?: boolean }) {
  return (
    <div className="rounded-xl border border-border/80 bg-card p-4">
      <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">{rotulo}</p>
      <p className={`mt-2 text-sm text-foreground ${mono ? "font-mono" : ""}`}>{valor || "-"}</p>
    </div>
  );
}

export default function DadosCadastrais() {
  const { cnpjAtivo } = useCnpj();
  const { selecaoOperacional, atualizarResultadoCadastral } = useSelecaoOperacional();
  const { consultar, loading, error } = useCadastro();
  const [documentoAtivo, setDocumentoAtivo] = useState("");
  const [resultadoAvulso, setResultadoAvulso] = useState<ResultadoConsultaCadastral | null>(null);

  const resultados = selecaoOperacional?.resultados_cadastrais ?? [];
  const documentoFallback = selecaoOperacional?.documento_principal || cnpjAtivo;

  useEffect(() => {
    if (!documentoAtivo && documentoFallback) {
      setDocumentoAtivo(documentoFallback);
    }
  }, [documentoAtivo, documentoFallback]);

  const resultadoAtivo = useMemo(
    () =>
      resultados.find((item) => item.documento_consultado === documentoAtivo) ??
      resultadoAvulso ??
      resultados[0] ??
      null,
    [documentoAtivo, resultadoAvulso, resultados],
  );

  const registroAtivo = resultadoAtivo?.registros[0];

  useEffect(() => {
    if (resultados.length > 0 || !documentoFallback || documentoFallback.length < 11) {
      return;
    }

    consultar([documentoFallback])
      .then((resposta) => {
        const resultado = resposta.resultados.find((item) => item.documento_consultado === documentoFallback) ?? null;
        setResultadoAvulso(resultado);
      })
      .catch(() => {
        // Sem selecao ativa, a tela continua explicita mesmo sem carga Oracle.
      });
  }, [consultar, documentoFallback, resultados.length]);

  useEffect(() => {
    const documentoConsulta = resultadoAtivo?.documento_consultado;
    if (!documentoConsulta || !resultadoAtivo || resultadoAtivo.origem !== "storage" || possuiFichaCompleta(registroAtivo)) {
      return;
    }

    consultar([documentoConsulta])
      .then((resposta) => {
        const resultadoOracle = resposta.resultados.find((item) => item.documento_consultado === documentoConsulta);
        if (!resultadoOracle || !resultadoOracle.encontrado) return;

        atualizarResultadoCadastral(documentoConsulta, {
          ...resultadoOracle,
          origem: "misto",
        });
      })
      .catch(() => {
        // A tela continua funcional com os dados locais já resolvidos.
      });
  }, [atualizarResultadoCadastral, consultar, registroAtivo, resultadoAtivo]);

  const recarregarNoOracle = async () => {
    if (!resultadoAtivo?.documento_consultado) return;

    try {
      const resposta = await consultar([resultadoAtivo.documento_consultado]);
      const resultadoOracle = resposta.resultados.find(
        (item) => item.documento_consultado === resultadoAtivo.documento_consultado,
      );
      if (!resultadoOracle) return;
      atualizarResultadoCadastral(resultadoAtivo.documento_consultado, resultadoOracle);
      toast.success("Ficha cadastral atualizada com dados do Oracle");
    } catch (erro) {
      toast.error("Nao foi possivel atualizar a ficha cadastral", {
        description: (erro as Error).message,
      });
    }
  };

  if (!resultadoAtivo && !documentoFallback) {
    return (
      <div className="mx-auto max-w-4xl space-y-6">
        <Card className="border-border/80">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <FileSearch className="h-4 w-4 text-primary" />
              Dados cadastrais
            </CardTitle>
            <CardDescription>Selecione um documento na entrada operacional antes de consultar a ficha cadastral.</CardDescription>
          </CardHeader>
          <CardContent>
            <Button asChild>
              <Link href="/">Ir para a entrada operacional</Link>
            </Button>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="mx-auto max-w-6xl space-y-6">
      <Card className="border-border/80">
        <CardContent className="grid gap-6 p-0 lg:grid-cols-[1.2fr_0.8fr]">
          <div className="space-y-4 bg-[linear-gradient(135deg,rgba(15,23,42,0.98),rgba(30,64,175,0.92))] p-6 text-primary-foreground">
            <div className="flex items-center gap-3">
              <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-white/10">
                <Building2 className="h-5 w-5" />
              </div>
              <div>
                <p className="text-xs font-semibold uppercase tracking-[0.24em] text-white/70">Consulta Cadastral</p>
                <h1 className="text-2xl font-bold tracking-tight">{formatarDocumento(resultadoAtivo?.documento_consultado || documentoFallback)}</h1>
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex flex-wrap gap-2 text-xs">
                <Badge variant="secondary" className="border-0 bg-white/12 text-white">
                  Origem {resultadoAtivo?.origem ?? "sessao"}
                </Badge>
                <Badge variant={resultadoAtivo?.encontrado ? "default" : "outline"} className="border-white/20">
                  {resultadoAtivo?.encontrado ? "Registro encontrado" : "Sem retorno Oracle"}
                </Badge>
                <Badge variant="secondary" className="border-0 bg-white/12 text-white">
                  Tipo {resultadoAtivo?.tipo_documento?.toUpperCase() ?? "N/D"}
                </Badge>
              </div>
              <p className="text-sm leading-6 text-white/85">
                A ficha cadastral concentra a visão de identificação, enquadramento e situação cadastral do documento em análise.
              </p>
            </div>

            <div className="flex flex-wrap gap-2">
              <Button variant="secondary" onClick={recarregarNoOracle} disabled={loading}>
                <RefreshCw className="mr-2 h-4 w-4" />
                Atualizar no Oracle
              </Button>
              {selecaoOperacional?.cnpjs_resolvidos.length === 1 ? (
                <Button asChild>
                  <Link href="/dashboard">
                    Ir para dashboard
                    <ArrowRight className="ml-2 h-4 w-4" />
                  </Link>
                </Button>
              ) : null}
            </div>
          </div>

          <div className="space-y-3 p-6">
            <Card className="border-border/70">
              <CardHeader className="pb-3">
                <CardTitle className="text-sm font-semibold">Documentos resolvidos</CardTitle>
                <CardDescription>Escolha qual documento deve ocupar a ficha principal.</CardDescription>
              </CardHeader>
              <CardContent className="space-y-2">
                {resultados.map((resultado) => (
                  <button
                    key={resultado.documento_consultado}
                    type="button"
                    onClick={() => setDocumentoAtivo(resultado.documento_consultado)}
                    className={`w-full rounded-lg border px-3 py-2 text-left text-sm transition-colors ${
                      documentoAtivo === resultado.documento_consultado
                        ? "border-primary bg-primary/5"
                        : "border-border/80 hover:bg-accent/40"
                    }`}
                  >
                    <p className="font-mono text-xs font-semibold text-primary">
                      {formatarDocumento(resultado.documento_consultado)}
                    </p>
                    <p className="mt-1 text-xs text-muted-foreground">{resultado.registros[0]?.nome ?? resultado.mensagem ?? "Sem nome retornado"}</p>
                  </button>
                ))}
              </CardContent>
            </Card>

            {error ? (
              <Card className="border-destructive/30 bg-destructive/5">
                <CardContent className="pt-4 text-sm text-destructive">{error}</CardContent>
              </Card>
            ) : null}
          </div>
        </CardContent>
      </Card>

      {loading && !registroAtivo ? (
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
          {Array.from({ length: 6 }).map((_, indice) => (
            <Skeleton key={indice} className="h-24 rounded-xl" />
          ))}
        </div>
      ) : null}

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-3">
        <BlocoFicha rotulo="Nome" valor={registroAtivo?.nome} />
        <BlocoFicha rotulo="Nome Fantasia" valor={registroAtivo?.nome_fantasia} />
        <BlocoFicha rotulo="IE" valor={registroAtivo?.ie} mono />
        <BlocoFicha rotulo="Endereco" valor={registroAtivo?.endereco} />
        <BlocoFicha rotulo="Municipio" valor={registroAtivo?.municipio} />
        <BlocoFicha rotulo="UF" valor={registroAtivo?.uf} mono />
        <BlocoFicha rotulo="Regime de Pagamento" valor={registroAtivo?.regime_pagamento} />
        <BlocoFicha rotulo="Situacao da IE" valor={registroAtivo?.situacao_ie} />
        <BlocoFicha rotulo="Periodo em Atividade" valor={registroAtivo?.periodo_atividade} />
        <BlocoFicha rotulo="Inicio da Atividade" valor={formatarData(registroAtivo?.data_inicio_atividade ?? null)} />
        <BlocoFicha rotulo="Ultima Situacao" valor={formatarData(registroAtivo?.data_ultima_situacao ?? null)} />
        <BlocoFicha rotulo="Redesim" valor={registroAtivo?.url_redesim} />
      </div>
    </div>
  );
}
