import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { DataTable } from "@/components/DataTable";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Loader2, Play, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import { usePipeline, useSistema } from "@/hooks/useAuditApi";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";
import {
  contarErrosTotais,
  listarConsultasComErro,
  montarResumoErrosExecucao,
} from "@/lib/pipeline";
import {
  classificarStatusOracle,
  identificarDicaOperacionalOracle,
} from "@/lib/oracle";
import type { SistemaStatus } from "@/types/audit";

const CONSULTAS_PADRAO = [
  "nfe",
  "nfce",
  "c100",
  "c170",
  "bloco_h",
  "reg0000",
  "reg0200",
  "reg0220",
];

export default function Extracao() {
  const { cnpjAtivo, definirCnpjAtivo } = useCnpj();
  const [dataLimite, setDataLimite] = useState("");
  const [consultasDisponiveis, setConsultasDisponiveis] =
    useState<string[]>(CONSULTAS_PADRAO);
  const [consultasSelecionadas, setConsultasSelecionadas] =
    useState<string[]>(CONSULTAS_PADRAO);
  const [statusSistema, setStatusSistema] = useState<SistemaStatus | null>(
    null
  );

  const { listarConsultas, carregarStatus } = useSistema();
  const {
    data: resultadoPipeline,
    loading: executando,
    error: erroPipeline,
    executar,
  } = usePipeline();
  const dicaErroPipeline = identificarDicaOperacionalOracle(erroPipeline);

  useEffect(() => {
    listarConsultas()
      .then(resposta => {
        if (resposta.consultas?.length) {
          setConsultasDisponiveis(resposta.consultas);
          setConsultasSelecionadas(resposta.consultas);
        }
      })
      .catch(() => {
        // Mantem fallback local quando backend ainda nao tiver consultas disponiveis.
      });
  }, [listarConsultas]);

  useEffect(() => {
    carregarStatus()
      .then(setStatusSistema)
      .catch(() => {
        // Mantem a tela funcional mesmo se o status operacional nao carregar.
      });
  }, [carregarStatus]);

  const cnpjFormatado = useMemo(() => formatarCnpj(cnpjAtivo), [cnpjAtivo]);
  const possuiExtracaoSelecionada = consultasSelecionadas.length > 0;
  const extracaoBloqueada =
    possuiExtracaoSelecionada && statusSistema?.oracle_conectada === false;
  const badgeOracle = classificarStatusOracle(statusSistema);
  const totalErrosExecucao = contarErrosTotais(resultadoPipeline);
  const consultasComErro = listarConsultasComErro(resultadoPipeline);

  const alternarConsulta = (consulta: string) => {
    setConsultasSelecionadas(atual =>
      atual.includes(consulta)
        ? atual.filter(item => item !== consulta)
        : [...atual, consulta]
    );
  };

  const executarPipeline = async () => {
    if (cnpjAtivo.length !== 14) {
      toast.error("Informe um CNPJ valido com 14 digitos");
      return;
    }

    const statusAtual = await carregarStatus()
      .then(resposta => {
        setStatusSistema(resposta);
        return resposta;
      })
      .catch(() => statusSistema);

    const extracaoBloqueadaAtual =
      consultasSelecionadas.length > 0 &&
      statusAtual?.oracle_conectada === false;

    if (extracaoBloqueadaAtual) {
      toast.error("Extracao bloqueada pela conexao Oracle ativa", {
        description:
          identificarDicaOperacionalOracle(
            statusAtual?.erro_oracle ?? erroPipeline
          ) ??
          "Selecione uma conexao Oracle valida em Configuracoes ou corrija VPN/DNS antes de extrair.",
      });
      return;
    }

    try {
      const resultado = await executar(
        cnpjAtivo,
        consultasSelecionadas,
        dataLimite || undefined,
        statusAtual?.oracle_indice_ativo ?? statusSistema?.oracle_indice_ativo
      );
      if (resultado.status === "concluido") {
        toast.success("Pipeline concluido", {
          description: `${resultado.tabelas_geradas.length} tabelas geradas`,
        });
      } else {
        toast.warning("Pipeline concluido com erros", {
          description: montarResumoErrosExecucao(resultado),
        });
      }
    } catch (erro) {
      const mensagemErro = (erro as Error).message;
      const dicaErro = identificarDicaOperacionalOracle(mensagemErro);
      toast.error("Falha na execucao do pipeline", {
        description: dicaErro ? `${mensagemErro} ${dicaErro}` : mensagemErro,
      });
    }
  };

  return (
    <div className="mx-auto max-w-5xl space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">
            Execucao do pipeline fiscal
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                CNPJ
              </Label>
              <Input
                value={cnpjFormatado}
                onChange={event => definirCnpjAtivo(event.target.value)}
                placeholder="00.000.000/0000-00"
                className="font-mono"
              />
            </div>
            <div className="space-y-2">
              <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                Data limite (opcional)
              </Label>
              <Input
                value={dataLimite}
                onChange={event => setDataLimite(event.target.value)}
                placeholder="YYYY-MM-DD (vazio = todo o historico)"
                className="font-mono"
              />
              <p className="text-[11px] text-muted-foreground">
                Deixe em branco para extrair todo o historico disponivel do
                CNPJ, sem filtro temporal.
              </p>
            </div>
          </div>

          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                Consultas SQL
              </Label>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setConsultasSelecionadas(consultasDisponiveis)}
                >
                  Selecionar todas
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setConsultasSelecionadas([])}
                >
                  Limpar
                </Button>
              </div>
            </div>
            <ScrollArea className="h-48 rounded border p-3">
              <div className="space-y-2">
                {consultasDisponiveis.map(consulta => (
                  <label
                    key={consulta}
                    className="flex cursor-pointer items-center gap-3 rounded px-2 py-1 hover:bg-accent/50"
                  >
                    <Checkbox
                      checked={consultasSelecionadas.includes(consulta)}
                      onCheckedChange={() => alternarConsulta(consulta)}
                    />
                    <span className="font-mono text-sm">{consulta}</span>
                  </label>
                ))}
              </div>
            </ScrollArea>
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3 rounded border px-3 py-2 text-xs">
            <div className="space-y-1">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant={badgeOracle.variante}>
                  {badgeOracle.texto}
                </Badge>
                <Badge variant="outline">
                  Conexao ativa: #{statusSistema?.oracle_indice_ativo ?? 0}
                </Badge>
              </div>
              <p className="text-muted-foreground">
                {possuiExtracaoSelecionada
                  ? "A extracao Oracle usa a conexao ativa definida em Configuracoes."
                  : "Sem consultas selecionadas, a execucao reutiliza dados ja extraidos do CNPJ."}
              </p>
            </div>
            {statusSistema?.erro_oracle ? (
              <p className="max-w-md text-right text-destructive">
                {statusSistema.erro_oracle}
              </p>
            ) : null}
          </div>

          <div className="flex items-center justify-end gap-3">
            {erroPipeline ? (
              <div className="max-w-md text-right">
                <p className="text-xs text-destructive">{erroPipeline}</p>
                {dicaErroPipeline ? (
                  <p className="text-[11px] text-muted-foreground">
                    {dicaErroPipeline}
                  </p>
                ) : null}
              </div>
            ) : null}
            <Button
              onClick={executarPipeline}
              disabled={
                executando || cnpjAtivo.length !== 14 || extracaoBloqueada
              }
              className="gap-2"
            >
              {executando ? (
                <Loader2 className="h-4 w-4 animate-spin" />
              ) : (
                <Play className="h-4 w-4" />
              )}
              {executando ? "Executando..." : "Executar pipeline"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {resultadoPipeline ? (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between text-sm font-semibold">
              <span>Resultado da execucao</span>
              <Badge
                variant={
                  resultadoPipeline.status === "concluido"
                    ? "default"
                    : "destructive"
                }
              >
                {resultadoPipeline.status}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="grid grid-cols-1 gap-2 text-xs md:grid-cols-4">
              <div className="rounded border p-2">
                <p className="text-muted-foreground">Tabelas geradas</p>
                <p className="font-semibold">
                  {resultadoPipeline.tabelas_geradas.length}
                </p>
              </div>
              <div className="rounded border p-2">
                <p className="text-muted-foreground">Duracao</p>
                <p className="font-semibold">
                  {resultadoPipeline.duracao_ms} ms
                </p>
              </div>
              <div className="rounded border p-2">
                <p className="text-muted-foreground">Erros</p>
                <p className="font-semibold">{totalErrosExecucao}</p>
                {totalErrosExecucao > 0 ? (
                  <p className="text-[11px] text-muted-foreground">
                    {montarResumoErrosExecucao(resultadoPipeline)}
                  </p>
                ) : null}
              </div>
              <div className="rounded border p-2">
                <p className="text-muted-foreground">Extracao</p>
                <p className="font-semibold">
                  {resultadoPipeline.extracao?.status ?? "nao executada"}
                </p>
              </div>
            </div>

            <div className="space-y-2">
              <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                Etapas
              </h3>
              <DataTable
                dados={resultadoPipeline?.etapas || []}
                colunas={["tabela", "status", "registros", "duracao_ms"]}
              />
            </div>

            {resultadoPipeline.erros_total.length > 0 ? (
              <div className="space-y-2">
                <h3 className="text-xs font-semibold uppercase tracking-wider text-destructive">
                  Erros
                </h3>
                {resultadoPipeline.erros_extracao.length > 0 ? (
                  <div className="space-y-2 rounded border border-destructive/30 p-3">
                    <p className="text-xs font-semibold uppercase tracking-wider text-destructive">
                      Extracao Oracle
                    </p>
                    <ul className="list-disc space-y-1 pl-4 text-xs text-destructive">
                      {resultadoPipeline.erros_extracao.map(erro => (
                        <li key={erro}>{erro}</li>
                      ))}
                    </ul>
                    {consultasComErro.length > 0 ? (
                      <p className="text-[11px] text-muted-foreground">
                        Consultas com falha: {consultasComErro.join(", ")}
                      </p>
                    ) : null}
                  </div>
                ) : null}
                {resultadoPipeline.erros_pipeline.length > 0 ? (
                  <div className="space-y-2 rounded border border-destructive/30 p-3">
                    <p className="text-xs font-semibold uppercase tracking-wider text-destructive">
                      Pipeline de tabelas
                    </p>
                    <ul className="list-disc space-y-1 pl-4 text-xs text-destructive">
                      {resultadoPipeline.erros_pipeline.map(erro => (
                        <li key={erro}>{erro}</li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </div>
            ) : null}

            <div className="flex justify-end">
              <Button
                variant="outline"
                size="sm"
                className="gap-2"
                onClick={() => executarPipeline()}
                disabled={executando}
              >
                <RefreshCw className="h-3.5 w-3.5" />
                Reexecutar
              </Button>
            </div>
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}
