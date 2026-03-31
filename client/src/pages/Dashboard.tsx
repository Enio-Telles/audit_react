import { useEffect, useMemo, useState } from "react";
import { Link } from "wouter";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Activity, ArrowRight, Database, Shield } from "lucide-react";
import { useHealthCheck, usePipeline, useSistema, useTabelas } from "@/hooks/useAuditApi";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";
import { classificarStatusOracle } from "@/lib/oracle";
import type { SistemaStatus } from "@/types/audit";

function Kpi({ titulo, valor }: { titulo: string; valor: string | number }) {
  return (
    <Card>
      <CardContent className="pt-4">
        <p className="text-xs uppercase tracking-wider text-muted-foreground">{titulo}</p>
        <p className="mt-1 text-xl font-semibold">{valor}</p>
      </CardContent>
    </Card>
  );
}

export default function Dashboard() {
  const { cnpjAtivo } = useCnpj();
  const { data: health, check } = useHealthCheck();
  const { carregarStatus } = useSistema();
  const { verificarStatus } = usePipeline();
  const { listar } = useTabelas();

  const [statusSistema, setStatusSistema] = useState<SistemaStatus | null>(null);
  const [statusPipeline, setStatusPipeline] = useState<{ completo: boolean; tabelas: Record<string, boolean> } | null>(null);
  const [quantidadeTabelas, setQuantidadeTabelas] = useState(0);

  useEffect(() => {
    check();
    carregarStatus().then(setStatusSistema).catch(() => undefined);
  }, [carregarStatus, check]);

  useEffect(() => {
    if (!cnpjAtivo) {
      setStatusPipeline(null);
      setQuantidadeTabelas(0);
      return;
    }

    verificarStatus(cnpjAtivo).then(setStatusPipeline).catch(() => setStatusPipeline(null));
    listar(cnpjAtivo)
      .then((tabelas) => setQuantidadeTabelas(tabelas.length))
      .catch(() => setQuantidadeTabelas(0));
  }, [cnpjAtivo, listar, verificarStatus]);

  const tabelasCompletas = useMemo(() => {
    if (!statusPipeline) return 0;
    return Object.values(statusPipeline.tabelas).filter(Boolean).length;
  }, [statusPipeline]);

  const badgeOracle = useMemo(() => classificarStatusOracle(statusSistema), [statusSistema]);

  return (
    <div className="mx-auto max-w-6xl space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-lg">
            <Shield className="h-5 w-5 text-primary" />
            Audit React
          </CardTitle>
        </CardHeader>
        <CardContent className="text-sm text-muted-foreground">
          <p>
            Plataforma de auditoria fiscal com pipeline real por CNPJ. CNPJ ativo: {cnpjAtivo ? formatarCnpj(cnpjAtivo) : "nao selecionado"}.
          </p>
          <div className="mt-3 flex flex-wrap gap-2">
            <Badge variant={health?.status === "ok" ? "default" : "secondary"}>API {health?.status ?? "indisponivel"}</Badge>
            <Badge variant={badgeOracle.variante}>{badgeOracle.texto}</Badge>
            <Badge variant="outline">Conexao Oracle ativa: #{statusSistema?.oracle_indice_ativo ?? 0}</Badge>
            <Badge variant="outline">Consultas SQL: {statusSistema?.consultas_disponiveis?.length ?? 0}</Badge>
          </div>
          {statusSistema?.erro_oracle ? <p className="mt-3 text-xs text-destructive">{statusSistema.erro_oracle}</p> : null}
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
        <Kpi titulo="CNPJ ativo" valor={cnpjAtivo ? formatarCnpj(cnpjAtivo) : "-"} />
        <Kpi titulo="Tabelas no storage" valor={quantidadeTabelas} />
        <Kpi titulo="Tabelas validadas" valor={tabelasCompletas} />
        <Kpi titulo="Pipeline" valor={statusPipeline?.completo ? "Completo" : "Pendente"} />
      </div>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Fluxo principal</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2">
          {[
            ["/extracao", "Extracao e pipeline"],
            ["/consulta", "Consulta das tabelas"],
            ["/mapeamento-oracle", "Mapa raiz Oracle"],
            ["/agregacao", "Edicao de agregacao"],
            ["/conversao", "Edicao de fatores"],
            ["/estoque", "Analise de estoque"],
          ].map(([rota, titulo]) => (
            <Link key={rota} href={rota}>
              <button className="flex w-full items-center justify-between rounded border px-3 py-2 text-left text-sm hover:bg-accent/40">
                <span>{titulo}</span>
                <ArrowRight className="h-4 w-4 text-muted-foreground" />
              </button>
            </Link>
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Atividade</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex items-center gap-3 text-sm text-muted-foreground">
            <Activity className="h-4 w-4" />
            <span>Use a pagina de Extracao para iniciar uma execucao real do pipeline.</span>
          </div>
          <div className="mt-4">
            <Link href="/extracao">
              <Button className="gap-2">
                <Database className="h-4 w-4" />
                Ir para Extracao
              </Button>
            </Link>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
