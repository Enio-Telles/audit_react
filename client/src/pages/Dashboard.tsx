/*
 * Dashboard — Swiss Design Fiscal
 * KPI cards, status do pipeline, atalhos para fluxo principal
 */
import { Link } from "wouter";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Database,
  Table2,
  Boxes,
  ArrowLeftRight,
  Package,
  ArrowRight,
  Clock,
  FileSpreadsheet,
  Shield,
  Activity,
} from "lucide-react";

const PIPELINE_IMG = "https://d2xsxph8kpxj0f.cloudfront.net/310419663026769585/Mc4oB7aYbzdrCVUYiRatje/pipeline-illustration-o2UtNHnKpPDRk2Y9DD9XvR.webp";

function KpiCard({ label, value, icon: Icon, accent = false }: {
  label: string;
  value: string;
  icon: typeof Database;
  accent?: boolean;
}) {
  return (
    <Card className={accent ? "border-primary/30 bg-primary/5" : ""}>
      <CardContent className="pt-5 pb-4 px-5">
        <div className="flex items-start justify-between">
          <div>
            <p className="text-[11px] font-semibold uppercase tracking-widest text-muted-foreground mb-2">
              {label}
            </p>
            <p className={`text-2xl font-bold tracking-tight ${accent ? "text-primary" : "text-foreground"}`}>
              {value}
            </p>
          </div>
          <div className={`flex items-center justify-center w-10 h-10 rounded-lg ${accent ? "bg-primary/10" : "bg-muted"}`}>
            <Icon className={`h-5 w-5 ${accent ? "text-primary" : "text-muted-foreground"}`} />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function QuickAction({ href, label, description, icon: Icon }: {
  href: string;
  label: string;
  description: string;
  icon: typeof Database;
}) {
  return (
    <Link href={href}>
      <div className="flex items-center gap-4 p-4 rounded-lg border border-border bg-card hover:bg-accent/50 transition-colors group cursor-pointer">
        <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-muted group-hover:bg-primary/10 transition-colors">
          <Icon className="h-5 w-5 text-muted-foreground group-hover:text-primary transition-colors" />
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold text-foreground">{label}</p>
          <p className="text-xs text-muted-foreground">{description}</p>
        </div>
        <ArrowRight className="h-4 w-4 text-muted-foreground/50 group-hover:text-primary transition-colors" />
      </div>
    </Link>
  );
}

export default function Dashboard() {
  return (
    <div className="space-y-6 max-w-6xl">
      {/* Hero section */}
      <div className="relative rounded-xl overflow-hidden border border-border">
        <div
          className="absolute inset-0 opacity-20"
          style={{ backgroundImage: `url(${PIPELINE_IMG})`, backgroundSize: "cover", backgroundPosition: "center" }}
        />
        <div className="relative px-6 py-8 bg-gradient-to-r from-background/95 via-background/80 to-background/60">
          <div className="flex items-center gap-3 mb-3">
            <div className="flex items-center justify-center w-10 h-10 rounded-lg bg-primary">
              <Shield className="h-5 w-5 text-primary-foreground" />
            </div>
            <div>
              <h2 className="text-xl font-bold tracking-tight text-foreground">
                Audit React
              </h2>
              <p className="text-xs text-muted-foreground font-medium">
                Sistema de Auditoria e Analise Fiscal
              </p>
            </div>
          </div>
          <p className="text-sm text-muted-foreground max-w-xl leading-relaxed">
            Pipeline completo de auditoria fiscal: extração Oracle, geração de tabelas analíticas,
            agregação de produtos, conversão de unidades e cálculos de estoque. Selecione um CNPJ
            para iniciar.
          </p>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KpiCard label="CNPJs Auditados" value="0" icon={Shield} accent />
        <KpiCard label="Tabelas Geradas" value="0" icon={FileSpreadsheet} />
        <KpiCard label="Parquets Ativos" value="0" icon={Database} />
        <KpiCard label="Ultimo Pipeline" value="—" icon={Clock} />
      </div>

      {/* Quick Actions + Recent Activity */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Quick Actions */}
        <div className="lg:col-span-2 space-y-3">
          <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
            Fluxo Principal
          </h3>
          <div className="space-y-2">
            <QuickAction
              href="/extracao"
              label="1. Extração"
              description="Selecionar CNPJ, consultas SQL e extrair dados do Oracle"
              icon={Database}
            />
            <QuickAction
              href="/consulta"
              label="2. Consulta"
              description="Abrir e filtrar tabelas Parquet geradas"
              icon={Table2}
            />
            <QuickAction
              href="/agregacao"
              label="3. Agregação"
              description="Agrupar produtos por descrição, NCM e CEST"
              icon={Boxes}
            />
            <QuickAction
              href="/conversao"
              label="4. Conversão"
              description="Editar fatores de conversão e unidades de referência"
              icon={ArrowLeftRight}
            />
            <QuickAction
              href="/estoque"
              label="5. Estoque"
              description="Movimentação, saldos mensais, anuais e omissões"
              icon={Package}
            />
          </div>
        </div>

        {/* Recent Activity */}
        <div className="space-y-3">
          <h3 className="text-sm font-bold uppercase tracking-widest text-muted-foreground">
            Atividade Recente
          </h3>
          <Card>
            <CardContent className="pt-5 pb-4">
              <div className="flex flex-col items-center justify-center py-8 text-center">
                <Activity className="h-8 w-8 text-muted-foreground/30 mb-3" />
                <p className="text-sm text-muted-foreground">
                  Nenhuma atividade registrada
                </p>
                <p className="text-xs text-muted-foreground/60 mt-1">
                  Inicie uma extração para começar
                </p>
                <Link href="/extracao">
                  <Button variant="outline" size="sm" className="mt-4 gap-2">
                    <Database className="h-3.5 w-3.5" />
                    Iniciar Extração
                  </Button>
                </Link>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="pb-3">
              <CardTitle className="text-sm font-semibold">Status do Sistema</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Backend Python</span>
                <Badge variant="outline" className="text-xs font-mono">Aguardando</Badge>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Oracle</span>
                <Badge variant="outline" className="text-xs font-mono">Desconectado</Badge>
              </div>
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Parquets</span>
                <Badge variant="outline" className="text-xs font-mono">0 arquivos</Badge>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
