import { useEffect, useState } from "react";
import { Link, useLocation } from "wouter";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";
import { useAlvosAnalise, useSistema } from "@/hooks/useAuditApi";
import { classificarStatusOracle } from "@/lib/oracle";
import { ArrowRight, Database, Boxes, Package, Table2, Search, Shield } from "lucide-react";
import { toast } from "sonner";

function normalizarDigitos(valor: string): string {
  return valor.replace(/\D/g, "");
}

function formatarCnpjInput(cnpj: string): string {
  const digits = normalizarDigitos(cnpj).slice(0, 14);
  if (digits.length <= 2) return digits;
  if (digits.length <= 5) return `${digits.slice(0, 2)}.${digits.slice(2)}`;
  if (digits.length <= 8) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5)}`;
  if (digits.length <= 12) return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`;
  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12)}`;
}

function LinhaCnpj({
  alvo,
  aoSelecionar,
}: {
  alvo: any;
  aoSelecionar: (cnpj: string) => void;
}) {
  return (
    <button
      type="button"
      onClick={() => aoSelecionar(alvo.cnpj)}
      className="w-full rounded-lg border border-border/80 bg-card p-3 text-left transition-colors hover:bg-accent/40"
    >
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-2">
          <span className="font-mono text-xs font-semibold text-primary">{formatarCnpj(alvo.cnpj)}</span>
          <Badge variant={alvo.status_pipeline === "completo" ? "default" : "outline"}>
            {alvo.status_pipeline === "completo" ? "Completo" : alvo.status_pipeline === "parcial" ? "Parcial" : "Sem pipeline"}
          </Badge>
        </div>
        <p className="text-sm font-semibold">{alvo.contribuinte ?? "Sem nome"}</p>
        <div className="flex gap-4 text-xs text-muted-foreground">
          <span>IE: {alvo.ie || "-"}</span>
          <span>Parquets: {alvo.total_parquets}</span>
        </div>
      </div>
    </button>
  );
}

export default function Home() {
  const [, navegar] = useLocation();
  const { cnpjAtivo, definirCnpjAtivo } = useCnpj();
  const { data: catalogo, listar } = useAlvosAnalise();
  const { carregarStatus } = useSistema();
  const [statusSistema, setStatusSistema] = useState<any | null>(null);
  const [entradaCnpj, setEntradaCnpj] = useState("");

  useEffect(() => {
    setEntradaCnpj(formatarCnpj(cnpjAtivo));
  }, [cnpjAtivo]);

  useEffect(() => {
    listar().catch(() => undefined);
    carregarStatus().then(setStatusSistema).catch(() => undefined);
  }, [carregarStatus, listar]);

  const alvos = catalogo?.alvos ?? [];
  const badgeOracle = classificarStatusOracle(statusSistema);

  const cnpjDigitado = normalizarDigitos(entradaCnpj).slice(0, 14);

  const alvosFiltrados = entradaCnpj.trim()
    ? alvos.filter((alvo: any) => alvo.cnpj.includes(cnpjDigitado) || (alvo.contribuinte ?? "").toLowerCase().includes(entradaCnpj.toLowerCase()))
    : alvos.slice(0, 10);

  const selecionarCnpj = (cnpj: string) => {
    definirCnpjAtivo(cnpj);
    toast.success("CNPJ selecionado", { description: formatarCnpj(cnpj) });
  };

  const irParaPagina = (rota: string) => {
    if (!cnpjAtivo) {
      toast.error("Selecione um CNPJ primeiro");
      return;
    }
    navegar(rota);
  };

  return (
    <div className="min-h-screen bg-background">
      <div className="mx-auto max-w-7xl px-6 py-8">
        {/* Header */}
        <Card className="mb-6 overflow-hidden border-border/80">
          <CardContent className="grid gap-6 p-6 lg:grid-cols-[1fr_1fr]">
            <div className="space-y-4 bg-[linear-gradient(135deg,rgba(15,23,42,0.98),rgba(30,64,175,0.92))] p-6 text-primary-foreground">
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-white/10">
                  <Shield className="h-5 w-5" />
                </div>
                <div>
                  <p className="text-xs font-semibold uppercase tracking-[0.24em] text-white/70">Audit React</p>
                  <h1 className="text-xl font-bold">Sistema de Auditoria Fiscal</h1>
                </div>
              </div>
              <div className="flex flex-wrap gap-2 text-xs">
                <Badge variant="secondary" className="bg-white/12 text-white">API {statusSistema?.api ?? "..."}</Badge>
                <Badge variant={badgeOracle.variante} className={badgeOracle.variante === "default" ? "bg-white text-slate-950" : ""}>
                  {badgeOracle.texto}
                </Badge>
                <Badge variant="secondary" className="bg-white/12 text-white">CNPJs: {catalogo?.resumo?.total_cnpjs ?? 0}</Badge>
              </div>
            </div>

            <div className="space-y-4 p-6">
              <h2 className="text-sm font-semibold">Módulos Disponíveis</h2>
              <div className="grid gap-2">
                <Button variant="outline" className="h-auto justify-start py-3" onClick={() => irParaPagina("/consulta")}>
                  <Table2 className="mr-2 h-4 w-4" />
                  <div className="text-left">
                    <span className="block text-sm font-semibold">Consulta SQL</span>
                    <span className="block text-xs text-muted-foreground">Visualizar tabelas e dados</span>
                  </div>
                </Button>
                <Button variant="outline" className="h-auto justify-start py-3" onClick={() => irParaPagina("/agregacao-conversao")}>
                  <Boxes className="mr-2 h-4 w-4" />
                  <div className="text-left">
                    <span className="block text-sm font-semibold">Agregação e Conversão</span>
                    <span className="block text-xs text-muted-foreground">Agrupar produtos e fatores</span>
                  </div>
                </Button>
                <Button variant="outline" className="h-auto justify-start py-3" onClick={() => irParaPagina("/estoque")}>
                  <Package className="mr-2 h-4 w-4" />
                  <div className="text-left">
                    <span className="block text-sm font-semibold">Estoque</span>
                    <span className="block text-xs text-muted-foreground">Movimentação e saldos</span>
                  </div>
                </Button>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Seleção de CNPJ */}
        <Card className="mb-6 border-border/80">
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-base">
              <Search className="h-4 w-4 text-primary" />
              Selecionar CNPJ
            </CardTitle>
            <CardDescription>Informe um CNPJ ou selecione da lista abaixo</CardDescription>
          </CardHeader>
          <CardContent>
            <div className="mb-4 flex gap-2">
              <Input
                value={entradaCnpj}
                onChange={(e) => setEntradaCnpj(formatarCnpjInput(e.target.value))}
                placeholder="00.000.000/0000-00"
                className="max-w-sm font-mono"
              />
              <Button onClick={() => cnpjDigitado && selecionarCnpj(cnpjDigitado)}>
                Selecionar
              </Button>
            </div>

            <div className="grid gap-3 lg:grid-cols-2">
              {alvosFiltrados.map((alvo: any) => (
                <LinhaCnpj key={alvo.cnpj} alvo={alvo} aoSelecionar={selecionarCnpj} />
              ))}
            </div>
          </CardContent>
        </Card>

        {/* CNPJ Ativo */}
        {cnpjAtivo && (
          <Card className="border-border/80">
            <CardHeader>
              <CardTitle className="text-base">CNPJ Ativo</CardTitle>
              <CardDescription>
                <span className="font-mono font-semibold">{formatarCnpj(cnpjAtivo)}</span>
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="flex flex-wrap gap-3">
                <Button variant="outline" onClick={() => irParaPagina("/consulta")}>
                  <Table2 className="mr-2 h-4 w-4" /> Consulta
                </Button>
                <Button variant="outline" onClick={() => irParaPagina("/agregacao-conversao")}>
                  <Boxes className="mr-2 h-4 w-4" /> Agregação e Conversão
                </Button>
                <Button variant="outline" onClick={() => irParaPagina("/estoque")}>
                  <Package className="mr-2 h-4 w-4" /> Estoque
                </Button>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </div>
  );
}
