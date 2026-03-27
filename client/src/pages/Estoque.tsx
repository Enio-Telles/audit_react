/*
 * Estoque — Swiss Design Fiscal
 * Subabas: mov_estoque, aba_mensal, aba_anual, produtos_selecionados, id_agrupados, nfe_entrada
 * Baseado na aba Estoque do audit_pyside
 */
import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Package,
  Search,
  Download,
  RefreshCw,
  ArrowUpDown,
  Calendar,
  CalendarDays,
  ShoppingCart,
  Layers,
  FileInput,
  TrendingUp,
  TrendingDown,
  Minus,
} from "lucide-react";
import { toast } from "sonner";

// Mock data for mov_estoque
const MOV_ESTOQUE = Array.from({ length: 12 }).map((_, i) => ({
  id: i + 1,
  id_agrupado: `G${String(Math.floor(i / 3) + 1).padStart(3, "0")}`,
  descricao: [
    "CERVEJA PILSEN 350ML",
    "REFRIGERANTE COLA 2L",
    "AGUA MINERAL 500ML",
    "LEITE INTEGRAL 1L",
  ][Math.floor(i / 3)],
  tipo: i % 3 === 0 ? "ENTRADA" : i % 3 === 1 ? "SAIDA" : "INVENTARIO",
  data: `2026-${String(Math.floor(i / 2) + 1).padStart(2, "0")}-15`,
  quantidade: Math.floor(Math.random() * 500) + 50,
  valor_unitario: (Math.random() * 10 + 1).toFixed(2),
  valor_total: (Math.random() * 5000 + 100).toFixed(2),
  saldo: Math.floor(Math.random() * 300),
}));

const ABA_MENSAL = Array.from({ length: 6 }).map((_, i) => ({
  mes: `2026-${String(i + 1).padStart(2, "0")}`,
  entradas: Math.floor(Math.random() * 10000) + 1000,
  saidas: Math.floor(Math.random() * 8000) + 800,
  saldo_inicial: Math.floor(Math.random() * 5000),
  saldo_final: Math.floor(Math.random() * 5000),
  custo_medio: (Math.random() * 15 + 2).toFixed(2),
  valor_estoque: (Math.random() * 50000 + 5000).toFixed(2),
}));

function SubTabContent({
  title,
  icon: Icon,
  children,
}: {
  title: string;
  icon: typeof Package;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <Icon className="h-4 w-4 text-primary" />
          <h3 className="text-sm font-semibold">{title}</h3>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" className="gap-1.5 text-xs">
            <RefreshCw className="h-3.5 w-3.5" />
            Atualizar
          </Button>
          <Button
            variant="outline"
            size="sm"
            className="gap-1.5 text-xs"
            onClick={() => toast.info("Funcionalidade em desenvolvimento")}
          >
            <Download className="h-3.5 w-3.5" />
            Exportar
          </Button>
        </div>
      </div>
      {children}
    </div>
  );
}

export default function Estoque() {
  const [activeTab, setActiveTab] = useState("mov_estoque");
  const [searchTerm, setSearchTerm] = useState("");

  // ⚡ Bolt: Memoized array filtering and hoisted toLowerCase to prevent O(N) recalculations and redundant string operations on every re-render
  const filteredMovEstoque = useMemo(() => {
    const term = searchTerm.toLowerCase();
    if (!term) return MOV_ESTOQUE;
    return MOV_ESTOQUE.filter(
      mov =>
        mov.descricao.toLowerCase().includes(term) ||
        mov.id_agrupado.toLowerCase().includes(term)
    );
  }, [searchTerm]);

  return (
    <div className="space-y-4 max-w-full">
      {/* KPI summary */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
        {[
          {
            label: "Total Entradas",
            value: "R$ 245.320,00",
            icon: TrendingUp,
            color: "text-green-600",
          },
          {
            label: "Total Saídas",
            value: "R$ 198.450,00",
            icon: TrendingDown,
            color: "text-red-500",
          },
          {
            label: "Saldo Atual",
            value: "R$ 46.870,00",
            icon: Package,
            color: "text-primary",
          },
          {
            label: "Produtos Ativos",
            value: "142",
            icon: Layers,
            color: "text-muted-foreground",
          },
        ].map(kpi => (
          <Card key={kpi.label}>
            <CardContent className="py-3 px-4">
              <p className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
                {kpi.label}
              </p>
              <div className="flex items-center gap-2 mt-1.5">
                <kpi.icon className={`h-4 w-4 ${kpi.color}`} />
                <span className="text-lg font-bold font-mono">{kpi.value}</span>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>

      {/* Search */}
      <div className="relative max-w-md">
        <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
        <Input
          placeholder="Filtrar por descrição ou ID agrupado..."
          value={searchTerm}
          onChange={e => setSearchTerm(e.target.value)}
          className="pl-8 h-9 text-xs"
        />
      </div>

      {/* Sub-tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="flex-wrap h-auto gap-1 p-1">
          <TabsTrigger value="mov_estoque" className="gap-1.5 text-xs">
            <Package className="h-3 w-3" />
            Movimentação
          </TabsTrigger>
          <TabsTrigger value="mensal" className="gap-1.5 text-xs">
            <Calendar className="h-3 w-3" />
            Mensal
          </TabsTrigger>
          <TabsTrigger value="anual" className="gap-1.5 text-xs">
            <CalendarDays className="h-3 w-3" />
            Anual
          </TabsTrigger>
          <TabsTrigger
            value="produtos_selecionados"
            className="gap-1.5 text-xs"
          >
            <ShoppingCart className="h-3 w-3" />
            Prod. Selecionados
          </TabsTrigger>
          <TabsTrigger value="id_agrupados" className="gap-1.5 text-xs">
            <Layers className="h-3 w-3" />
            ID Agrupados
          </TabsTrigger>
          <TabsTrigger value="nfe_entrada" className="gap-1.5 text-xs">
            <FileInput className="h-3 w-3" />
            NFe Entrada
          </TabsTrigger>
        </TabsList>

        {/* Movimentação de Estoque */}
        <TabsContent value="mov_estoque" className="mt-4">
          <SubTabContent title="Movimentação de Estoque" icon={Package}>
            <Card>
              <CardContent className="p-0">
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead className="bg-muted/50 sticky top-0">
                      <tr>
                        {[
                          "ID Agrupado",
                          "Descrição",
                          "Tipo",
                          "Data",
                          "Quantidade",
                          "Vlr Unit",
                          "Vlr Total",
                          "Saldo",
                        ].map(col => (
                          <th
                            key={col}
                            className="px-3 py-2.5 text-left font-semibold text-muted-foreground uppercase tracking-wider whitespace-nowrap border-b border-border"
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {filteredMovEstoque.map(mov => (
                        <tr
                          key={mov.id}
                          className="border-b border-border/50 hover:bg-accent/30 transition-colors"
                        >
                          <td className="px-3 py-2 font-mono font-medium">
                            {mov.id_agrupado}
                          </td>
                          <td className="px-3 py-2 max-w-xs truncate">
                            {mov.descricao}
                          </td>
                          <td className="px-3 py-2">
                            <Badge
                              variant="outline"
                              className={`text-[10px] font-mono ${
                                mov.tipo === "ENTRADA"
                                  ? "text-green-700 border-green-200 bg-green-50"
                                  : mov.tipo === "SAIDA"
                                    ? "text-red-600 border-red-200 bg-red-50"
                                    : "text-blue-600 border-blue-200 bg-blue-50"
                              }`}
                            >
                              {mov.tipo}
                            </Badge>
                          </td>
                          <td className="px-3 py-2 font-mono">{mov.data}</td>
                          <td className="px-3 py-2 font-mono text-right">
                            {mov.quantidade}
                          </td>
                          <td className="px-3 py-2 font-mono text-right">
                            R$ {mov.valor_unitario}
                          </td>
                          <td className="px-3 py-2 font-mono text-right">
                            R$ {mov.valor_total}
                          </td>
                          <td className="px-3 py-2 font-mono text-right font-medium">
                            {mov.saldo}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          </SubTabContent>
        </TabsContent>

        {/* Mensal */}
        <TabsContent value="mensal" className="mt-4">
          <SubTabContent title="Consolidação Mensal" icon={Calendar}>
            <Card>
              <CardContent className="p-0">
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead className="bg-muted/50 sticky top-0">
                      <tr>
                        {[
                          "Mês",
                          "Entradas",
                          "Saídas",
                          "Saldo Inicial",
                          "Saldo Final",
                          "Custo Médio",
                          "Valor Estoque",
                        ].map(col => (
                          <th
                            key={col}
                            className="px-3 py-2.5 text-left font-semibold text-muted-foreground uppercase tracking-wider whitespace-nowrap border-b border-border"
                          >
                            {col}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {ABA_MENSAL.map(mes => (
                        <tr
                          key={mes.mes}
                          className="border-b border-border/50 hover:bg-accent/30 transition-colors"
                        >
                          <td className="px-3 py-2 font-mono font-medium">
                            {mes.mes}
                          </td>
                          <td className="px-3 py-2 font-mono text-right text-green-700">
                            {mes.entradas.toLocaleString("pt-BR")}
                          </td>
                          <td className="px-3 py-2 font-mono text-right text-red-600">
                            {mes.saidas.toLocaleString("pt-BR")}
                          </td>
                          <td className="px-3 py-2 font-mono text-right">
                            {mes.saldo_inicial.toLocaleString("pt-BR")}
                          </td>
                          <td className="px-3 py-2 font-mono text-right font-medium">
                            {mes.saldo_final.toLocaleString("pt-BR")}
                          </td>
                          <td className="px-3 py-2 font-mono text-right">
                            R$ {mes.custo_medio}
                          </td>
                          <td className="px-3 py-2 font-mono text-right font-medium">
                            R$ {mes.valor_estoque}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </CardContent>
            </Card>
          </SubTabContent>
        </TabsContent>

        {/* Anual */}
        <TabsContent value="anual" className="mt-4">
          <SubTabContent title="Consolidação Anual" icon={CalendarDays}>
            <Card>
              <CardContent className="pt-6">
                <div className="flex flex-col items-center justify-center py-8 text-center">
                  <CalendarDays className="h-10 w-10 text-muted-foreground/30 mb-3" />
                  <p className="text-sm font-medium text-muted-foreground">
                    Dados anuais serão exibidos aqui
                  </p>
                  <p className="text-xs text-muted-foreground/60 mt-1">
                    Execute o pipeline para gerar a consolidação anual
                  </p>
                </div>
              </CardContent>
            </Card>
          </SubTabContent>
        </TabsContent>

        {/* Produtos Selecionados */}
        <TabsContent value="produtos_selecionados" className="mt-4">
          <SubTabContent title="Produtos Selecionados" icon={ShoppingCart}>
            <Card>
              <CardContent className="pt-6">
                <div className="flex flex-col items-center justify-center py-8 text-center">
                  <ShoppingCart className="h-10 w-10 text-muted-foreground/30 mb-3" />
                  <p className="text-sm font-medium text-muted-foreground">
                    Produtos selecionados para análise
                  </p>
                  <p className="text-xs text-muted-foreground/60 mt-1">
                    Selecione produtos na aba de Agregação para análise
                    detalhada
                  </p>
                </div>
              </CardContent>
            </Card>
          </SubTabContent>
        </TabsContent>

        {/* ID Agrupados */}
        <TabsContent value="id_agrupados" className="mt-4">
          <SubTabContent title="IDs Agrupados" icon={Layers}>
            <Card>
              <CardContent className="pt-6">
                <div className="flex flex-col items-center justify-center py-8 text-center">
                  <Layers className="h-10 w-10 text-muted-foreground/30 mb-3" />
                  <p className="text-sm font-medium text-muted-foreground">
                    Mapeamento de IDs agrupados
                  </p>
                  <p className="text-xs text-muted-foreground/60 mt-1">
                    Gerado automaticamente após a agregação de produtos
                  </p>
                </div>
              </CardContent>
            </Card>
          </SubTabContent>
        </TabsContent>

        {/* NFe Entrada */}
        <TabsContent value="nfe_entrada" className="mt-4">
          <SubTabContent title="NFe Entrada" icon={FileInput}>
            <Card>
              <CardContent className="pt-6">
                <div className="flex flex-col items-center justify-center py-8 text-center">
                  <FileInput className="h-10 w-10 text-muted-foreground/30 mb-3" />
                  <p className="text-sm font-medium text-muted-foreground">
                    Notas fiscais de entrada
                  </p>
                  <p className="text-xs text-muted-foreground/60 mt-1">
                    Dados de NFe de entrada enriquecidos com classificação CO
                    SEFIN
                  </p>
                </div>
              </CardContent>
            </Card>
          </SubTabContent>
        </TabsContent>
      </Tabs>
    </div>
  );
}
