/*
 * Agregação — Swiss Design Fiscal
 * Tabela agrupável, seleção de linhas, agregação manual, reprocessamento
 * Baseado na aba Agregação do audit_pyside
 */
import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Boxes,
  Search,
  Merge,
  Undo2,
  RefreshCw,
  ArrowUpDown,
  CheckCircle2,
  History,
  Wand2,
  Filter,
  GitBranch,
} from "lucide-react";
import { toast } from "sonner";

// Mock data
const PRODUTOS_EXEMPLO = Array.from({ length: 20 }).map((_, i) => ({
  id: i + 1,
  descricao: [
    "CERVEJA PILSEN 350ML LATA",
    "CERVEJA PILSEN 350 ML LT",
    "CERV PILSEN 350ML",
    "REFRIGERANTE COLA 2L PET",
    "REFRIG COLA 2LT",
    "AGUA MINERAL 500ML",
    "AGUA MIN 500 ML",
    "LEITE INTEGRAL 1L",
    "LEITE INT 1LT CX",
    "ACUCAR CRISTAL 1KG",
    "ACUCAR CRIST 1 KG",
    "ARROZ TIPO 1 5KG",
    "FEIJAO PRETO 1KG",
    "OLEO SOJA 900ML",
    "FARINHA TRIGO 1KG",
    "CAFE TORRADO 500G",
    "SAL REFINADO 1KG",
    "MACARRAO ESPAGUETE 500G",
    "BISCOITO CREAM CRACKER 200G",
    "MARGARINA 500G",
  ][i],
  ncm: [
    "2203.00.00",
    "2203.00.00",
    "2203.00.00",
    "2202.10.00",
    "2202.10.00",
    "2201.10.00",
    "2201.10.00",
    "0401.10.10",
    "0401.10.10",
    "1701.99.00",
    "1701.99.00",
    "1006.30.21",
    "0713.33.19",
    "1507.90.11",
    "1101.00.10",
    "0901.21.00",
    "2501.00.20",
    "1902.19.00",
    "1905.31.00",
    "1517.10.00",
  ][i],
  cest: [
    "03.001.00",
    "03.001.00",
    "03.001.00",
    "03.011.00",
    "03.011.00",
    "03.024.00",
    "03.024.00",
    "",
    "",
    "17.073.00",
    "17.073.00",
    "17.047.00",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
    "",
  ][i],
  unidade: "UN",
  qtd_nfe: Math.floor(Math.random() * 500) + 10,
  grupo: i < 3 ? "G001" : i < 5 ? "G002" : i < 7 ? "G003" : null,
}));

export default function Agregacao() {
  const [searchTerm, setSearchTerm] = useState("");
  const [selectedRows, setSelectedRows] = useState<number[]>([]);
  const [activeTab, setActiveTab] = useState("candidatos");

  const toggleRow = (id: number) => {
    setSelectedRows(prev =>
      prev.includes(id) ? prev.filter(r => r !== id) : [...prev, id]
    );
  };

  const handleAgregar = () => {
    if (selectedRows.length < 2) {
      toast.error("Selecione ao menos 2 produtos para agregar");
      return;
    }
    toast.info("Funcionalidade em desenvolvimento", {
      description: `${selectedRows.length} produtos selecionados para agregação. O backend precisa estar conectado.`,
    });
  };

  // ⚡ Bolt: Memoized expensive array filtering and extracted toLowerCase to prevent unnecessary re-renders when selecting rows
  const filteredProdutos = useMemo(() => {
    const term = searchTerm.toLowerCase();
    return PRODUTOS_EXEMPLO.filter(
      p =>
        p.descricao.toLowerCase().includes(term) || p.ncm.includes(searchTerm)
    );
  }, [searchTerm]);

  const grupos = useMemo(() => {
    return Array.from(
      new Set(PRODUTOS_EXEMPLO.filter(p => p.grupo).map(p => p.grupo))
    );
  }, []);

  return (
    <div className="space-y-4 max-w-full">
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <div className="flex items-center justify-between">
          <TabsList>
            <TabsTrigger value="candidatos" className="gap-2">
              <Boxes className="h-3.5 w-3.5" />
              Candidatos
            </TabsTrigger>
            <TabsTrigger value="agrupados" className="gap-2">
              <GitBranch className="h-3.5 w-3.5" />
              Grupos Existentes
            </TabsTrigger>
            <TabsTrigger value="historico" className="gap-2">
              <History className="h-3.5 w-3.5" />
              Histórico
            </TabsTrigger>
          </TabsList>
          <div className="flex items-center gap-2">
            <Button variant="outline" size="sm" className="gap-1.5 text-xs">
              <Wand2 className="h-3.5 w-3.5" />
              Sugerir Pares
            </Button>
            <Button variant="outline" size="sm" className="gap-1.5 text-xs">
              <RefreshCw className="h-3.5 w-3.5" />
              Reprocessar
            </Button>
          </div>
        </div>

        <TabsContent value="candidatos" className="mt-4 space-y-4">
          {/* Action bar */}
          <Card>
            <CardContent className="py-3 px-4">
              <div className="flex items-center gap-3">
                <div className="relative flex-1 max-w-md">
                  <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-muted-foreground" />
                  <Input
                    placeholder="Filtrar por descrição, NCM, CEST..."
                    value={searchTerm}
                    onChange={e => setSearchTerm(e.target.value)}
                    className="pl-8 h-8 text-xs"
                  />
                </div>
                <Badge variant="secondary" className="text-xs">
                  {selectedRows.length} selecionados
                </Badge>
                <Button
                  size="sm"
                  className="gap-1.5 text-xs"
                  onClick={handleAgregar}
                  disabled={selectedRows.length < 2}
                >
                  <Merge className="h-3.5 w-3.5" />
                  Agregar Selecionados
                </Button>
                {selectedRows.length > 0 && (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="gap-1.5 text-xs"
                    onClick={() => setSelectedRows([])}
                  >
                    <Undo2 className="h-3.5 w-3.5" />
                    Limpar
                  </Button>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Table */}
          <Card>
            <CardContent className="p-0">
              <div className="overflow-x-auto">
                <table className="w-full text-xs">
                  <thead className="bg-muted/50 sticky top-0">
                    <tr>
                      <th className="px-3 py-2.5 w-10 border-b border-border">
                        <Checkbox
                          checked={
                            selectedRows.length === filteredProdutos.length &&
                            filteredProdutos.length > 0
                          }
                          onCheckedChange={checked => {
                            if (checked) {
                              setSelectedRows(filteredProdutos.map(p => p.id));
                            } else {
                              setSelectedRows([]);
                            }
                          }}
                        />
                      </th>
                      {[
                        "Descrição",
                        "NCM",
                        "CEST",
                        "Unid",
                        "Qtd NFe",
                        "Grupo",
                      ].map(col => (
                        <th
                          key={col}
                          className="px-3 py-2.5 text-left font-semibold text-muted-foreground uppercase tracking-wider whitespace-nowrap border-b border-border"
                        >
                          <button className="flex items-center gap-1 hover:text-foreground transition-colors">
                            {col}
                            <ArrowUpDown className="h-3 w-3" />
                          </button>
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {filteredProdutos.map(produto => (
                      <tr
                        key={produto.id}
                        className={`border-b border-border/50 transition-colors ${
                          selectedRows.includes(produto.id)
                            ? "bg-primary/5"
                            : "hover:bg-accent/30"
                        }`}
                      >
                        <td className="px-3 py-2">
                          <Checkbox
                            checked={selectedRows.includes(produto.id)}
                            onCheckedChange={() => toggleRow(produto.id)}
                          />
                        </td>
                        <td className="px-3 py-2 max-w-sm">
                          <span className="font-medium">
                            {produto.descricao}
                          </span>
                        </td>
                        <td className="px-3 py-2 font-mono">{produto.ncm}</td>
                        <td className="px-3 py-2 font-mono">
                          {produto.cest || "—"}
                        </td>
                        <td className="px-3 py-2 font-mono">
                          {produto.unidade}
                        </td>
                        <td className="px-3 py-2 font-mono text-right">
                          {produto.qtd_nfe}
                        </td>
                        <td className="px-3 py-2">
                          {produto.grupo ? (
                            <Badge
                              variant="outline"
                              className="text-[10px] font-mono"
                            >
                              {produto.grupo}
                            </Badge>
                          ) : (
                            <span className="text-muted-foreground/40">—</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="agrupados" className="mt-4">
          <Card>
            <CardContent className="pt-6">
              <div className="space-y-4">
                {grupos.map(grupo => {
                  const membros = PRODUTOS_EXEMPLO.filter(
                    p => p.grupo === grupo
                  );
                  return (
                    <div
                      key={grupo}
                      className="border border-border rounded-lg p-4"
                    >
                      <div className="flex items-center justify-between mb-3">
                        <div className="flex items-center gap-2">
                          <Badge className="font-mono text-xs">{grupo}</Badge>
                          <span className="text-xs text-muted-foreground">
                            {membros.length} produtos
                          </span>
                        </div>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="text-xs gap-1.5"
                        >
                          <Undo2 className="h-3 w-3" />
                          Desagregar
                        </Button>
                      </div>
                      <div className="space-y-1">
                        {membros.map(m => (
                          <div
                            key={m.id}
                            className="flex items-center gap-3 px-3 py-1.5 rounded bg-muted/30 text-xs"
                          >
                            <CheckCircle2 className="h-3 w-3 text-green-600 shrink-0" />
                            <span className="flex-1">{m.descricao}</span>
                            <span className="font-mono text-muted-foreground">
                              {m.ncm}
                            </span>
                          </div>
                        ))}
                      </div>
                    </div>
                  );
                })}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="historico" className="mt-4">
          <Card>
            <CardContent className="pt-6">
              <div className="flex flex-col items-center justify-center py-12 text-center">
                <History className="h-10 w-10 text-muted-foreground/30 mb-3" />
                <p className="text-sm font-medium text-muted-foreground">
                  Nenhum histórico de agregação
                </p>
                <p className="text-xs text-muted-foreground/60 mt-1">
                  As operações de agregação serão registradas aqui
                </p>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
