/*
 * Consulta — Swiss Design Fiscal
 * Listar, abrir, filtrar e exportar tabelas Parquet
 * Baseado em Tabelas.tsx do sefin_audit_5
 */
import { useState, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table2,
  FolderOpen,
  Search,
  Filter,
  Download,
  ChevronLeft,
  ChevronRight,
  ArrowUpDown,
  RefreshCw,
  FileSpreadsheet,
  Columns3,
} from "lucide-react";
import { toast } from "sonner";

const EMPTY_IMG =
  "https://d2xsxph8kpxj0f.cloudfront.net/310419663026769585/Mc4oB7aYbzdrCVUYiRatje/empty-state-data-cF9dEXpBzGYMu4F6vC5B9c.webp";

// Mock data for demonstration
const TABELAS_EXEMPLO = [
  {
    nome: "produtos_unidades",
    registros: 15420,
    colunas: 12,
    tamanho: "2.3 MB",
    atualizado: "2026-03-25",
  },
  {
    nome: "produtos",
    registros: 8730,
    colunas: 8,
    tamanho: "1.1 MB",
    atualizado: "2026-03-25",
  },
  {
    nome: "produtos_agrupados",
    registros: 4215,
    colunas: 14,
    tamanho: "890 KB",
    atualizado: "2026-03-25",
  },
  {
    nome: "produtos_final",
    registros: 4215,
    colunas: 16,
    tamanho: "1.2 MB",
    atualizado: "2026-03-25",
  },
  {
    nome: "fatores_conversao",
    registros: 3890,
    colunas: 10,
    tamanho: "650 KB",
    atualizado: "2026-03-25",
  },
  {
    nome: "mov_estoque",
    registros: 45230,
    colunas: 18,
    tamanho: "8.5 MB",
    atualizado: "2026-03-25",
  },
  {
    nome: "aba_mensal",
    registros: 12400,
    colunas: 15,
    tamanho: "3.2 MB",
    atualizado: "2026-03-25",
  },
  {
    nome: "aba_anual",
    registros: 4215,
    colunas: 12,
    tamanho: "1.0 MB",
    atualizado: "2026-03-25",
  },
];

export default function Consulta() {
  const [selectedTable, setSelectedTable] = useState<string | null>(null);
  const [searchTerm, setSearchTerm] = useState("");
  const [filterColumn, setFilterColumn] = useState("todas");
  const [filterText, setFilterText] = useState("");
  const [page, setPage] = useState(1);

  // ⚡ Bolt: Memoized array filtering and hoisted toLowerCase to prevent O(N) recalculations and redundant string operations on every re-render
  const filteredTables = useMemo(() => {
    const term = searchTerm.toLowerCase();
    return TABELAS_EXEMPLO.filter(t => t.nome.toLowerCase().includes(term));
  }, [searchTerm]);

  const handleExport = () => {
    toast.info("Funcionalidade em desenvolvimento", {
      description:
        "A exportação será habilitada quando o backend estiver conectado.",
    });
  };

  return (
    <div className="space-y-4 max-w-full">
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-4 h-[calc(100vh-10rem)]">
        {/* File browser sidebar */}
        <div className="lg:col-span-1">
          <Card className="h-full flex flex-col">
            <CardHeader className="pb-3 shrink-0">
              <CardTitle className="text-sm font-semibold flex items-center gap-2">
                <FolderOpen className="h-4 w-4 text-primary" />
                Tabelas Disponíveis
              </CardTitle>
              <div className="relative mt-2">
                <Search className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-muted-foreground" />
                <Input
                  placeholder="Buscar tabela..."
                  value={searchTerm}
                  onChange={e => setSearchTerm(e.target.value)}
                  className="pl-8 h-8 text-xs"
                />
              </div>
            </CardHeader>
            <CardContent className="flex-1 overflow-hidden p-0">
              <ScrollArea className="h-full px-3 pb-3">
                <div className="space-y-1">
                  {filteredTables.map(tabela => (
                    <button
                      key={tabela.nome}
                      onClick={() => setSelectedTable(tabela.nome)}
                      className={`w-full text-left px-3 py-2.5 rounded-md transition-colors ${
                        selectedTable === tabela.nome
                          ? "bg-primary/10 border border-primary/20"
                          : "hover:bg-accent/50"
                      }`}
                    >
                      <div className="flex items-center gap-2">
                        <FileSpreadsheet
                          className={`h-3.5 w-3.5 shrink-0 ${
                            selectedTable === tabela.nome
                              ? "text-primary"
                              : "text-muted-foreground"
                          }`}
                        />
                        <span className="text-xs font-medium font-mono truncate">
                          {tabela.nome}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 mt-1 ml-5.5">
                        <span className="text-[10px] text-muted-foreground">
                          {tabela.registros.toLocaleString("pt-BR")} registros
                        </span>
                        <span className="text-[10px] text-muted-foreground/50">
                          |
                        </span>
                        <span className="text-[10px] text-muted-foreground">
                          {tabela.tamanho}
                        </span>
                      </div>
                    </button>
                  ))}
                </div>
              </ScrollArea>
            </CardContent>
          </Card>
        </div>

        {/* Table viewer */}
        <div className="lg:col-span-3">
          <Card className="h-full flex flex-col">
            {selectedTable ? (
              <>
                <CardHeader className="pb-3 shrink-0">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-3">
                      <CardTitle className="text-sm font-semibold font-mono">
                        {selectedTable}
                      </CardTitle>
                      <Badge
                        variant="outline"
                        className="text-[10px] font-mono"
                      >
                        {TABELAS_EXEMPLO.find(
                          t => t.nome === selectedTable
                        )?.registros.toLocaleString("pt-BR")}{" "}
                        registros
                      </Badge>
                    </div>
                    <div className="flex items-center gap-2">
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-7 text-xs gap-1.5"
                      >
                        <Columns3 className="h-3 w-3" />
                        Colunas
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-7 text-xs gap-1.5"
                        onClick={handleExport}
                      >
                        <Download className="h-3 w-3" />
                        Exportar
                      </Button>
                      <Button
                        variant="outline"
                        size="sm"
                        className="h-7 text-xs gap-1.5"
                      >
                        <RefreshCw className="h-3 w-3" />
                      </Button>
                    </div>
                  </div>
                  {/* Filters */}
                  <div className="flex items-center gap-2 mt-3">
                    <Select
                      value={filterColumn}
                      onValueChange={setFilterColumn}
                    >
                      <SelectTrigger className="w-40 h-8 text-xs">
                        <SelectValue placeholder="Coluna" />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="todas">Todas as colunas</SelectItem>
                        <SelectItem value="descricao">Descrição</SelectItem>
                        <SelectItem value="ncm">NCM</SelectItem>
                        <SelectItem value="cest">CEST</SelectItem>
                      </SelectContent>
                    </Select>
                    <div className="relative flex-1">
                      <Filter className="absolute left-2.5 top-2 h-3.5 w-3.5 text-muted-foreground" />
                      <Input
                        placeholder="Filtrar registros..."
                        value={filterText}
                        onChange={e => setFilterText(e.target.value)}
                        className="pl-8 h-8 text-xs"
                      />
                    </div>
                    <Button variant="outline" size="sm" className="h-8 text-xs">
                      Aplicar
                    </Button>
                  </div>
                </CardHeader>
                <CardContent className="flex-1 overflow-auto p-0">
                  {/* Table placeholder */}
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead className="bg-muted/50 sticky top-0">
                        <tr>
                          {[
                            "#",
                            "ID",
                            "Descrição",
                            "NCM",
                            "CEST",
                            "Unidade",
                            "Valor",
                            "Qtd",
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
                        {Array.from({ length: 15 }).map((_, i) => (
                          <tr
                            key={i}
                            className="border-b border-border/50 hover:bg-accent/30 transition-colors"
                          >
                            <td className="px-3 py-2 font-mono text-muted-foreground">
                              {i + 1}
                            </td>
                            <td className="px-3 py-2 font-mono">{1000 + i}</td>
                            <td className="px-3 py-2 max-w-xs truncate">
                              Produto exemplo {i + 1} - Descrição detalhada
                            </td>
                            <td className="px-3 py-2 font-mono">2106.90.10</td>
                            <td className="px-3 py-2 font-mono">17.031.00</td>
                            <td className="px-3 py-2 font-mono">UN</td>
                            <td className="px-3 py-2 font-mono text-right">
                              R$ {(Math.random() * 100).toFixed(2)}
                            </td>
                            <td className="px-3 py-2 font-mono text-right">
                              {Math.floor(Math.random() * 1000)}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </CardContent>
                {/* Pagination */}
                <div className="flex items-center justify-between px-4 py-2.5 border-t border-border shrink-0">
                  <span className="text-xs text-muted-foreground">
                    Mostrando 1-50 de{" "}
                    {TABELAS_EXEMPLO.find(
                      t => t.nome === selectedTable
                    )?.registros.toLocaleString("pt-BR")}
                  </span>
                  <div className="flex items-center gap-1">
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 w-7 p-0"
                      disabled={page === 1}
                      onClick={() => setPage(page - 1)}
                    >
                      <ChevronLeft className="h-3.5 w-3.5" />
                    </Button>
                    <span className="text-xs font-mono px-2">
                      Página {page}
                    </span>
                    <Button
                      variant="outline"
                      size="sm"
                      className="h-7 w-7 p-0"
                      onClick={() => setPage(page + 1)}
                    >
                      <ChevronRight className="h-3.5 w-3.5" />
                    </Button>
                  </div>
                </div>
              </>
            ) : (
              <CardContent className="flex-1 flex flex-col items-center justify-center">
                <img
                  src={EMPTY_IMG}
                  alt=""
                  className="w-32 h-auto opacity-60 mb-4"
                />
                <p className="text-sm font-medium text-muted-foreground">
                  Selecione uma tabela para visualizar
                </p>
                <p className="text-xs text-muted-foreground/60 mt-1">
                  Escolha um arquivo Parquet na lista à esquerda
                </p>
              </CardContent>
            )}
          </Card>
        </div>
      </div>
    </div>
  );
}
