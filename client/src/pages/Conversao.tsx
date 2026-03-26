/*
 * Conversão — Swiss Design Fiscal
 * Editar fatores de conversão, unid_ref, importar/exportar Excel
 * Baseado na aba Conversão do audit_pyside
 */
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import {
  ArrowLeftRight,
  Search,
  Download,
  Upload,
  RefreshCw,
  Save,
  ArrowUpDown,
  AlertTriangle,
  CheckCircle2,
  Edit3,
} from "lucide-react";
import { toast } from "sonner";

// Mock data
const FATORES_EXEMPLO = Array.from({ length: 15 }).map((_, i) => ({
  id: i + 1,
  id_agrupado: `G${String(i + 1).padStart(3, "0")}`,
  descricao_padrao: [
    "CERVEJA PILSEN 350ML",
    "REFRIGERANTE COLA 2L",
    "AGUA MINERAL 500ML",
    "LEITE INTEGRAL 1L",
    "ACUCAR CRISTAL 1KG",
    "ARROZ TIPO 1 5KG",
    "FEIJAO PRETO 1KG",
    "OLEO SOJA 900ML",
    "FARINHA TRIGO 1KG",
    "CAFE TORRADO 500G",
    "SAL REFINADO 1KG",
    "MACARRAO ESPAGUETE 500G",
    "BISCOITO CREAM CRACKER 200G",
    "MARGARINA 500G",
    "DETERGENTE LIQUIDO 500ML",
  ][i],
  unid_compra: ["LT", "UN", "UN", "LT", "KG", "KG", "KG", "LT", "KG", "KG", "KG", "KG", "UN", "UN", "UN"][i],
  unid_venda: ["UN", "UN", "UN", "UN", "KG", "KG", "KG", "UN", "KG", "UN", "KG", "UN", "UN", "UN", "UN"][i],
  unid_ref: ["UN", "UN", "UN", "UN", "KG", "KG", "KG", "UN", "KG", "UN", "KG", "UN", "UN", "UN", "UN"][i],
  fator: [1.0, 1.0, 1.0, 1.0, 1.0, 5.0, 1.0, 1.0, 1.0, 0.5, 1.0, 0.5, 1.0, 1.0, 1.0][i],
  status: i % 4 === 0 ? "pendente" : "ok",
}));

export default function Conversao() {
  const [searchTerm, setSearchTerm] = useState("");
  const [filterStatus, setFilterStatus] = useState("todos");
  const [editingId, setEditingId] = useState<number | null>(null);
  const [editFator, setEditFator] = useState("");
  const [editUnidRef, setEditUnidRef] = useState("");

  const filteredFatores = FATORES_EXEMPLO.filter((f) => {
    const matchSearch = f.descricao_padrao.toLowerCase().includes(searchTerm.toLowerCase()) ||
      f.id_agrupado.toLowerCase().includes(searchTerm.toLowerCase());
    const matchStatus = filterStatus === "todos" || f.status === filterStatus;
    return matchSearch && matchStatus;
  });

  const handleEdit = (id: number) => {
    const fator = FATORES_EXEMPLO.find((f) => f.id === id);
    if (fator) {
      setEditingId(id);
      setEditFator(String(fator.fator));
      setEditUnidRef(fator.unid_ref);
    }
  };

  const handleSave = () => {
    toast.info("Funcionalidade em desenvolvimento", {
      description: "A edição será salva quando o backend estiver conectado.",
    });
    setEditingId(null);
  };

  const handleImport = () => {
    toast.info("Funcionalidade em desenvolvimento", {
      description: "A importação de Excel será habilitada com o backend.",
    });
  };

  const handleExport = () => {
    toast.info("Funcionalidade em desenvolvimento", {
      description: "A exportação será habilitada com o backend.",
    });
  };

  const pendentes = FATORES_EXEMPLO.filter((f) => f.status === "pendente").length;

  return (
    <div className="space-y-4 max-w-6xl">
      {/* Status bar */}
      {pendentes > 0 && (
        <div className="flex items-center gap-3 px-4 py-3 rounded-lg bg-warning/10 border border-warning/20">
          <AlertTriangle className="h-4 w-4 text-warning shrink-0" />
          <p className="text-sm text-foreground">
            <span className="font-semibold">{pendentes} fatores</span> com recálculo pendente.
            Edite os valores e clique em "Recalcular" para atualizar as tabelas derivadas.
          </p>
          <Button variant="outline" size="sm" className="ml-auto gap-1.5 text-xs">
            <RefreshCw className="h-3.5 w-3.5" />
            Recalcular Derivados
          </Button>
        </div>
      )}

      {/* Toolbar */}
      <Card>
        <CardContent className="py-3 px-4">
          <div className="flex items-center gap-3">
            <div className="relative flex-1 max-w-md">
              <Search className="absolute left-2.5 top-2 h-3.5 w-3.5 text-muted-foreground" />
              <Input
                placeholder="Filtrar por descrição ou ID agrupado..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-8 h-8 text-xs"
              />
            </div>
            <Select value={filterStatus} onValueChange={setFilterStatus}>
              <SelectTrigger className="w-36 h-8 text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="todos">Todos</SelectItem>
                <SelectItem value="ok">Confirmados</SelectItem>
                <SelectItem value="pendente">Pendentes</SelectItem>
              </SelectContent>
            </Select>
            <div className="flex items-center gap-1.5 ml-auto">
              <Button variant="outline" size="sm" className="gap-1.5 text-xs" onClick={handleImport}>
                <Upload className="h-3.5 w-3.5" />
                Importar Excel
              </Button>
              <Button variant="outline" size="sm" className="gap-1.5 text-xs" onClick={handleExport}>
                <Download className="h-3.5 w-3.5" />
                Exportar Excel
              </Button>
            </div>
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
                  {["ID Agrupado", "Descrição Padrão", "Unid Compra", "Unid Venda", "Unid Ref", "Fator", "Status", "Ações"].map((col) => (
                    <th key={col} className="px-3 py-2.5 text-left font-semibold text-muted-foreground uppercase tracking-wider whitespace-nowrap border-b border-border">
                      <button className="flex items-center gap-1 hover:text-foreground transition-colors">
                        {col}
                        {col !== "Ações" && <ArrowUpDown className="h-3 w-3" />}
                      </button>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filteredFatores.map((fator) => (
                  <tr
                    key={fator.id}
                    className={`border-b border-border/50 transition-colors ${
                      fator.status === "pendente" ? "bg-warning/5" : "hover:bg-accent/30"
                    }`}
                  >
                    <td className="px-3 py-2 font-mono font-medium">{fator.id_agrupado}</td>
                    <td className="px-3 py-2 max-w-xs truncate">{fator.descricao_padrao}</td>
                    <td className="px-3 py-2 font-mono">{fator.unid_compra}</td>
                    <td className="px-3 py-2 font-mono">{fator.unid_venda}</td>
                    <td className="px-3 py-2">
                      {editingId === fator.id ? (
                        <Input
                          value={editUnidRef}
                          onChange={(e) => setEditUnidRef(e.target.value)}
                          className="h-7 w-16 text-xs font-mono"
                        />
                      ) : (
                        <span className="font-mono">{fator.unid_ref}</span>
                      )}
                    </td>
                    <td className="px-3 py-2">
                      {editingId === fator.id ? (
                        <Input
                          value={editFator}
                          onChange={(e) => setEditFator(e.target.value)}
                          className="h-7 w-20 text-xs font-mono text-right"
                          type="number"
                          step="0.001"
                        />
                      ) : (
                        <span className="font-mono">{fator.fator.toFixed(3)}</span>
                      )}
                    </td>
                    <td className="px-3 py-2">
                      {fator.status === "ok" ? (
                        <Badge variant="outline" className="text-[10px] gap-1 text-green-700 border-green-200 bg-green-50">
                          <CheckCircle2 className="h-2.5 w-2.5" />
                          OK
                        </Badge>
                      ) : (
                        <Badge variant="outline" className="text-[10px] gap-1 text-amber-700 border-amber-200 bg-amber-50">
                          <AlertTriangle className="h-2.5 w-2.5" />
                          Pendente
                        </Badge>
                      )}
                    </td>
                    <td className="px-3 py-2">
                      {editingId === fator.id ? (
                        <div className="flex items-center gap-1">
                          <Button variant="default" size="sm" className="h-6 text-[10px] gap-1" onClick={handleSave}>
                            <Save className="h-3 w-3" />
                            Salvar
                          </Button>
                          <Button variant="ghost" size="sm" className="h-6 text-[10px]" onClick={() => setEditingId(null)}>
                            Cancelar
                          </Button>
                        </div>
                      ) : (
                        <Button variant="ghost" size="sm" className="h-6 text-[10px] gap-1" onClick={() => handleEdit(fator.id)}>
                          <Edit3 className="h-3 w-3" />
                          Editar
                        </Button>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
