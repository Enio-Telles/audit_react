/*
 * Extração — Swiss Design Fiscal
 * Selecionar CNPJ, consultas SQL, data limite, executar pipeline
 * Baseado em AuditarCNPJ.tsx do sefin_audit_5
 */
import { useState } from "react";
import { usePipeline } from "@/hooks/useAuditApi";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Checkbox } from "@/components/ui/checkbox";
import { RefreshCw } from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Play,
  History,
  Database,
  FileText,
  CheckCircle2,
  Circle,
  Clock,
  AlertCircle,
  FolderOpen,
} from "lucide-react";
import { toast } from "sonner";

const CONSULTAS_DISPONIVEIS = [
  {
    id: "nfe",
    nome: "NFe - Notas Fiscais Eletrônicas",
    categoria: "Documentos",
  },
  {
    id: "nfce",
    nome: "NFCe - Notas Fiscais Consumidor",
    categoria: "Documentos",
  },
  {
    id: "c100",
    nome: "C100 - Registro de Documentos Fiscais",
    categoria: "EFD",
  },
  { id: "c170", nome: "C170 - Itens dos Documentos Fiscais", categoria: "EFD" },
  { id: "bloco_h", nome: "Bloco H - Inventário Físico", categoria: "EFD" },
  { id: "reg0000", nome: "Reg 0000 - Abertura do Arquivo", categoria: "EFD" },
  {
    id: "reg0200",
    nome: "Reg 0200 - Tabela de Identificação do Item",
    categoria: "EFD",
  },
  {
    id: "reg0220",
    nome: "Reg 0220 - Fatores de Conversão de Unidades",
    categoria: "EFD",
  },
];

const ETAPAS_PIPELINE = [
  { id: "conexao", label: "Conexão Oracle" },
  { id: "extracao", label: "Extração de Dados" },
  { id: "parquet", label: "Geração de Parquets" },
  { id: "produtos", label: "Tabelas de Produtos" },
  { id: "analises", label: "Análises e Relatórios" },
];

function formatCnpj(value: string) {
  const digits = value.replace(/\D/g, "").slice(0, 14);
  if (digits.length <= 2) return digits;
  if (digits.length <= 5) return `${digits.slice(0, 2)}.${digits.slice(2)}`;
  if (digits.length <= 8)
    return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5)}`;
  if (digits.length <= 12)
    return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8)}`;
  return `${digits.slice(0, 2)}.${digits.slice(2, 5)}.${digits.slice(5, 8)}/${digits.slice(8, 12)}-${digits.slice(12)}`;
}

export default function Extracao() {
  const [cnpj, setCnpj] = useState("");
  const [dataLimite, setDataLimite] = useState("");
  const [consultasSelecionadas, setConsultasSelecionadas] = useState<string[]>(
    CONSULTAS_DISPONIVEIS.map(c => c.id)
  );
  const [activeTab, setActiveTab] = useState("nova");

  const toggleConsulta = (id: string) => {
    setConsultasSelecionadas(prev =>
      prev.includes(id) ? prev.filter(c => c !== id) : [...prev, id]
    );
  };

  const { executar, loading, data } = usePipeline();

  const handleExecutar = async () => {
    const cnpjLimpo = cnpj.replace(/\D/g, "");
    if (cnpjLimpo.length !== 14) {
      toast.error("Informe um CNPJ válido com 14 dígitos");
      return;
    }
    if (consultasSelecionadas.length === 0) {
      toast.error("Selecione ao menos uma consulta SQL");
      return;
    }

    try {
      await executar(cnpjLimpo, consultasSelecionadas, dataLimite || undefined);
      toast.success("Extração e Pipeline finalizados com sucesso.");
    } catch (err: any) {
      toast.error("Erro na extração", { description: err.message });
    }
  };

  const categorias = Array.from(
    new Set(CONSULTAS_DISPONIVEIS.map(c => c.categoria))
  );

  return (
    <div className="space-y-6 max-w-5xl">
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList>
          <TabsTrigger value="nova" className="gap-2">
            <Play className="h-3.5 w-3.5" />
            Nova Extração
          </TabsTrigger>
          <TabsTrigger value="historico" className="gap-2">
            <History className="h-3.5 w-3.5" />
            Histórico
          </TabsTrigger>
        </TabsList>

        <TabsContent value="nova" className="space-y-4 mt-4">
          {/* CNPJ + Data Limite */}
          <Card>
            <CardHeader className="pb-4">
              <CardTitle className="text-sm font-semibold flex items-center gap-2">
                <Database className="h-4 w-4 text-primary" />
                Dados da Extração
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label
                    htmlFor="cnpj-input"
                    className="text-xs font-semibold uppercase tracking-wider text-muted-foreground"
                  >
                    CNPJ do Contribuinte
                  </Label>
                  <Input
                    id="cnpj-input"
                    placeholder="00.000.000/0000-00"
                    value={cnpj}
                    onChange={e => setCnpj(formatCnpj(e.target.value))}
                    className="font-mono text-base h-11"
                  />
                </div>
                <div className="space-y-2">
                  <Label
                    htmlFor="data-limite-input"
                    className="text-xs font-semibold uppercase tracking-wider text-muted-foreground"
                  >
                    Data Limite EFD (opcional)
                  </Label>
                  <Input
                    id="data-limite-input"
                    placeholder="DD/MM/AAAA"
                    value={dataLimite}
                    onChange={e => setDataLimite(e.target.value)}
                    className="font-mono text-base h-11"
                  />
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Consultas SQL */}
          <Card>
            <CardHeader className="pb-4">
              <div className="flex items-center justify-between">
                <CardTitle className="text-sm font-semibold flex items-center gap-2">
                  <FileText className="h-4 w-4 text-primary" />
                  Consultas SQL
                </CardTitle>
                <div className="flex gap-2">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-xs h-7"
                    onClick={() =>
                      setConsultasSelecionadas(
                        CONSULTAS_DISPONIVEIS.map(c => c.id)
                      )
                    }
                  >
                    Selecionar Todas
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-xs h-7"
                    onClick={() => setConsultasSelecionadas([])}
                  >
                    Limpar
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <ScrollArea className="h-64">
                <div className="space-y-4">
                  {categorias.map(cat => (
                    <div key={cat}>
                      <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground mb-2">
                        {cat}
                      </p>
                      <div className="space-y-1">
                        {CONSULTAS_DISPONIVEIS.filter(
                          c => c.categoria === cat
                        ).map(consulta => (
                          <label
                            key={consulta.id}
                            htmlFor={`checkbox-${consulta.id}`}
                            className="flex items-center gap-3 px-3 py-2 rounded-md hover:bg-accent/50 transition-colors cursor-pointer"
                          >
                            <Checkbox
                              id={`checkbox-${consulta.id}`}
                              checked={consultasSelecionadas.includes(
                                consulta.id
                              )}
                              onCheckedChange={() =>
                                toggleConsulta(consulta.id)
                              }
                            />
                            <span className="text-sm">{consulta.nome}</span>
                            <Badge
                              variant="outline"
                              className="ml-auto text-[10px] font-mono"
                            >
                              {consulta.id}
                            </Badge>
                          </label>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </ScrollArea>
            </CardContent>
          </Card>

          {/* Pipeline Steps Preview */}
          <Card>
            <CardHeader className="pb-4">
              <CardTitle className="text-sm font-semibold flex items-center gap-2">
                <Clock className="h-4 w-4 text-primary" />
                Etapas do Pipeline{" "}
                {loading && <RefreshCw className="h-4 w-4 animate-spin ml-2" />}
              </CardTitle>
            </CardHeader>
            <CardContent>
              {data ? (
                <div className="space-y-4">
                  <div className="flex justify-between items-center bg-muted/50 p-2 rounded">
                    <span className="font-semibold text-sm">
                      Status: {data.status}
                    </span>
                    <span className="text-xs text-muted-foreground">
                      Duração: {data.duracao_ms}ms
                    </span>
                  </div>
                  {data.etapas && data.etapas.length > 0 ? (
                    <div className="space-y-2">
                      {data.etapas.map((etapa, idx) => (
                        <div
                          key={idx}
                          className="flex justify-between items-center border-b pb-2 text-sm"
                        >
                          <div className="flex items-center gap-2">
                            <span
                              className={`w-2 h-2 rounded-full ${etapa.status === "concluida" ? "bg-green-500" : etapa.status === "erro" ? "bg-red-500" : "bg-yellow-500"}`}
                            ></span>
                            <span className="font-medium">{etapa.tabela}</span>
                          </div>
                          <div className="flex items-center gap-4 text-xs text-muted-foreground">
                            {etapa.mensagem && (
                              <span className="text-red-500 max-w-xs truncate">
                                {etapa.mensagem}
                              </span>
                            )}
                            <span>{etapa.duracao_ms}ms</span>
                            <span>{etapa.registros} reg.</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground">
                      Nenhuma etapa executada.
                    </p>
                  )}
                  {data.erros && data.erros.length > 0 && (
                    <div className="bg-red-50 text-red-600 p-2 rounded text-xs mt-2">
                      <strong>Erros:</strong>
                      <ul className="list-disc pl-4 mt-1">
                        {data.erros.map((erro, i) => (
                          <li key={i}>{erro}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                </div>
              ) : (
                <div className="flex items-center gap-2 flex-wrap">
                  {ETAPAS_PIPELINE.map((etapa, idx) => (
                    <div key={etapa.id} className="flex items-center gap-2">
                      <div className="flex items-center gap-2 px-3 py-1.5 rounded-md bg-muted">
                        <Circle className="h-3 w-3 text-muted-foreground/40" />
                        <span className="text-xs font-medium text-muted-foreground">
                          {etapa.label}
                        </span>
                      </div>
                      {idx < ETAPAS_PIPELINE.length - 1 && (
                        <div className="w-4 h-px bg-border" />
                      )}
                    </div>
                  ))}
                </div>
              )}
            </CardContent>
          </Card>

          {/* Execute Button */}
          <div className="flex justify-end">
            <Button
              size="lg"
              className="gap-2 px-8"
              onClick={handleExecutar}
              disabled={cnpj.replace(/\D/g, "").length !== 14 || loading}
            >
              {loading ? (
                <RefreshCw className="h-4 w-4 animate-spin" />
              ) : (
                <Play className="h-4 w-4" />
              )}
              {loading ? "Executando..." : "Executar Pipeline Completo"}
            </Button>
          </div>
        </TabsContent>

        <TabsContent value="historico" className="mt-4">
          <Card>
            <CardContent className="pt-6">
              <div className="flex flex-col items-center justify-center py-12 text-center">
                <FolderOpen className="h-10 w-10 text-muted-foreground/30 mb-3" />
                <p className="text-sm font-medium text-muted-foreground">
                  Nenhuma extração registrada
                </p>
                <p className="text-xs text-muted-foreground/60 mt-1">
                  Execute uma extração para ver o histórico aqui
                </p>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
