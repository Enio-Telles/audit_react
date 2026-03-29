/*
 * Configurações — Swiss Design Fiscal
 * Preferências do sistema, conexão Oracle, caminhos, perfis
 */
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import {
  Settings,
  Database,
  FolderOpen,
  Shield,
  Server,
  Save,
  TestTube,
  CheckCircle2,
  XCircle,
} from "lucide-react";
import { toast } from "sonner";

export default function Configuracoes() {
  const handleSave = () => {
    toast.info("Funcionalidade em desenvolvimento", {
      description: "As configurações serão salvas quando o backend estiver conectado.",
    });
  };

  const handleTestConnection = () => {
    toast.info("Funcionalidade em desenvolvimento", {
      description: "O teste de conexão requer o backend Python configurado.",
    });
  };

  return (
    <div className="space-y-6 max-w-3xl">
      {/* Oracle Connection */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <Database className="h-4 w-4 text-primary" />
            Conexão Oracle
          </CardTitle>
          <CardDescription className="text-xs">
            Configurações de conexão com o banco de dados Oracle para extração de dados fiscais.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="host-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Host</Label>
              <Input id="host-input" placeholder="oracle.sefin.local" className="font-mono text-sm" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="porta-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Porta</Label>
              <Input id="porta-input" placeholder="1521" className="font-mono text-sm" />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="service-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Service Name</Label>
              <Input id="service-input" placeholder="SEFIN" className="font-mono text-sm" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="schema-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Schema</Label>
              <Input id="schema-input" placeholder="AUDIT" className="font-mono text-sm" />
            </div>
          </div>
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="user-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Usuário</Label>
              <Input id="user-input" placeholder="audit_user" className="font-mono text-sm" />
            </div>
            <div className="space-y-2">
              <Label htmlFor="senha-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Senha</Label>
              <Input id="senha-input" type="password" placeholder="********" className="font-mono text-sm" />
            </div>
          </div>
          <div className="flex items-center gap-3">
            <Button variant="outline" size="sm" className="gap-1.5" onClick={handleTestConnection}>
              <TestTube className="h-3.5 w-3.5" />
              Testar Conexão
            </Button>
            <Badge variant="outline" className="text-xs gap-1">
              <XCircle className="h-3 w-3 text-red-500" />
              Desconectado
            </Badge>
          </div>
        </CardContent>
      </Card>

      {/* Paths */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <FolderOpen className="h-4 w-4 text-primary" />
            Caminhos do Sistema
          </CardTitle>
          <CardDescription className="text-xs">
            Diretórios para armazenamento de dados, consultas SQL e exportações.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="space-y-2">
            <Label htmlFor="dir-cnpj-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Diretório de CNPJs
            </Label>
            <Input id="dir-cnpj-input" placeholder="/storage/CNPJ" className="font-mono text-sm" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="dir-sql-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Diretório de Consultas SQL
            </Label>
            <Input id="dir-sql-input" placeholder="/cruzamentos/consultas" className="font-mono text-sm" />
          </div>
          <div className="space-y-2">
            <Label htmlFor="dir-exports-input" className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
              Diretório de Exportações
            </Label>
            <Input id="dir-exports-input" placeholder="/storage/exports" className="font-mono text-sm" />
          </div>
        </CardContent>
      </Card>

      {/* Backend Status */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <Server className="h-4 w-4 text-primary" />
            Status do Backend
          </CardTitle>
          <CardDescription className="text-xs">
            Estado dos serviços do sistema.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-3">
          {[
            { nome: "API Python (FastAPI)", status: "offline", porta: "8000" },
            { nome: "Gateway Node (Express)", status: "offline", porta: "3001" },
            { nome: "Audit Engine", status: "offline", porta: "—" },
          ].map((service) => (
            <div key={service.nome} className="flex items-center justify-between py-2">
              <div>
                <p className="text-sm font-medium">{service.nome}</p>
                <p className="text-xs text-muted-foreground font-mono">Porta: {service.porta}</p>
              </div>
              <Badge variant="outline" className="text-xs gap-1">
                <XCircle className="h-3 w-3 text-red-500" />
                Offline
              </Badge>
            </div>
          ))}
        </CardContent>
      </Card>

      {/* Preferences */}
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold flex items-center gap-2">
            <Settings className="h-4 w-4 text-primary" />
            Preferências
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="flex items-center justify-between">
            <div>
              <Label htmlFor="switch-reprocessamento" className="text-sm font-medium">Reprocessamento automático</Label>
              <p id="desc-reprocessamento" className="text-xs text-muted-foreground">Recalcular tabelas derivadas automaticamente após edições</p>
            </div>
            <Switch id="switch-reprocessamento" aria-describedby="desc-reprocessamento" />
          </div>
          <Separator />
          <div className="flex items-center justify-between">
            <div>
              <Label htmlFor="switch-logs" className="text-sm font-medium">Logs detalhados</Label>
              <p id="desc-logs" className="text-xs text-muted-foreground">Registrar todas as operações no log fiscal</p>
            </div>
            <Switch id="switch-logs" aria-describedby="desc-logs" defaultChecked />
          </div>
          <Separator />
          <div className="flex items-center justify-between">
            <div>
              <Label htmlFor="switch-exportacao" className="text-sm font-medium">Exportação com formatação</Label>
              <p id="desc-exportacao" className="text-xs text-muted-foreground">Aplicar formatação de colunas ao exportar Excel</p>
            </div>
            <Switch id="switch-exportacao" aria-describedby="desc-exportacao" defaultChecked />
          </div>
        </CardContent>
      </Card>

      {/* Save */}
      <div className="flex justify-end">
        <Button className="gap-2" onClick={handleSave}>
          <Save className="h-4 w-4" />
          Salvar Configurações
        </Button>
      </div>
    </div>
  );
}
