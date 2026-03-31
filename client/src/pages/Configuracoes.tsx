import { useEffect, useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Switch } from "@/components/ui/switch";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Database, RefreshCw, Save, Search, Server, TestTube2 } from "lucide-react";
import { toast } from "sonner";
import { useSistema } from "@/hooks/useAuditApi";
import { classificarStatusOracle } from "@/lib/oracle";
import type {
  ConfiguracaoSistema,
  OracleColuna,
  OracleMapeamentoFonte,
  OracleObjeto,
  OracleValidacaoMapeamento,
  SistemaStatus,
} from "@/types/audit";

interface ConexaoOracleResumo {
  status: string;
  usuario: string;
  banco: string;
  host: string;
}

export default function Configuracoes() {
  const {
    loading,
    error,
    carregarStatus,
    carregarConfiguracoes,
    salvarConfiguracoes,
    testarConexaoOracle,
    listarMapeamentosOracle,
    salvarMapeamentosOracle,
    validarMapeamentosOracle,
    buscarFontesOracle,
    listarColunasOracle,
  } = useSistema();

  const [statusSistema, setStatusSistema] = useState<SistemaStatus | null>(null);
  const [mapeamentos, setMapeamentos] = useState<OracleMapeamentoFonte[]>([]);
  const [validacoes, setValidacoes] = useState<Record<string, OracleValidacaoMapeamento>>({});
  const [configuracoes, setConfiguracoes] = useState<ConfiguracaoSistema>({
    reprocessamento_automatico: true,
    logs_detalhados: true,
    exportacao_formatada: true,
    diretorio_consultas_sql: "",
    oracle_indice_ativo: 0,
  });
  const [mapeamentosEditados, setMapeamentosEditados] = useState<Record<string, string>>({});
  const [aliasSelecionado, setAliasSelecionado] = useState<string>("");
  const [termoBusca, setTermoBusca] = useState("C170");
  const [objetosEncontrados, setObjetosEncontrados] = useState<OracleObjeto[]>([]);
  const [colunasObjeto, setColunasObjeto] = useState<OracleColuna[]>([]);
  const [objetoInspecionado, setObjetoInspecionado] = useState<string>("");
  const [conexaoOracle, setConexaoOracle] = useState<ConexaoOracleResumo | null>(null);
  const indiceOracleAtivo = configuracoes.oracle_indice_ativo ?? statusSistema?.oracle_indice_ativo ?? 0;

  const carregarTudo = async () => {
    try {
      const [status, cfg, mapa] = await Promise.all([
        carregarStatus(),
        carregarConfiguracoes(),
        listarMapeamentosOracle(),
      ]);

      setStatusSistema(status);
      setConfiguracoes(cfg.configuracoes);
      setMapeamentos(mapa.mapeamentos);
      setMapeamentosEditados(
        Object.fromEntries(mapa.mapeamentos.map((item) => [item.chave, item.fonte_configurada])),
      );
      if (!aliasSelecionado && mapa.mapeamentos.length > 0) {
        setAliasSelecionado(mapa.mapeamentos[0].chave);
      }
    } catch (erroLocal) {
      toast.error("Falha ao carregar configuracoes", {
        description: (erroLocal as Error).message,
      });
    }
  };

  useEffect(() => {
    carregarTudo();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    setConexaoOracle(null);
  }, [indiceOracleAtivo]);

  const salvarPreferencias = async () => {
    try {
      await salvarConfiguracoes(configuracoes);
      toast.success("Configuracoes salvas com sucesso");
      await carregarTudo();
    } catch (erroLocal) {
      toast.error("Falha ao salvar configuracoes", {
        description: (erroLocal as Error).message,
      });
    }
  };

  const salvarMapeamentos = async () => {
    try {
      const resposta = await salvarMapeamentosOracle(mapeamentosEditados);
      setMapeamentos(resposta.mapeamentos);
      setStatusSistema((atual) =>
        atual
          ? {
              ...atual,
              fontes_oracle_detalhadas: resposta.mapeamentos,
              fontes_oracle: Object.fromEntries(
                resposta.mapeamentos.map((item) => [item.chave, item.fonte_configurada]),
              ),
            }
          : atual,
      );
      toast.success("Mapeamentos Oracle salvos");
    } catch (erroLocal) {
      toast.error("Falha ao salvar mapeamentos", {
        description: (erroLocal as Error).message,
      });
    }
  };

  const executarTesteConexao = async () => {
    try {
      const resposta = await testarConexaoOracle(indiceOracleAtivo);
      setConexaoOracle(resposta.conexao);
      toast.success("Conexao Oracle validada");
    } catch (erroLocal) {
      toast.error("Falha ao testar conexao Oracle", {
        description: (erroLocal as Error).message,
      });
    }
  };

  const executarValidacaoMapeamentos = async () => {
    try {
      const resposta = await validarMapeamentosOracle(indiceOracleAtivo);
      setValidacoes(Object.fromEntries(resposta.validacoes.map((item) => [item.chave, item])));
      toast.success("Validacao concluida", {
        description: `${resposta.total_ok} mapeamento(s) ok, ${resposta.total_erro} com atencao`,
      });
    } catch (erroLocal) {
      toast.error("Falha ao validar mapeamentos", {
        description: (erroLocal as Error).message,
      });
    }
  };

  const executarBuscaObjetos = async () => {
    try {
      const resposta = await buscarFontesOracle(termoBusca, indiceOracleAtivo);
      setObjetosEncontrados(resposta.objetos);
      toast.success("Busca Oracle concluida", {
        description: `${resposta.objetos.length} objeto(s) encontrado(s)`,
      });
    } catch (erroLocal) {
      toast.error("Falha ao buscar objetos Oracle", {
        description: (erroLocal as Error).message,
      });
    }
  };

  const inspecionarObjeto = async (objeto: OracleObjeto) => {
    try {
      const resposta = await listarColunasOracle(objeto.object_name, objeto.owner, indiceOracleAtivo);
      setColunasObjeto(resposta.colunas);
      setObjetoInspecionado(`${objeto.owner}.${objeto.object_name}`);
    } catch (erroLocal) {
      toast.error("Falha ao listar colunas", {
        description: (erroLocal as Error).message,
      });
    }
  };

  const statusOracleBase = classificarStatusOracle(statusSistema);
  const statusOracle = statusSistema?.oracle_conectada
    ? { texto: "conectada", variante: statusOracleBase.variante }
    : statusSistema?.oracle_configurada
      ? { texto: "configurada, porem desconectada", variante: statusOracleBase.variante }
      : { texto: "pendente", variante: statusOracleBase.variante };

  const aplicarObjetoNoAlias = (objeto: OracleObjeto) => {
    if (!aliasSelecionado) {
      toast.error("Selecione um alias Oracle antes de aplicar um objeto");
      return;
    }

    const nomeCompleto = `${objeto.owner}.${objeto.object_name}`;
    setMapeamentosEditados((atual) => ({ ...atual, [aliasSelecionado]: nomeCompleto }));
    toast.success("Fonte aplicada ao alias selecionado", {
      description: `${aliasSelecionado} -> ${nomeCompleto}`,
    });
  };

  return (
    <div className="mx-auto max-w-7xl space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-sm font-semibold">
            <Server className="h-4 w-4 text-primary" />
            Status operacional
          </CardTitle>
          <CardDescription className="text-xs">
            API, conexao Oracle, consultas SQL e estado efetivo do ambiente.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4 text-sm">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-4">
            <div className="rounded border p-3">
              <p className="text-xs uppercase tracking-wider text-muted-foreground">API</p>
              <p className="mt-1 font-semibold">{statusSistema?.api ?? "desconhecido"}</p>
            </div>
            <div className="rounded border p-3">
              <p className="text-xs uppercase tracking-wider text-muted-foreground">Oracle</p>
              <div className="mt-1 flex items-center gap-2">
                <p className="font-semibold">{statusOracle.texto}</p>
                <Badge variant={statusOracle.variante}>#{statusSistema?.oracle_indice_ativo ?? indiceOracleAtivo}</Badge>
              </div>
            </div>
            <div className="rounded border p-3">
              <p className="text-xs uppercase tracking-wider text-muted-foreground">Consultas</p>
              <p className="mt-1 font-semibold">{statusSistema?.consultas_disponiveis.length ?? 0}</p>
            </div>
            <div className="rounded border p-3">
              <p className="text-xs uppercase tracking-wider text-muted-foreground">Diretorio base</p>
              <p className="mt-1 truncate font-mono text-xs">{statusSistema?.diretorio_base_cnpj ?? "-"}</p>
            </div>
          </div>

          {statusSistema?.erro_oracle ? <p className="text-xs text-destructive">{statusSistema.erro_oracle}</p> : null}

          <div className="flex flex-wrap items-center justify-between gap-3 rounded border p-3">
            <div className="space-y-1">
              <p className="text-sm font-medium">Diagnostico de conexao Oracle</p>
              {conexaoOracle ? (
                <p className="font-mono text-xs text-muted-foreground">
                  {conexaoOracle.usuario} @ {conexaoOracle.banco} ({conexaoOracle.host})
                </p>
              ) : (
                <p className="text-xs text-muted-foreground">
                  Valide a conexao Oracle ativa antes de ajustar aliases ou executar extracao.
                </p>
              )}
            </div>
            <Button variant="outline" onClick={executarTesteConexao} disabled={loading} className="gap-2">
              <TestTube2 className="h-4 w-4" />
              Testar conexao
            </Button>
          </div>

          <div className="rounded border p-3">
            <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
              <div className="space-y-2">
                <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Conexao Oracle ativa
                </Label>
                <Select
                  value={String(indiceOracleAtivo)}
                  onValueChange={(valor) =>
                    setConfiguracoes((atual) => ({ ...atual, oracle_indice_ativo: Number(valor) }))
                  }
                >
                  <SelectTrigger className="w-full min-w-64 font-mono">
                    <SelectValue placeholder="Selecione a conexao Oracle" />
                  </SelectTrigger>
                  <SelectContent>
                    {(statusSistema?.conexoes_oracle ?? [{ indice: 0, host: "", porta: null, servico: "", configurada: false, ativa: true, erro: null }]).map(
                      (conexao) => (
                        <SelectItem key={conexao.indice} value={String(conexao.indice)}>
                          {`#${conexao.indice} ${conexao.host || "sem host"}${conexao.servico ? ` / ${conexao.servico}` : ""}`}
                        </SelectItem>
                      ),
                    )}
                  </SelectContent>
                </Select>
              </div>

              <div className="max-w-md text-xs text-muted-foreground">
                <p>O indice selecionado sera usado por padrao nos testes, buscas, validacoes e extracoes Oracle.</p>
              </div>
            </div>

            <div className="mt-4 space-y-2">
              {(statusSistema?.conexoes_oracle ?? []).map((conexao) => (
                <div key={conexao.indice} className="flex flex-wrap items-center justify-between gap-3 rounded border px-3 py-2 text-xs">
                  <div className="space-y-1">
                    <p className="font-mono font-semibold">
                      #{conexao.indice} {conexao.host || "host nao informado"}
                    </p>
                    <p className="text-muted-foreground">
                      {conexao.porta ?? "-"} / {conexao.servico || "servico nao informado"}
                    </p>
                    {conexao.erro ? <p className="text-destructive">{conexao.erro}</p> : null}
                  </div>
                  <div className="flex items-center gap-2">
                    {conexao.ativa ? <Badge>ativa</Badge> : null}
                    <Badge variant={conexao.configurada ? "outline" : "secondary"}>
                      {conexao.configurada ? "configurada" : "incompleta"}
                    </Badge>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1.4fr_1fr]">
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center gap-2 text-sm font-semibold">
              <Database className="h-4 w-4 text-primary" />
              Mapeamento Oracle por alias
            </CardTitle>
            <CardDescription className="text-xs">
              Salva overrides persistidos usados pelos SQLs versionados na extracao.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <p className="text-xs text-muted-foreground">
                Alias selecionado para receber resultados de busca:{" "}
                <span className="font-mono text-foreground">{aliasSelecionado || "-"}</span>
              </p>
              <div className="flex flex-wrap gap-2">
                <Button variant="outline" onClick={executarValidacaoMapeamentos} disabled={loading} className="gap-2">
                  <RefreshCw className="h-4 w-4" />
                  Validar aliases
                </Button>
                <Button onClick={salvarMapeamentos} disabled={loading} className="gap-2">
                  <Save className="h-4 w-4" />
                  Salvar aliases
                </Button>
              </div>
            </div>

            <ScrollArea className="h-[520px] rounded border">
              <div className="min-w-[780px]">
                <div className="grid grid-cols-[150px_1fr_110px_110px_120px] gap-3 border-b bg-muted/40 px-3 py-2 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
                  <span>Alias</span>
                  <span>Fonte efetiva</span>
                  <span>Origem</span>
                  <span>Status</span>
                  <span>Acoes</span>
                </div>

                {mapeamentos.map((item) => {
                  const validacao = validacoes[item.chave];
                  const valorEditado = mapeamentosEditados[item.chave] ?? item.fonte_configurada;

                  return (
                    <div
                      key={item.chave}
                      className={`grid grid-cols-[150px_1fr_110px_110px_120px] gap-3 border-b px-3 py-3 ${
                        aliasSelecionado === item.chave ? "bg-accent/40" : ""
                      }`}
                    >
                      <div className="space-y-1">
                        <p className="font-mono text-xs font-semibold">{item.chave}</p>
                        <p className="text-[11px] text-muted-foreground">{item.env_var}</p>
                      </div>

                      <div className="space-y-2">
                        <Input
                          value={valorEditado}
                          onChange={(event) =>
                            setMapeamentosEditados((atual) => ({
                              ...atual,
                              [item.chave]: event.target.value.toUpperCase(),
                            }))
                          }
                          className="font-mono text-xs"
                        />
                        <p className="text-[11px] text-muted-foreground">Padrao: {item.fonte_padrao}</p>
                        {validacao?.erro ? <p className="text-[11px] text-destructive">{validacao.erro}</p> : null}
                        {validacao?.colunas_amostra?.length ? (
                          <p className="text-[11px] text-muted-foreground">
                            Colunas: {validacao.colunas_amostra.join(", ")}
                          </p>
                        ) : null}
                      </div>

                      <div className="pt-2">
                        <Badge variant="outline">{item.origem}</Badge>
                      </div>

                      <div className="pt-2">
                        <Badge variant={validacao ? (validacao.existe && !validacao.erro ? "default" : "destructive") : "secondary"}>
                          {validacao ? (validacao.existe && !validacao.erro ? "ok" : "erro") : "nao validado"}
                        </Badge>
                      </div>

                      <div className="flex items-start justify-end pt-1">
                        <Button
                          variant={aliasSelecionado === item.chave ? "default" : "outline"}
                          size="sm"
                          onClick={() => setAliasSelecionado(item.chave)}
                        >
                          Selecionar
                        </Button>
                      </div>
                    </div>
                  );
                })}
              </div>
            </ScrollArea>
          </CardContent>
        </Card>

        <div className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Busca de objetos Oracle</CardTitle>
              <CardDescription className="text-xs">
                Localize views e tabelas reais e aplique o resultado ao alias selecionado.
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="flex gap-2">
                <Input
                  value={termoBusca}
                  onChange={(event) => setTermoBusca(event.target.value)}
                  placeholder="Ex.: C170, REG0200, NFE"
                  className="font-mono"
                />
                <Button onClick={executarBuscaObjetos} disabled={loading || !termoBusca.trim()} className="gap-2">
                  <Search className="h-4 w-4" />
                  Buscar
                </Button>
              </div>

              <ScrollArea className="h-64 rounded border">
                <div className="space-y-2 p-3">
                  {objetosEncontrados.length === 0 ? (
                    <p className="text-xs text-muted-foreground">Nenhum resultado carregado ainda.</p>
                  ) : (
                    objetosEncontrados.map((objeto) => (
                      <div key={`${objeto.owner}.${objeto.object_name}`} className="rounded border p-3">
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <p className="font-mono text-xs font-semibold">
                              {objeto.owner}.{objeto.object_name}
                            </p>
                            <p className="text-[11px] text-muted-foreground">{objeto.object_type}</p>
                          </div>
                          <div className="flex gap-2">
                            <Button variant="outline" size="sm" onClick={() => inspecionarObjeto(objeto)}>
                              Colunas
                            </Button>
                            <Button size="sm" onClick={() => aplicarObjetoNoAlias(objeto)}>
                              Usar
                            </Button>
                          </div>
                        </div>
                      </div>
                    ))
                  )}
                </div>
              </ScrollArea>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Colunas do objeto selecionado</CardTitle>
              <CardDescription className="font-mono text-xs">{objetoInspecionado || "Nenhum objeto inspecionado"}</CardDescription>
            </CardHeader>
            <CardContent>
              <ScrollArea className="h-64 rounded border">
                <div className="space-y-2 p-3">
                  {colunasObjeto.length === 0 ? (
                    <p className="text-xs text-muted-foreground">Use "Colunas" em um objeto Oracle para carregar o schema.</p>
                  ) : (
                    colunasObjeto.map((coluna) => (
                      <div key={`${coluna.object_name}-${coluna.column_name}`} className="flex items-center justify-between gap-3 rounded border px-3 py-2 text-xs">
                        <span className="font-mono">{coluna.column_name}</span>
                        <span className="text-muted-foreground">
                          {coluna.data_type}
                          {coluna.data_length ? ` (${coluna.data_length})` : ""}
                        </span>
                      </div>
                    ))
                  )}
                </div>
              </ScrollArea>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-sm font-semibold">Preferencias do sistema</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="space-y-2">
                <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                  Diretorio de consultas SQL
                </Label>
                <Input
                  value={configuracoes.diretorio_consultas_sql}
                  onChange={(event) =>
                    setConfiguracoes((atual) => ({ ...atual, diretorio_consultas_sql: event.target.value }))
                  }
                  className="font-mono text-sm"
                />
              </div>

              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium">Reprocessamento automatico</p>
                  <p className="text-xs text-muted-foreground">Reexecuta dependentes apos edicoes manuais.</p>
                </div>
                <Switch
                  checked={configuracoes.reprocessamento_automatico}
                  onCheckedChange={(valor) =>
                    setConfiguracoes((atual) => ({ ...atual, reprocessamento_automatico: valor }))
                  }
                />
              </div>

              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium">Logs detalhados</p>
                  <p className="text-xs text-muted-foreground">Registra etapas e contagens no backend.</p>
                </div>
                <Switch
                  checked={configuracoes.logs_detalhados}
                  onCheckedChange={(valor) => setConfiguracoes((atual) => ({ ...atual, logs_detalhados: valor }))}
                />
              </div>

              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium">Exportacao formatada</p>
                  <p className="text-xs text-muted-foreground">Aplica estilo em arquivos XLSX exportados.</p>
                </div>
                <Switch
                  checked={configuracoes.exportacao_formatada}
                  onCheckedChange={(valor) =>
                    setConfiguracoes((atual) => ({ ...atual, exportacao_formatada: valor }))
                  }
                />
              </div>

              <div className="flex items-center justify-end gap-3">
                {error ? <span className="text-xs text-destructive">{error}</span> : null}
                <Button onClick={salvarPreferencias} disabled={loading} className="gap-2">
                  <Save className="h-4 w-4" />
                  Salvar configuracoes
                </Button>
              </div>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}
