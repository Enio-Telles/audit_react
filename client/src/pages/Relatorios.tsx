import { useCallback, useEffect, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Loader2,
  Save,
  FileText,
  FilePlus,
  Download,
  Trash2,
  Upload,
  Plus,
  X,
  UserCog,
  FolderOpen,
  FileStack,
  CheckCircle2,
} from "lucide-react";
import { toast } from "sonner";
import { useRelatorio } from "@/hooks/useRelatorio";
import { useSelecaoOperacional } from "@/contexts/SelecaoOperacionalContext";
import type { DadosAuditor, DadosRelatorio, Dsf, CnpjComRelatorio, DetInfo, DiagnosticoRelatorios, ManifestacoesRelatorio } from "@/types/relatorio";

const OPCOES_MANIFESTACAO: Array<{ chave: keyof ManifestacoesRelatorio; rotulo: string }> = [
  { chave: "regularizou_integralmente", rotulo: "Regularizou integralmente as pendencias" },
  { chave: "apresentou_contestacao", rotulo: "Apresentou contestacao" },
  { chave: "solicitou_prorrogacao", rotulo: "Solicitou prorrogacao de prazo" },
  { chave: "nao_apresentou_manifestacao", rotulo: "Nao apresentou manifestacao" },
];

const MANIFESTACOES_PADRAO: ManifestacoesRelatorio = {
  regularizou_integralmente: false,
  apresentou_contestacao: false,
  solicitou_prorrogacao: false,
  nao_apresentou_manifestacao: true,
};

const AUDITOR_VAZIO: DadosAuditor = {
  nome: "",
  cargo: "Auditor Fiscal de Tributos Estaduais",
  matricula: "",
  orgao: "SEFIN/CRE/GEFIS - Gerencia de Fiscalizacao",
  endereco: "Avenida Farquar, n. 2986 - Palacio Rio Madeira - Bairro Pedrinhas - CEP 76.801-470 - Porto Velho/RO",
  local_data: "",
};

const RELATORIO_VAZIO: DadosRelatorio = {
  cnpj: "",
  contribuinte: "",
  ie: "",
  dsf: "",
  notificacao_det: "",
  manifestacao: "Nao apresentou manifestacao",
  manifestacoes: MANIFESTACOES_PADRAO,
  contatos_realizados: "",
  decisao_fiscal: "",
  desfecho: "",
  arquivos_notificacao_incluidos: [],
};

function formatarCnpj(cnpj: string): string {
  const limpo = cnpj.replace(/\D/g, "").slice(0, 14);
  if (limpo.length <= 2) return limpo;
  if (limpo.length <= 5) return `${limpo.slice(0, 2)}.${limpo.slice(2)}`;
  if (limpo.length <= 8) return `${limpo.slice(0, 2)}.${limpo.slice(2, 5)}.${limpo.slice(5)}`;
  if (limpo.length <= 12) return `${limpo.slice(0, 2)}.${limpo.slice(2, 5)}.${limpo.slice(5, 8)}/${limpo.slice(8)}`;
  return `${limpo.slice(0, 2)}.${limpo.slice(2, 5)}.${limpo.slice(5, 8)}/${limpo.slice(8, 12)}-${limpo.slice(12)}`;
}

function resumirManifestacoes(manifestacoes: ManifestacoesRelatorio): string {
  const marcadas = OPCOES_MANIFESTACAO
    .filter((opcao) => manifestacoes[opcao.chave])
    .map((opcao) => opcao.rotulo);

  return marcadas.length > 0 ? marcadas.join("; ") : "Nenhuma opcao marcada";
}

export default function Relatorios() {
  const api = useRelatorio();
  const { selecaoOperacional } = useSelecaoOperacional();

  // ---- Aba ativa ----
  const [abaAtiva, setAbaAtiva] = useState("auditor");

  // ---- Auditor ----
  const [auditor, setAuditor] = useState<DadosAuditor>(AUDITOR_VAZIO);
  const [auditorSalvo, setAuditorSalvo] = useState(false);

  // ---- DSF ----
  const [dsfs, setDsfs] = useState<Dsf[]>([]);
  const [dsfAtiva, setDsfAtiva] = useState<string>("");
  const [dsfNumero, setDsfNumero] = useState("");
  const [dsfDescricao, setDsfDescricao] = useState("");
  const [dsfCnpjs, setDsfCnpjs] = useState<string[]>([]);
  const [novoCnpjDsf, setNovoCnpjDsf] = useState("");

  // ---- Relatorio CNPJ ----
  const [cnpjSelecionado, setCnpjSelecionado] = useState("");
  const [relatorio, setRelatorio] = useState<DadosRelatorio>(RELATORIO_VAZIO);
  const [dets, setDets] = useState<DetInfo[]>([]);
  const [cnpjsComRelatorio, setCnpjsComRelatorio] = useState<CnpjComRelatorio[]>([]);
  const [diagnostico, setDiagnostico] = useState<DiagnosticoRelatorios | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const prontoPdf = diagnostico?.pronto_pdf ?? false;
  const dependenciasFaltantes = diagnostico?.dependencias_faltantes ?? [];
  const dependenciasAtivas = (diagnostico?.dependencias ?? [])
    .filter((item) => item.instalado)
    .map((item) => item.nome);
  const modeloIndividual = diagnostico?.modelos_docx?.individual;
  const modeloGeral = diagnostico?.modelos_docx?.geral;
  const prontoPdfIndividual = prontoPdf && Boolean(modeloIndividual?.pronto);
  const prontoPdfGeral = prontoPdf && Boolean(modeloGeral?.pronto);
  const motivoPdfIndisponivel = dependenciasFaltantes.length > 0
    ? `Dependencias ausentes no backend: ${dependenciasFaltantes.join(", ")}`
    : "Geracao de PDF temporariamente indisponivel.";
  const motivoModeloIndividual = modeloIndividual?.mensagem || "Modelo Word individual indisponivel.";
  const motivoModeloGeral = modeloGeral?.mensagem || "Modelo Word geral indisponivel.";

  // ---- Carregamento inicial ----
  useEffect(() => {
    api.carregarAuditor().then((a) => {
      if (a && a.nome) {
        setAuditor(a);
        setAuditorSalvo(true);
      }
    }).catch(() => {});

    api.listarDsfs().then(setDsfs).catch(() => {});
    api.listarCnpjsComRelatorio().then(setCnpjsComRelatorio).catch(() => {});
    api.carregarDiagnostico().then(setDiagnostico).catch(() => {});
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ---- Handlers Auditor ----
  const handleSalvarAuditor = async () => {
    try {
      const salvo = await api.salvarAuditor(auditor);
      setAuditor(salvo);
      setAuditorSalvo(true);
      toast.success("Dados do auditor salvos com sucesso");
    } catch {
      toast.error("Erro ao salvar dados do auditor");
    }
  };

  // ---- Handlers DSF ----
  const handleAdicionarCnpjDsf = () => {
    const limpo = novoCnpjDsf.replace(/\D/g, "");
    if (limpo.length !== 14) {
      toast.error("CNPJ deve ter 14 digitos");
      return;
    }
    if (dsfCnpjs.includes(limpo)) {
      toast.error("CNPJ ja adicionado a esta DSF");
      return;
    }
    setDsfCnpjs([...dsfCnpjs, limpo]);
    setNovoCnpjDsf("");
  };

  const handleRemoverCnpjDsf = (cnpj: string) => {
    setDsfCnpjs(dsfCnpjs.filter((c) => c !== cnpj));
  };

  const handleSalvarDsf = async () => {
    if (!dsfNumero.trim()) {
      toast.error("Informe o numero da DSF");
      return;
    }
    try {
      const dsf = await api.salvarDsf(dsfNumero, dsfDescricao, dsfCnpjs);
      toast.success(`DSF ${dsf.numero} salva com ${dsf.cnpjs.length} CNPJs`);
      const lista = await api.listarDsfs();
      setDsfs(lista);
      const diag = await api.carregarDiagnostico();
      setDiagnostico(diag);
      setDsfAtiva(dsfNumero);
    } catch {
      toast.error("Erro ao salvar DSF");
    }
  };

  const handleSelecionarDsf = (numero: string) => {
    setDsfAtiva(numero);
    const dsf = dsfs.find((d) => d.numero === numero);
    if (dsf) {
      setDsfNumero(dsf.numero);
      setDsfDescricao(dsf.descricao);
      setDsfCnpjs([...dsf.cnpjs]);
    }
  };

  const handleNovaDsf = () => {
    setDsfAtiva("");
    setDsfNumero("");
    setDsfDescricao("");
    setDsfCnpjs([]);
  };

  const handleExcluirDsf = async () => {
    if (!dsfAtiva) return;
    try {
      await api.excluirDsf(dsfAtiva);
      toast.success(`DSF ${dsfAtiva} excluida`);
      handleNovaDsf();
      const lista = await api.listarDsfs();
      setDsfs(lista);
      const diag = await api.carregarDiagnostico();
      setDiagnostico(diag);
    } catch {
      toast.error("Erro ao excluir DSF");
    }
  };

  // ---- Handlers Relatorio CNPJ ----
  const handleCarregarRelatorio = useCallback(async (cnpj: string) => {
    const limpo = cnpj.replace(/\D/g, "");
    if (limpo.length !== 14) return;
    setCnpjSelecionado(limpo);
    try {
      const dados = await api.carregarRelatorio(limpo);
      const detsList = await api.listarDets(limpo);
      const arquivosSelecionados = detsList.filter((item) => item.selecionado).map((item) => item.nome);
      if (dados && dados.contribuinte) {
        setRelatorio({
          ...RELATORIO_VAZIO,
          ...dados,
          cnpj: limpo,
          manifestacoes: dados.manifestacoes ?? MANIFESTACOES_PADRAO,
          arquivos_notificacao_incluidos: dados.arquivos_notificacao_incluidos?.length
            ? dados.arquivos_notificacao_incluidos
            : arquivosSelecionados,
        });
      } else {
        setRelatorio({
          ...RELATORIO_VAZIO,
          cnpj: limpo,
          arquivos_notificacao_incluidos: arquivosSelecionados,
        });
      }
      setDets(detsList);
    } catch {
      setRelatorio({ ...RELATORIO_VAZIO, cnpj: limpo });
      setDets([]);
    }
  }, [api]);

  useEffect(() => {
    if (!selecaoOperacional || selecaoOperacional.cnpjs_resolvidos.length === 0) {
      return;
    }

    if (selecaoOperacional.cnpjs_resolvidos.length === 1) {
      const cnpj = selecaoOperacional.cnpjs_resolvidos[0];
      if (cnpj && cnpj !== cnpjSelecionado) {
        handleCarregarRelatorio(cnpj).catch(() => undefined);
        setAbaAtiva("cnpj");
      }
      return;
    }

    setDsfCnpjs(selecaoOperacional.cnpjs_resolvidos);
    setAbaAtiva("gerar");
  }, [cnpjSelecionado, handleCarregarRelatorio, selecaoOperacional]);

  const handleSalvarRelatorio = async () => {
    if (!cnpjSelecionado) {
      toast.error("Selecione um CNPJ");
      return;
    }
    if (!relatorio.contribuinte.trim()) {
      toast.error("Informe o nome do contribuinte");
      return;
    }
    try {
      const relatorioSalvo = await api.salvarRelatorio(cnpjSelecionado, relatorio);
      setRelatorio({ ...RELATORIO_VAZIO, ...relatorioSalvo, cnpj: cnpjSelecionado });
      toast.success(`Relatorio do CNPJ ${formatarCnpj(cnpjSelecionado)} salvo`);
      setDets(await api.listarDets(cnpjSelecionado));
      const lista = await api.listarCnpjsComRelatorio();
      setCnpjsComRelatorio(lista);
      const dsfsAtualizadas = await api.listarDsfs();
      setDsfs(dsfsAtualizadas);
      const diag = await api.carregarDiagnostico();
      setDiagnostico(diag);
    } catch {
      toast.error("Erro ao salvar relatorio");
    }
  };

  const handleUploadDet = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file || !cnpjSelecionado) return;
    try {
      await api.uploadDet(cnpjSelecionado, file);
      toast.success(`DET '${file.name}' enviado com sucesso`);
      const detsList = await api.listarDets(cnpjSelecionado);
      setDets(detsList);
      setRelatorio((atual) => ({
        ...atual,
        arquivos_notificacao_incluidos: detsList.filter((item) => item.selecionado).map((item) => item.nome),
      }));
      const lista = await api.listarCnpjsComRelatorio();
      setCnpjsComRelatorio(lista);
      const diag = await api.carregarDiagnostico();
      setDiagnostico(diag);
    } catch {
      toast.error("Erro ao enviar DET");
    }
    if (fileInputRef.current) fileInputRef.current.value = "";
  };

  const handleGerarPdfIndividual = async () => {
    if (!cnpjSelecionado) {
      toast.error("Selecione um CNPJ");
      return;
    }
    try {
      await api.gerarPdfIndividual(cnpjSelecionado);
      toast.success("PDF individual gerado e baixado");
    } catch (erro) {
      toast.error(`Erro ao gerar PDF: ${(erro as Error).message}`);
    }
  };

  const handleGerarDocxIndividual = async () => {
    if (!cnpjSelecionado) {
      toast.error("Selecione um CNPJ");
      return;
    }
    try {
      await api.gerarDocxIndividual(cnpjSelecionado);
      toast.success("DOCX individual gerado e baixado");
    } catch (erro) {
      toast.error(`Erro ao gerar DOCX: ${(erro as Error).message}`);
    }
  };

  const handleAlternarManifestacao = (chave: keyof ManifestacoesRelatorio) => {
    setRelatorio((atual) => {
      const manifestacoes = {
        ...atual.manifestacoes,
        [chave]: !atual.manifestacoes[chave],
      };
      return {
        ...atual,
        manifestacoes,
        manifestacao: resumirManifestacoes(manifestacoes),
      };
    });
  };

  const handleAlternarDet = (nomeArquivo: string, marcado: boolean) => {
    setRelatorio((atual) => {
      const nomes = marcado
        ? Array.from(new Set([...atual.arquivos_notificacao_incluidos, nomeArquivo]))
        : atual.arquivos_notificacao_incluidos.filter((nome) => nome !== nomeArquivo);
      return { ...atual, arquivos_notificacao_incluidos: nomes };
    });
    setDets((atuais) =>
      atuais.map((item) =>
        item.nome === nomeArquivo ? { ...item, selecionado: marcado } : item,
      ),
    );
  };

  const handleGerarPdfGeral = async () => {
    if (!dsfAtiva && dsfCnpjs.length === 0) {
      toast.error("Selecione uma DSF ou adicione CNPJs");
      return;
    }
    try {
      await api.gerarPdfGeral(dsfAtiva || undefined, dsfCnpjs.length > 0 ? dsfCnpjs : undefined);
      toast.success("Relatorio geral consolidado gerado e baixado");
    } catch (erro) {
      toast.error(`Erro ao gerar relatorio geral: ${(erro as Error).message}`);
    }
  };

  return (
    <div className="mx-auto max-w-5xl space-y-6">
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-sm font-semibold">
            <FileText className="h-4 w-4" />
            Relatorios Fiscais Conclusivos
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            Geracao de relatorios de analise Fisconforme nao cumprido por CNPJ e consolidado por DSF
          </p>
        </CardHeader>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center gap-2 text-sm">
            <CheckCircle2 className="h-4 w-4" />
            Diagnostico operacional
          </CardTitle>
        </CardHeader>
        <CardContent className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
            <div className="rounded border p-3">
              <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">Prontidao PDF</p>
              <p className="mt-1 text-sm font-semibold">{prontoPdf ? "Pronta" : "Dependencias ausentes"}</p>
              <p className="mt-1 text-xs text-muted-foreground">
                {prontoPdf ? `Backend pronto com: ${dependenciasAtivas.join(", ")}` : motivoPdfIndisponivel}
              </p>
            </div>
          <div className="rounded border p-3">
            <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">Modelos Word</p>
            <p className="mt-1 text-sm font-semibold">
              {modeloIndividual?.pronto && modeloGeral?.pronto ? "Individual e geral ok" : "Verificar templates"}
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              Individual: {modeloIndividual?.existe ? "ok" : "ausente"} | Geral: {modeloGeral?.existe ? "ok" : "ausente"}
            </p>
          </div>
          <div className="rounded border p-3">
            <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">DSFs efetivas</p>
            <p className="mt-1 text-sm font-semibold">{diagnostico?.total_dsfs ?? dsfs.length}</p>
            <p className="mt-1 text-xs text-muted-foreground">
              Inclui DSFs persistidas e inferidas a partir dos relatorios dos CNPJs.
            </p>
          </div>
          <div className="rounded border p-3">
            <p className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">Pipeline local</p>
            <p className="mt-1 text-sm font-semibold">
              {diagnostico?.pipeline_local?.completo ? "Completo" : "Pendente"}
            </p>
            <p className="mt-1 text-xs text-muted-foreground">
              {diagnostico?.pipeline_local
                ? `${diagnostico.pipeline_local.total_tabelas_ok}/${diagnostico.pipeline_local.total_tabelas} tabelas ok para ${formatarCnpj(diagnostico.pipeline_local.cnpj_referencia)}`
                : "Sem diagnostico do pipeline local."}
            </p>
          </div>
        </CardContent>
      </Card>

      <Tabs value={abaAtiva} onValueChange={setAbaAtiva}>
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="auditor" className="gap-1.5 text-xs">
            <UserCog className="h-3.5 w-3.5" /> Auditor
          </TabsTrigger>
          <TabsTrigger value="dsf" className="gap-1.5 text-xs">
            <FolderOpen className="h-3.5 w-3.5" /> DSF
          </TabsTrigger>
          <TabsTrigger value="cnpj" className="gap-1.5 text-xs">
            <FilePlus className="h-3.5 w-3.5" /> CNPJ
          </TabsTrigger>
          <TabsTrigger value="gerar" className="gap-1.5 text-xs">
            <FileStack className="h-3.5 w-3.5" /> Gerar PDFs
          </TabsTrigger>
        </TabsList>

        {/* ============ ABA AUDITOR ============ */}
        <TabsContent value="auditor">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-sm">
                <UserCog className="h-4 w-4" />
                Dados do Auditor
                {auditorSalvo && <Badge variant="outline" className="ml-2 text-green-600"><CheckCircle2 className="mr-1 h-3 w-3" />Salvo</Badge>}
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Nome completo</Label>
                  <Input value={auditor.nome} onChange={(e) => setAuditor({ ...auditor, nome: e.target.value })} placeholder="Nome do auditor" />
                </div>
                <div className="space-y-2">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Matricula</Label>
                  <Input value={auditor.matricula} onChange={(e) => setAuditor({ ...auditor, matricula: e.target.value })} placeholder="Numero da matricula" />
                </div>
              </div>
              <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                <div className="space-y-2">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Cargo</Label>
                  <Input value={auditor.cargo} onChange={(e) => setAuditor({ ...auditor, cargo: e.target.value })} />
                </div>
                <div className="space-y-2">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Orgao</Label>
                  <Input value={auditor.orgao} onChange={(e) => setAuditor({ ...auditor, orgao: e.target.value })} />
                </div>
              </div>
              <div className="space-y-2">
                <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Local e data</Label>
                <Input value={auditor.local_data} onChange={(e) => setAuditor({ ...auditor, local_data: e.target.value })} placeholder="Porto Velho, 28 de marco de 2026" />
              </div>
              <div className="flex justify-end">
                <Button onClick={handleSalvarAuditor} disabled={api.loading} className="gap-2">
                  {api.loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                  Salvar auditor
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* ============ ABA DSF ============ */}
        <TabsContent value="dsf">
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
            {/* Lista de DSFs */}
            <Card className="lg:col-span-1">
              <CardHeader className="pb-2">
                <CardTitle className="flex items-center justify-between text-sm">
                  <span>DSFs cadastradas</span>
                  <Button variant="outline" size="sm" onClick={handleNovaDsf} className="gap-1 text-xs">
                    <Plus className="h-3 w-3" /> Nova
                  </Button>
                </CardTitle>
              </CardHeader>
              <CardContent>
                <ScrollArea className="h-64">
                  {dsfs.length === 0 ? (
                    <p className="py-4 text-center text-xs text-muted-foreground">Nenhuma DSF efetiva encontrada</p>
                  ) : (
                    <div className="space-y-1">
                      {dsfs.map((dsf) => (
                        <button
                          key={dsf.numero}
                          onClick={() => handleSelecionarDsf(dsf.numero)}
                          className={`w-full rounded px-3 py-2 text-left text-xs transition-colors hover:bg-accent/50 ${
                            dsfAtiva === dsf.numero ? "bg-accent font-semibold" : ""
                          }`}
                        >
                          <div className="font-mono font-semibold">{dsf.numero}</div>
                          <div className="text-muted-foreground">{dsf.descricao || "Sem descricao"}</div>
                          <div className="mt-1 flex gap-1">
                            <Badge variant="outline" className="text-[10px]">{dsf.cnpjs.length} CNPJs</Badge>
                            <Badge variant="outline" className="text-[10px] capitalize">{dsf.origem || "persistida"}</Badge>
                          </div>
                        </button>
                      ))}
                    </div>
                  )}
                </ScrollArea>
              </CardContent>
            </Card>

            {/* Formulario DSF */}
            <Card className="lg:col-span-2">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">
                  {dsfAtiva ? `Editar DSF ${dsfAtiva}` : "Nova DSF"}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
                  <div className="space-y-2">
                    <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Numero da DSF</Label>
                    <Input value={dsfNumero} onChange={(e) => setDsfNumero(e.target.value)} placeholder="20263710400226" className="font-mono" />
                  </div>
                  <div className="space-y-2">
                    <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Descricao</Label>
                    <Input value={dsfDescricao} onChange={(e) => setDsfDescricao(e.target.value)} placeholder="Acervo Fisconforme" />
                  </div>
                </div>

                <Separator />

                <div className="space-y-2">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                    CNPJs vinculados ({dsfCnpjs.length})
                  </Label>
                  <div className="flex gap-2">
                    <Input
                      value={novoCnpjDsf ? formatarCnpj(novoCnpjDsf) : ""}
                      onChange={(e) => setNovoCnpjDsf(e.target.value.replace(/\D/g, "").slice(0, 14))}
                      placeholder="00.000.000/0000-00"
                      className="font-mono"
                      onKeyDown={(e) => { if (e.key === "Enter") handleAdicionarCnpjDsf(); }}
                    />
                    <Button variant="outline" onClick={handleAdicionarCnpjDsf} className="gap-1 shrink-0">
                      <Plus className="h-3.5 w-3.5" /> Adicionar
                    </Button>
                  </div>

                  <ScrollArea className="h-40 rounded border p-2">
                    {dsfCnpjs.length === 0 ? (
                      <p className="py-4 text-center text-xs text-muted-foreground">Nenhum CNPJ vinculado</p>
                    ) : (
                      <div className="space-y-1">
                        {dsfCnpjs.map((cnpj) => {
                          const info = cnpjsComRelatorio.find((c) => c.cnpj === cnpj);
                          return (
                            <div key={cnpj} className="flex items-center justify-between rounded px-2 py-1.5 hover:bg-accent/30">
                              <div>
                                <span className="font-mono text-xs font-semibold">{formatarCnpj(cnpj)}</span>
                                {info ? (
                                  <span className="ml-2 text-xs text-muted-foreground">{info.contribuinte}</span>
                                ) : (
                                  <span className="ml-2 text-[10px] text-orange-500">Sem relatorio</span>
                                )}
                              </div>
                              <Button variant="ghost" size="sm" onClick={() => handleRemoverCnpjDsf(cnpj)} className="h-6 w-6 p-0">
                                <X className="h-3 w-3" />
                              </Button>
                            </div>
                          );
                        })}
                      </div>
                    )}
                  </ScrollArea>
                </div>

                <div className="flex items-center justify-between">
                  {dsfAtiva && (
                    <Button variant="destructive" size="sm" onClick={handleExcluirDsf} disabled={api.loading} className="gap-1">
                      <Trash2 className="h-3.5 w-3.5" /> Excluir DSF
                    </Button>
                  )}
                  <div className="ml-auto">
                    <Button onClick={handleSalvarDsf} disabled={api.loading} className="gap-2">
                      {api.loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                      Salvar DSF
                    </Button>
                  </div>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* ============ ABA CNPJ ============ */}
        <TabsContent value="cnpj">
          <div className="grid grid-cols-1 gap-4 lg:grid-cols-3">
            {/* Lista de CNPJs */}
            <Card className="lg:col-span-1">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">CNPJs com relatorio</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="mb-3 space-y-2">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Carregar CNPJ</Label>
                  <Input
                    value={cnpjSelecionado ? formatarCnpj(cnpjSelecionado) : ""}
                    onChange={(e) => {
                      const limpo = e.target.value.replace(/\D/g, "").slice(0, 14);
                      setCnpjSelecionado(limpo);
                      if (limpo.length === 14) handleCarregarRelatorio(limpo);
                    }}
                    placeholder="00.000.000/0000-00"
                    className="font-mono"
                  />
                </div>
                <Separator className="my-2" />
                <ScrollArea className="h-52">
                  {cnpjsComRelatorio.length === 0 ? (
                    <p className="py-4 text-center text-xs text-muted-foreground">Nenhum relatorio preenchido</p>
                  ) : (
                    <div className="space-y-1">
                      {cnpjsComRelatorio.map((item) => (
                        <button
                          key={item.cnpj}
                          onClick={() => handleCarregarRelatorio(item.cnpj)}
                          className={`w-full rounded px-3 py-2 text-left text-xs transition-colors hover:bg-accent/50 ${
                            cnpjSelecionado === item.cnpj ? "bg-accent font-semibold" : ""
                          }`}
                        >
                          <div className="font-mono font-semibold">{formatarCnpj(item.cnpj)}</div>
                          <div className="text-muted-foreground">{item.contribuinte}</div>
                          <div className="mt-1 flex gap-1">
                            {item.tem_det && <Badge variant="outline" className="text-[10px] text-green-600">DET</Badge>}
                            <Badge variant="outline" className="text-[10px]">{item.manifestacao || "Sem info"}</Badge>
                          </div>
                          {item.aviso_det ? (
                            <p className="mt-1 text-[10px] text-amber-600">{item.aviso_det}</p>
                          ) : null}
                        </button>
                      ))}
                    </div>
                  )}
                </ScrollArea>
              </CardContent>
            </Card>

            {/* Formulario Relatorio */}
            <Card className="lg:col-span-2">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">
                  {cnpjSelecionado ? `Relatorio — ${formatarCnpj(cnpjSelecionado)}` : "Selecione um CNPJ"}
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-3">
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                  <div className="space-y-1.5">
                    <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Contribuinte</Label>
                    <Input value={relatorio.contribuinte} onChange={(e) => setRelatorio({ ...relatorio, contribuinte: e.target.value })} placeholder="Razao social" />
                  </div>
                  <div className="space-y-1.5">
                    <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Inscricao Estadual</Label>
                    <Input value={relatorio.ie} onChange={(e) => setRelatorio({ ...relatorio, ie: e.target.value })} placeholder="IE" className="font-mono" />
                  </div>
                </div>
                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                  <div className="space-y-1.5">
                    <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">DSF</Label>
                    <Input value={relatorio.dsf} onChange={(e) => setRelatorio({ ...relatorio, dsf: e.target.value })} placeholder="Numero da DSF" className="font-mono" />
                  </div>
                  <div className="space-y-1.5">
                    <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Notificacao DET n.</Label>
                    <Input value={relatorio.notificacao_det} onChange={(e) => setRelatorio({ ...relatorio, notificacao_det: e.target.value })} placeholder="Numero da DET" className="font-mono" />
                  </div>
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Manifestacao do contribuinte</Label>
                  <div className="space-y-2 rounded border p-3">
                    <p className="text-xs text-muted-foreground">Marque com X as opcoes aplicaveis ao relatorio final.</p>
                    {OPCOES_MANIFESTACAO.map((opcao) => {
                      const ativo = relatorio.manifestacoes[opcao.chave];
                      return (
                        <button
                          key={opcao.chave}
                          type="button"
                          onClick={() => handleAlternarManifestacao(opcao.chave)}
                          className="flex w-full items-center gap-3 rounded px-2 py-1.5 text-left text-sm transition-colors hover:bg-accent/40"
                        >
                          <span className={`flex h-4 w-4 items-center justify-center border text-[10px] font-bold ${ativo ? "border-primary bg-primary text-primary-foreground" : "border-border bg-background text-transparent"}`}>
                            X
                          </span>
                          <span className={ativo ? "font-medium text-foreground" : "text-foreground"}>{opcao.rotulo}</span>
                        </button>
                      );
                    })}
                    <p className="text-[11px] text-muted-foreground">Resumo: {resumirManifestacoes(relatorio.manifestacoes)}</p>
                  </div>
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Contatos realizados</Label>
                  <Textarea value={relatorio.contatos_realizados} onChange={(e) => setRelatorio({ ...relatorio, contatos_realizados: e.target.value })} placeholder="Descreva os contatos realizados..." rows={3} />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Decisao fiscal</Label>
                  <Textarea value={relatorio.decisao_fiscal} onChange={(e) => setRelatorio({ ...relatorio, decisao_fiscal: e.target.value })} placeholder="Descreva a decisao fiscal..." rows={3} />
                </div>
                <div className="space-y-1.5">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Desfecho / Situacao final</Label>
                  <Textarea value={relatorio.desfecho} onChange={(e) => setRelatorio({ ...relatorio, desfecho: e.target.value })} placeholder="Descreva o desfecho..." rows={3} />
                </div>

                <Separator />

                {/* Upload DET */}
                <div className="space-y-2">
                  <Label className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">PDF DET</Label>
                  <div className="flex items-center gap-2">
                    <input ref={fileInputRef} type="file" accept=".pdf" onChange={handleUploadDet} className="hidden" />
                    <Button variant="outline" size="sm" onClick={() => fileInputRef.current?.click()} disabled={!cnpjSelecionado || api.loading} className="gap-1">
                      <Upload className="h-3.5 w-3.5" /> Enviar DET
                    </Button>
                    {dets.length > 0 && (
                      <Badge variant="outline" className="text-green-600">
                        <CheckCircle2 className="mr-1 h-3 w-3" /> {dets.length} DET(s)
                      </Badge>
                    )}
                  </div>
                  {relatorio.aviso_det ? (
                    <p className="text-xs text-amber-600">{relatorio.aviso_det}</p>
                  ) : null}
                  {dets.length > 0 && (
                    <div className="space-y-1 rounded border p-2">
                      {dets.map((det) => (
                        <div key={det.nome} className="flex items-center gap-2 text-xs">
                          <Checkbox
                            checked={relatorio.arquivos_notificacao_incluidos.includes(det.nome)}
                            onCheckedChange={(checked) => handleAlternarDet(det.nome, checked === true)}
                            aria-label={`Incluir ${det.nome}`}
                          />
                          <FileText className="h-3 w-3 text-muted-foreground" />
                          <span className="font-mono">{det.nome}</span>
                          <span className="text-muted-foreground">({(det.tamanho_bytes / 1024).toFixed(0)} KB)</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>

                <div className="flex items-center justify-between gap-3 pt-2">
                  <div className="flex items-center gap-2">
                    <Button
                      variant="outline"
                      onClick={handleGerarDocxIndividual}
                      disabled={!cnpjSelecionado || api.loading || !modeloIndividual?.pronto}
                      className="gap-2"
                      title={!modeloIndividual?.pronto ? motivoModeloIndividual : undefined}
                    >
                      {api.loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <FileText className="h-4 w-4" />}
                      Gerar DOCX individual
                    </Button>
                    <Button
                      variant="outline"
                      onClick={handleGerarPdfIndividual}
                      disabled={!cnpjSelecionado || api.loading || !prontoPdfIndividual}
                      className="gap-2"
                      title={!prontoPdf ? motivoPdfIndisponivel : !prontoPdfIndividual ? motivoModeloIndividual : undefined}
                    >
                      {api.loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
                      Gerar PDF individual
                    </Button>
                  </div>
                  <Button onClick={handleSalvarRelatorio} disabled={!cnpjSelecionado || api.loading} className="gap-2">
                    {api.loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Save className="h-4 w-4" />}
                    Salvar relatorio
                  </Button>
                </div>
                {prontoPdf && !prontoPdfIndividual ? (
                  <p className="text-xs text-amber-600">{motivoModeloIndividual}</p>
                ) : null}
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* ============ ABA GERAR PDFs ============ */}
        <TabsContent value="gerar">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-sm">
                <FileStack className="h-4 w-4" />
                Gerar Relatorios em PDF
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-6">
              {/* Gerar por DSF */}
              <div className="space-y-3">
                <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Relatorio Geral Consolidado por DSF</h3>
                <p className="text-xs text-muted-foreground">
                  Selecione uma DSF para gerar o relatorio geral com todos os CNPJs vinculados, incluindo apenas DETs locais realmente disponiveis.
                </p>
                <div className="flex flex-wrap gap-2">
                  {dsfs.length === 0 ? (
                    <p className="text-xs text-muted-foreground">Nenhuma DSF efetiva encontrada no storage.</p>
                  ) : (
                    dsfs.map((dsf) => (
                      <Button
                        key={dsf.numero}
                        variant={dsfAtiva === dsf.numero ? "default" : "outline"}
                        size="sm"
                        onClick={() => handleSelecionarDsf(dsf.numero)}
                        className="gap-1"
                      >
                        <FolderOpen className="h-3 w-3" />
                        DSF {dsf.numero}
                        <Badge variant="secondary" className="ml-1 text-[10px]">{dsf.cnpjs.length}</Badge>
                      </Button>
                    ))
                  )}
                </div>

                {dsfAtiva && (
                  <div className="rounded border p-3 space-y-2">
                    <p className="text-xs font-semibold">DSF {dsfAtiva} — {dsfCnpjs.length} CNPJs vinculados:</p>
                    <div className="flex flex-wrap gap-1.5">
                      {dsfCnpjs.map((cnpj) => {
                        const info = cnpjsComRelatorio.find((c) => c.cnpj === cnpj);
                        return (
                          <Badge key={cnpj} variant={info ? "outline" : "destructive"} className="text-[10px] font-mono">
                            {formatarCnpj(cnpj)}
                            {info && <CheckCircle2 className="ml-1 h-2.5 w-2.5 text-green-600" />}
                          </Badge>
                        );
                      })}
                    </div>
                    <Button
                      onClick={handleGerarPdfGeral}
                      disabled={api.loading || !prontoPdfGeral}
                      className="mt-2 gap-2"
                      title={!prontoPdf ? motivoPdfIndisponivel : !prontoPdfGeral ? motivoModeloGeral : undefined}
                    >
                      {api.loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <Download className="h-4 w-4" />}
                      Gerar relatorio geral consolidado
                    </Button>
                    {!prontoPdf ? <p className="text-xs text-amber-600">{motivoPdfIndisponivel}</p> : null}
                    {prontoPdf && !prontoPdfGeral ? <p className="text-xs text-amber-600">{motivoModeloGeral}</p> : null}
                  </div>
                )}
              </div>

              <Separator />

              {/* Resumo CNPJs com relatorio */}
              <div className="space-y-3">
                <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">CNPJs com relatorio preenchido</h3>
                {cnpjsComRelatorio.length === 0 ? (
                  <p className="text-xs text-muted-foreground">Nenhum CNPJ com relatorio preenchido. Preencha na aba CNPJ.</p>
                ) : (
                  <div className="overflow-x-auto rounded border">
                    <table className="w-full text-xs">
                      <thead className="bg-muted/50">
                        <tr>
                          <th className="px-3 py-2 text-left">CNPJ</th>
                          <th className="px-3 py-2 text-left">Contribuinte</th>
                          <th className="px-3 py-2 text-left">DSF</th>
                          <th className="px-3 py-2 text-center">DET</th>
                          <th className="px-3 py-2 text-left">Manifestacao</th>
                          <th className="px-3 py-2 text-center">Acao</th>
                        </tr>
                      </thead>
                      <tbody>
                        {cnpjsComRelatorio.map((item) => (
                          <tr key={item.cnpj} className="border-t">
                            <td className="px-3 py-2 font-mono">{formatarCnpj(item.cnpj)}</td>
                            <td className="px-3 py-2">{item.contribuinte}</td>
                            <td className="px-3 py-2 font-mono">{item.dsf}</td>
                            <td className="px-3 py-2 text-center">
                              {item.tem_det ? <CheckCircle2 className="mx-auto h-3.5 w-3.5 text-green-600" /> : <span className="text-muted-foreground">—</span>}
                            </td>
                            <td className="px-3 py-2">{item.manifestacao}</td>
                            <td className="px-3 py-2 text-center">
                              <div className="flex items-center justify-center gap-1">
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={async () => {
                                    try {
                                      await api.gerarDocxIndividual(item.cnpj);
                                      toast.success(`DOCX de ${item.contribuinte} gerado`);
                                    } catch (erro) {
                                      toast.error(`Erro: ${(erro as Error).message}`);
                                    }
                                  }}
                                  disabled={api.loading || !modeloIndividual?.pronto}
                                  title={!modeloIndividual?.pronto ? motivoModeloIndividual : undefined}
                                  className="h-7 gap-1 text-[10px]"
                                >
                                  <FileText className="h-3 w-3" /> DOCX
                                </Button>
                                <Button
                                  variant="ghost"
                                  size="sm"
                                  onClick={async () => {
                                    try {
                                      await api.gerarPdfIndividual(item.cnpj);
                                      toast.success(`PDF de ${item.contribuinte} gerado`);
                                    } catch (erro) {
                                      toast.error(`Erro: ${(erro as Error).message}`);
                                    }
                                  }}
                                  disabled={api.loading || !prontoPdfIndividual}
                                  title={!prontoPdf ? motivoPdfIndisponivel : !prontoPdfIndividual ? motivoModeloIndividual : undefined}
                                  className="h-7 gap-1 text-[10px]"
                                >
                                  <Download className="h-3 w-3" /> PDF
                                </Button>
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              {api.error && (
                <div className="rounded border border-destructive/30 p-3">
                  <p className="text-xs text-destructive">{api.error}</p>
                </div>
              )}
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>
    </div>
  );
}
