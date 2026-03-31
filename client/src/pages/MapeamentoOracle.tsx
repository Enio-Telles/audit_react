import { useEffect, useMemo, useState } from "react";
import { toast } from "sonner";
import { Boxes, Database, FileText, RefreshCw, Table2 } from "lucide-react";
import { useSistema } from "@/hooks/useAuditApi";
import type {
  OracleArquivoSqlAnalise,
  OracleBlocoExtracao,
  OracleDiretorioSqlSugerido,
  OracleFonteRaizMapeada,
  OracleMapeamentoRaizResponse,
} from "@/types/audit";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Empty, EmptyContent, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "@/components/ui/empty";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";

function CartaoKpi({ titulo, valor, detalhe }: { titulo: string; valor: string | number; detalhe: string }) {
  return (
    <Card>
      <CardContent className="pt-4">
        <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">{titulo}</p>
        <p className="mt-2 font-mono text-2xl font-semibold text-foreground">{valor}</p>
        <p className="mt-1 text-xs text-muted-foreground">{detalhe}</p>
      </CardContent>
    </Card>
  );
}

function ListaBadges({ itens }: { itens: string[] }) {
  if (itens.length === 0) {
    return <span className="text-xs text-muted-foreground">-</span>;
  }

  return (
    <div className="flex flex-wrap gap-1.5">
      {itens.map((item) => (
        <Badge key={item} variant="outline" className="font-mono text-[11px]">
          {item}
        </Badge>
      ))}
    </div>
  );
}

function CartaoBloco({ bloco }: { bloco: OracleBlocoExtracao }) {
  return (
    <Card className="border-border/80">
      <CardHeader className="space-y-2">
        <div className="flex items-start justify-between gap-3">
          <div>
            <CardTitle className="text-sm font-semibold">{bloco.nome_bloco}</CardTitle>
            <CardDescription className="mt-1 font-mono text-[11px]">{bloco.parquet_saida}</CardDescription>
          </div>
          <Badge variant={bloco.paralelizavel ? "default" : "secondary"}>
            {bloco.paralelizavel ? "paralelizavel" : "serial"}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-3 text-sm">
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Fontes Oracle</p>
          <ListaBadges itens={bloco.fontes_oracle} />
        </div>
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Filtro CNPJ</p>
          <p className="text-sm text-foreground">{bloco.filtro_cnpj}</p>
        </div>
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Filtro temporal</p>
          <p className="text-sm text-foreground">{bloco.filtro_temporal}</p>
        </div>
        <div className="space-y-1">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Chave principal</p>
          <ListaBadges itens={bloco.chave_principal} />
        </div>
      </CardContent>
    </Card>
  );
}

function LinhaFonte({ fonte }: { fonte: OracleFonteRaizMapeada }) {
  return (
    <TableRow>
      <TableCell className="align-top">
        <div className="space-y-1">
          <p className="font-mono text-xs font-semibold">{fonte.fonte_oracle}</p>
          <p className="text-[11px] text-muted-foreground">{fonte.dominio}</p>
        </div>
      </TableCell>
      <TableCell className="align-top">
        <div className="space-y-1">
          <p className="font-mono text-xs">{fonte.camada_bronze}</p>
          <p className="font-mono text-[11px] text-muted-foreground">{fonte.camada_silver}</p>
          <p className="font-mono text-[11px] text-muted-foreground">{fonte.camada_gold}</p>
        </div>
      </TableCell>
      <TableCell className="align-top">
        <div className="space-y-1">
          <p className="text-xs font-medium">{fonte.chave_recorte}</p>
          <ListaBadges itens={fonte.chave_principal} />
        </div>
      </TableCell>
      <TableCell className="align-top">
        <div className="space-y-1">
          <p className="text-xs">{fonte.filtro_cnpj}</p>
          <p className="text-[11px] text-muted-foreground">{fonte.filtro_temporal}</p>
        </div>
      </TableCell>
      <TableCell className="align-top">
        <div className="space-y-2">
          <ListaBadges itens={fonte.arquivos_sql} />
          <p className="text-[11px] text-muted-foreground">{fonte.ocorrencias} ocorrencia(s)</p>
        </div>
      </TableCell>
    </TableRow>
  );
}

function PainelArquivoSql({ arquivo }: { arquivo: OracleArquivoSqlAnalise }) {
  return (
    <AccordionItem value={arquivo.arquivo}>
      <AccordionTrigger className="hover:no-underline">
        <div className="flex w-full flex-col gap-2">
          <div className="flex flex-wrap items-center gap-2">
            <span className="font-mono text-xs font-semibold text-foreground">{arquivo.arquivo}</span>
            <Badge variant={arquivo.tem_bind_cnpj ? "default" : "destructive"}>
              {arquivo.tem_bind_cnpj ? "com bind CNPJ" : "sem bind CNPJ"}
            </Badge>
            {arquivo.tem_window_function ? <Badge variant="outline">window</Badge> : null}
            {arquivo.tem_group_by ? <Badge variant="outline">group by</Badge> : null}
            {arquivo.tem_distinct ? <Badge variant="outline">distinct</Badge> : null}
            {arquivo.tem_union ? <Badge variant="outline">union</Badge> : null}
          </div>
          <p className="text-sm text-muted-foreground">{arquivo.objetivo_real}</p>
        </div>
      </AccordionTrigger>
      <AccordionContent className="space-y-4">
        <div className="grid gap-4 lg:grid-cols-2">
          <div className="space-y-2">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Tabelas raiz</p>
            <ListaBadges itens={arquivo.tabelas_raiz} />
          </div>
          <div className="space-y-2">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Binds</p>
            <ListaBadges itens={arquivo.binds} />
          </div>
          <div className="space-y-2">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Categorias</p>
            <ListaBadges itens={arquivo.categorias} />
          </div>
          <div className="space-y-2">
            <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Dimensoes fiscais</p>
            <ListaBadges itens={arquivo.dimensoes_fiscais} />
          </div>
        </div>

        <div className="space-y-2 rounded border border-border/70 bg-muted/20 p-3">
          <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Gargalos e custo operacional</p>
          <div className="space-y-2">
            {arquivo.gargalos.map((gargalo) => (
              <p key={gargalo} className="text-sm text-foreground">
                {gargalo}
              </p>
            ))}
          </div>
        </div>
      </AccordionContent>
    </AccordionItem>
  );
}

export default function MapeamentoOracle() {
  const { carregarMapeamentoRaizOracle } = useSistema();
  const [loading, setLoading] = useState(false);
  const [erro, setErro] = useState<string | null>(null);
  const [mapa, setMapa] = useState<OracleMapeamentoRaizResponse | null>(null);
  const [diretorioInformado, setDiretorioInformado] = useState("");

  const carregarMapa = async (diretorio?: string) => {
    setLoading(true);
    setErro(null);
    try {
      const resposta = await carregarMapeamentoRaizOracle(diretorio);
      setMapa(resposta);
      setDiretorioInformado(resposta.diretorio_analisado);
    } catch (erroLocal) {
      const mensagem = (erroLocal as Error).message;
      setErro(mensagem);
      toast.error("Falha ao analisar SQLs Oracle", { description: mensagem });
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    carregarMapa().catch(() => undefined);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const diretoriosSugeridos = useMemo<OracleDiretorioSqlSugerido[]>(
    () => mapa?.diretorios_sugeridos ?? [],
    [mapa],
  );
  const blocos = useMemo<OracleBlocoExtracao[]>(() => mapa?.blocos_extracao ?? [], [mapa]);
  const fontes = useMemo<OracleFonteRaizMapeada[]>(() => mapa?.fontes_raiz ?? [], [mapa]);
  const arquivosSql = useMemo<OracleArquivoSqlAnalise[]>(() => mapa?.arquivos_sql ?? [], [mapa]);

  return (
    <div className="mx-auto max-w-7xl space-y-6">
      <Card className="border-border/80">
        <CardHeader>
          <CardTitle className="flex items-center gap-2 text-sm font-semibold">
            <Database className="h-4 w-4 text-primary" />
            Mapeamento raiz das fontes Oracle
          </CardTitle>
          <CardDescription className="text-xs">
            Analise estrutural dos SQLs fiscais para decompor extracoes por CNPJ, persistencia em Parquet e recomposicao lazy em Polars.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-3 lg:grid-cols-[1.4fr_auto]">
            <Input
              value={diretorioInformado}
              onChange={(event) => setDiretorioInformado(event.target.value)}
              placeholder="Diretorio com SQLs fiscais de referencia"
              className="font-mono text-sm"
            />
            <Button onClick={() => carregarMapa(diretorioInformado)} disabled={loading} className="gap-2">
              <RefreshCw className={`h-4 w-4 ${loading ? "animate-spin" : ""}`} />
              Reanalisar SQLs
            </Button>
          </div>

          <div className="flex flex-wrap gap-2">
            {diretoriosSugeridos.map((item) => (
              <Button
                key={`${item.chave}-${item.caminho}`}
                variant={item.caminho === diretorioInformado ? "default" : "outline"}
                size="sm"
                onClick={() => carregarMapa(item.caminho)}
                disabled={loading}
                className="font-mono text-[11px]"
              >
                {item.rotulo}
              </Button>
            ))}
          </div>

          <div className="grid grid-cols-1 gap-4 md:grid-cols-4">
            <CartaoKpi
              titulo="SQLs lidos"
              valor={mapa?.resumo.total_sqls ?? 0}
              detalhe="Arquivos fiscais encontrados no diretorio analisado"
            />
            <CartaoKpi
              titulo="Fontes raiz"
              valor={mapa?.resumo.total_fontes_raiz ?? 0}
              detalhe="Tabelas e views Oracle usadas como origem"
            />
            <CartaoKpi
              titulo="Blocos CNPJ"
              valor={mapa?.resumo.total_blocos_extracao ?? 0}
              detalhe="Blocos recomendados para extracao em Parquet"
            />
            <CartaoKpi
              titulo="Bind CNPJ"
              valor={mapa?.resumo.total_sqls_com_bind_cnpj ?? 0}
              detalhe="Consultas que ja respeitam recorte explicito por contribuinte"
            />
          </div>

          <div className="grid gap-3 rounded border border-border/70 bg-muted/20 p-4 text-sm lg:grid-cols-2">
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Diretorio analisado</p>
              <p className="mt-1 font-mono text-xs text-foreground">{mapa?.diretorio_analisado ?? "-"}</p>
            </div>
            <div>
              <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Diretorio SQL ativo da extracao</p>
              <p className="mt-1 font-mono text-xs text-foreground">{mapa?.diretorio_ativo ?? "-"}</p>
            </div>
          </div>
        </CardContent>
      </Card>

      {erro && !mapa ? (
        <Empty className="border">
          <EmptyHeader>
            <EmptyMedia variant="icon">
              <Database className="size-5" />
            </EmptyMedia>
            <EmptyTitle>Analise indisponivel</EmptyTitle>
            <EmptyDescription>{erro}</EmptyDescription>
          </EmptyHeader>
          <EmptyContent>
            <Button variant="outline" onClick={() => carregarMapa(diretorioInformado)} disabled={loading}>
              Tentar novamente
            </Button>
          </EmptyContent>
        </Empty>
      ) : null}

      {mapa ? (
        <>
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-sm font-semibold">
                <Boxes className="h-4 w-4 text-primary" />
                Blocos de extracao por CNPJ
              </CardTitle>
              <CardDescription className="text-xs">
                Cada bloco representa a menor unidade reutilizavel recomendada para sair do Oracle e entrar no lake Parquet.
              </CardDescription>
            </CardHeader>
            <CardContent className="grid gap-4 xl:grid-cols-2">
              {blocos.map((bloco) => (
                <CartaoBloco key={bloco.nome_bloco} bloco={bloco} />
              ))}
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-sm font-semibold">
                <Table2 className="h-4 w-4 text-primary" />
                Fontes raiz Oracle
              </CardTitle>
              <CardDescription className="text-xs">
                Mapa operacional das tabelas base, chaves de recorte, filtros e destino sugerido em camadas bronze, silver e gold.
              </CardDescription>
            </CardHeader>
            <CardContent>
              <ScrollArea className="rounded border">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Fonte Oracle</TableHead>
                      <TableHead>Parquets sugeridos</TableHead>
                      <TableHead>Chaves</TableHead>
                      <TableHead>Recorte</TableHead>
                      <TableHead>SQLs</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {fontes.map((fonte) => (
                      <LinhaFonte key={fonte.fonte_oracle} fonte={fonte} />
                    ))}
                  </TableBody>
                </Table>
              </ScrollArea>
            </CardContent>
          </Card>

          <div className="grid gap-6 xl:grid-cols-[1.2fr_0.8fr]">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-sm font-semibold">
                  <FileText className="h-4 w-4 text-primary" />
                  Leitura estrutural dos SQLs
                </CardTitle>
                <CardDescription className="text-xs">
                  Fontes, binds, dimensoes fiscais e gargalos provaveis no banco para cada arquivo de referencia.
                </CardDescription>
              </CardHeader>
              <CardContent>
                <Accordion type="multiple" className="w-full">
                  {arquivosSql.map((arquivo) => (
                    <PainelArquivoSql key={arquivo.arquivo} arquivo={arquivo} />
                  ))}
                </Accordion>
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <CardTitle className="flex items-center gap-2 text-sm font-semibold">
                  <Database className="h-4 w-4 text-primary" />
                  Estrategia de recomposicao em Polars
                </CardTitle>
                <CardDescription className="text-xs">
                  Ordem sugerida para scans, joins e materializacao lazy fora do Oracle.
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-3">
                {mapa.estrategia_polars.map((passo, indice) => (
                  <div key={passo} className="rounded border border-border/70 bg-muted/20 p-3">
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-muted-foreground">Passo {indice + 1}</p>
                    <p className="mt-2 text-sm text-foreground">{passo}</p>
                  </div>
                ))}
              </CardContent>
            </Card>
          </div>
        </>
      ) : null}
    </div>
  );
}
