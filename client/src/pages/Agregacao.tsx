import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { toast } from "sonner";
import { useAgregacao, useTabelas } from "@/hooks/useAuditApi";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";

interface ProdutoLinha {
  id_item?: string;
  id_produto?: string | number;
  descricao?: string;
  ncm?: string;
  cest?: string;
}

interface GrupoLinha {
  id_agrupado: string;
  descr_padrao?: string;
  descricao_padrao?: string;
  qtd_membros: number;
  origem: string;
}

function obterIdentificadorProduto(produto: ProdutoLinha): string {
  return String(produto.id_item ?? produto.id_produto ?? "").trim();
}

export default function Agregacao() {
  const { cnpjAtivo } = useCnpj();
  const { lerTodasPaginas } = useTabelas();
  const { agregar, desagregar, loading } = useAgregacao();

  const [produtos, setProdutos] = useState<ProdutoLinha[]>([]);
  const [grupos, setGrupos] = useState<GrupoLinha[]>([]);
  const [selecionados, setSelecionados] = useState<string[]>([]);
  const [descricaoPadrao, setDescricaoPadrao] = useState("");

  // Top Table Filters
  const [topFiltroDescricao, setTopFiltroDescricao] = useState("");
  const [topFiltroNCM, setTopFiltroNCM] = useState("");
  const [topFiltroCEST, setTopFiltroCEST] = useState("");
  const [topFiltroGlobal, setTopFiltroGlobal] = useState("");

  // Bottom Table Filters
  const [botFiltroDescNorm, setBotFiltroDescNorm] = useState("");
  const [botFiltroNCM, setBotFiltroNCM] = useState("");
  const [botFiltroCEST, setBotFiltroCEST] = useState("");

  const carregarDados = async () => {
    if (!cnpjAtivo) return;

    const mensagensErro: string[] = [];

    try {
      const respostaProdutos = await lerTodasPaginas(
        cnpjAtivo,
        "produtos",
        "parquets",
        undefined,
        undefined,
        "id_produto"
      );
      setProdutos((respostaProdutos?.dados ?? []) as unknown as ProdutoLinha[]);
    } catch (erro) {
      const erroComParciais = erro as Error & { dadosParciais?: unknown[] };
      setProdutos((erroComParciais.dadosParciais ?? []) as ProdutoLinha[]);
      mensagensErro.push(`produtos: ${erroComParciais.message}`);
    }

    try {
      const respostaGrupos = await lerTodasPaginas(
        cnpjAtivo,
        "produtos_agrupados",
        "parquets",
        undefined,
        undefined,
        "descricao_padrao"
      );
      setGrupos((respostaGrupos?.dados ?? []) as unknown as GrupoLinha[]);
    } catch (erro) {
      const erroComParciais = erro as Error & { dadosParciais?: unknown[] };
      setGrupos((erroComParciais.dadosParciais ?? []) as GrupoLinha[]);
      mensagensErro.push(`grupos: ${erroComParciais.message}`);
    }

    if (mensagensErro.length > 0) {
      toast.error("Falha ao carregar dados de agregacao", {
        description: mensagensErro.join(" | "),
      });
    }
  };

  useEffect(() => {
    carregarDados();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cnpjAtivo]);

  const limparFiltrosTop = () => {
    setTopFiltroDescricao("");
    setTopFiltroNCM("");
    setTopFiltroCEST("");
    setTopFiltroGlobal("");
  };

  const limparFiltrosBot = () => {
    setBotFiltroDescNorm("");
    setBotFiltroNCM("");
    setBotFiltroCEST("");
  };

  const produtosFiltrados = useMemo(() => {
    return produtos.filter(produto => {
      const descMatch =
        !topFiltroDescricao ||
        produto.descricao
          ?.toLowerCase()
          .includes(topFiltroDescricao.toLowerCase());
      const ncmMatch =
        !topFiltroNCM ||
        String(produto.ncm ?? "")
          .toLowerCase()
          .includes(topFiltroNCM.toLowerCase());
      const cestMatch =
        !topFiltroCEST ||
        String(produto.cest ?? "")
          .toLowerCase()
          .includes(topFiltroCEST.toLowerCase());
      const globalMatch =
        !topFiltroGlobal ||
        obterIdentificadorProduto(produto)
          .toLowerCase()
          .includes(topFiltroGlobal.toLowerCase()) ||
        produto.descricao
          ?.toLowerCase()
          .includes(topFiltroGlobal.toLowerCase()) ||
        String(produto.ncm ?? "")
          .toLowerCase()
          .includes(topFiltroGlobal.toLowerCase()) ||
        String(produto.cest ?? "")
          .toLowerCase()
          .includes(topFiltroGlobal.toLowerCase());

      return descMatch && ncmMatch && cestMatch && globalMatch;
    });
  }, [
    produtos,
    topFiltroDescricao,
    topFiltroNCM,
    topFiltroCEST,
    topFiltroGlobal,
  ]);

  const gruposFiltrados = useMemo(() => {
    return grupos.filter(grupo => {
      const descMatch =
        !botFiltroDescNorm ||
        (grupo.descricao_padrao ?? grupo.descr_padrao ?? "")
          .toLowerCase()
          .includes(botFiltroDescNorm.toLowerCase());
      // Assuming groups don't explicitly have ncm/cest in this simple view, but we keep the filters for future expansion.
      return descMatch;
    });
  }, [grupos, botFiltroDescNorm, botFiltroNCM, botFiltroCEST]);

  const alternarSelecionado = (id: string) => {
    setSelecionados(atual =>
      atual.includes(id) ? atual.filter(item => item !== id) : [...atual, id]
    );
  };

  const executarAgregacao = async () => {
    if (!cnpjAtivo) return;
    if (selecionados.length < 2) {
      toast.error("Selecione pelo menos 2 produtos para agregar");
      return;
    }

    try {
      await agregar(cnpjAtivo, selecionados, descricaoPadrao || undefined);
      toast.success("Agregacao registrada com sucesso");
      setSelecionados([]);
      setDescricaoPadrao("");
      await carregarDados();
    } catch (erro) {
      toast.error("Falha ao agregar", { description: (erro as Error).message });
    }
  };

  const executarDesagregacao = async (idGrupo: string) => {
    if (!cnpjAtivo) return;

    try {
      await desagregar(cnpjAtivo, idGrupo);
      toast.success("Grupo desagregado com sucesso");
      await carregarDados();
    } catch (erro) {
      toast.error("Falha ao desagregar", {
        description: (erro as Error).message,
      });
    }
  };

  if (!cnpjAtivo) {
    return (
      <Card>
        <CardContent className="py-10 text-center text-sm text-muted-foreground">
          Selecione um CNPJ ativo no cabecalho para editar agregacoes.
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader className="bg-muted/30">
          <CardTitle className="text-sm font-semibold">
            Tabela Agrupada Filtravel (Selecione linhas para agregar)
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4 pt-4">
          <div className="flex flex-wrap items-center gap-2 mb-4">
            <Input
              className="w-[200px]"
              placeholder="Filtrar Descricao..."
              value={topFiltroDescricao}
              onChange={e => setTopFiltroDescricao(e.target.value)}
            />
            <Input
              className="w-[120px]"
              placeholder="Filtrar NCM"
              value={topFiltroNCM}
              onChange={e => setTopFiltroNCM(e.target.value)}
            />
            <Input
              className="w-[120px]"
              placeholder="Filtrar CEST"
              value={topFiltroCEST}
              onChange={e => setTopFiltroCEST(e.target.value)}
            />
            <Input
              className="w-[200px]"
              placeholder="Busca global..."
              value={topFiltroGlobal}
              onChange={e => setTopFiltroGlobal(e.target.value)}
            />
            <Button variant="secondary" onClick={limparFiltrosTop}>
              Limpar filtros
            </Button>
          </div>

          <div className="flex items-center gap-2 bg-muted p-2 rounded border">
            <Input
              className="w-[300px]"
              placeholder="Descricao padrao do novo grupo (opcional)"
              value={descricaoPadrao}
              onChange={event => setDescricaoPadrao(event.target.value)}
            />
            <Button
              onClick={executarAgregacao}
              disabled={loading || selecionados.length < 2}
            >
              Agregar selecionados ({selecionados.length})
            </Button>
            <Button
              variant="outline"
              onClick={() => setSelecionados([])}
              disabled={selecionados.length === 0}
            >
              Desfazer selecao
            </Button>
          </div>

          <div className="overflow-x-auto rounded border h-[300px] relative">
            <table className="w-full text-xs">
              <thead className="bg-muted/50 sticky top-0 z-10 shadow-sm">
                <tr>
                  <th className="px-3 py-2 text-left w-10">Sel</th>
                  <th className="px-3 py-2 text-left">ID</th>
                  <th className="px-3 py-2 text-left">Descricao</th>
                  <th className="px-3 py-2 text-left">NCM</th>
                  <th className="px-3 py-2 text-left">CEST</th>
                </tr>
              </thead>
              <tbody>
                {produtosFiltrados.map(produto => (
                  <tr
                    key={obterIdentificadorProduto(produto)}
                    className={`border-t hover:bg-muted/30 ${selecionados.includes(obterIdentificadorProduto(produto)) ? "bg-primary/10" : ""}`}
                  >
                    <td className="px-3 py-2">
                      <Checkbox
                        checked={selecionados.includes(
                          obterIdentificadorProduto(produto)
                        )}
                        onCheckedChange={() =>
                          alternarSelecionado(
                            obterIdentificadorProduto(produto)
                          )
                        }
                      />
                    </td>
                    <td className="px-3 py-2 font-mono text-[10px] text-muted-foreground">
                      {obterIdentificadorProduto(produto)}
                    </td>
                    <td className="px-3 py-2 font-medium">
                      {produto.descricao}
                    </td>
                    <td className="px-3 py-2 font-mono">{produto.ncm ?? ""}</td>
                    <td className="px-3 py-2 font-mono">
                      {produto.cest ?? ""}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="text-xs text-muted-foreground text-right">
            {produtosFiltrados.length} linhas filtradas
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="bg-muted/30">
          <CardTitle className="text-sm font-semibold">
            Linhas Agregadas (Mesma Tabela de Referencia)
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4 pt-4">
          <div className="flex flex-wrap items-center gap-2 mb-4">
            <Input
              className="w-[200px]"
              placeholder="Filtrar Desc. Norm"
              value={botFiltroDescNorm}
              onChange={e => setBotFiltroDescNorm(e.target.value)}
            />
            <Input
              className="w-[120px]"
              placeholder="Filtrar NCM"
              value={botFiltroNCM}
              onChange={e => setBotFiltroNCM(e.target.value)}
            />
            <Input
              className="w-[120px]"
              placeholder="Filtrar CEST"
              value={botFiltroCEST}
              onChange={e => setBotFiltroCEST(e.target.value)}
            />
            <Button variant="secondary" onClick={limparFiltrosBot}>
              Limpar filtros
            </Button>
          </div>

          <div className="space-y-2 h-[250px] overflow-y-auto pr-2">
            {gruposFiltrados.map(grupo => (
              <div
                key={grupo.id_agrupado}
                className="flex items-center justify-between rounded border p-3 text-sm bg-card hover:bg-muted/20 transition-colors"
              >
                <div>
                  <p className="font-mono text-xs text-muted-foreground mb-1">
                    {grupo.id_agrupado}
                  </p>
                  <p className="font-medium">
                    {grupo.descricao_padrao ??
                      grupo.descr_padrao ??
                      "Sem descricao"}
                  </p>
                </div>
                <div className="flex items-center gap-3">
                  <Badge variant="secondary" className="font-normal">
                    {grupo.qtd_membros} membros
                  </Badge>
                  <Badge variant="outline" className="font-normal">
                    {grupo.origem}
                  </Badge>
                  <Button
                    variant="destructive"
                    size="sm"
                    onClick={() => executarDesagregacao(grupo.id_agrupado)}
                    disabled={loading}
                  >
                    Desagregar
                  </Button>
                </div>
              </div>
            ))}
            {gruposFiltrados.length === 0 ? (
              <p className="text-sm text-muted-foreground p-4 text-center border rounded border-dashed">
                Nenhuma linha agrupada ou filtro muito restritivo.
              </p>
            ) : null}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
