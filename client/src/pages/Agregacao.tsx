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
  const [filtro, setFiltro] = useState("");
  const [descricaoPadrao, setDescricaoPadrao] = useState("");

  const carregarDados = async () => {
    if (!cnpjAtivo) return;

    const mensagensErro: string[] = [];

    try {
      const respostaProdutos = await lerTodasPaginas(cnpjAtivo, "produtos", "parquets", undefined, undefined, "id_produto");
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
        "descricao_padrao",
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

  const produtosFiltrados = useMemo(() => {
    const termo = filtro.toLowerCase();
    return produtos.filter(
      (produto) =>
        obterIdentificadorProduto(produto).toLowerCase().includes(termo) ||
        produto.descricao?.toLowerCase().includes(termo) ||
        String(produto.ncm ?? "").toLowerCase().includes(termo),
    );
  }, [filtro, produtos]);

  const alternarSelecionado = (id: string) => {
    setSelecionados((atual) =>
      atual.includes(id) ? atual.filter((item) => item !== id) : [...atual, id],
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
      toast.error("Falha ao desagregar", { description: (erro as Error).message });
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
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Agregacao manual - {formatarCnpj(cnpjAtivo)}</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-3">
            <Input placeholder="Filtrar produtos" value={filtro} onChange={(event) => setFiltro(event.target.value)} />
            <Input
              placeholder="Descricao padrao do grupo (opcional)"
              value={descricaoPadrao}
              onChange={(event) => setDescricaoPadrao(event.target.value)}
            />
            <div className="flex items-center gap-2">
              <Button onClick={executarAgregacao} disabled={loading || selecionados.length < 2}>
                Agregar selecionados ({selecionados.length})
              </Button>
              <Button variant="outline" onClick={() => setSelecionados([])}>
                Limpar
              </Button>
            </div>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Produtos candidatos</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto rounded border">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  <th className="px-3 py-2 text-left">Sel</th>
                  <th className="px-3 py-2 text-left">ID</th>
                  <th className="px-3 py-2 text-left">Descricao</th>
                  <th className="px-3 py-2 text-left">NCM</th>
                  <th className="px-3 py-2 text-left">CEST</th>
                </tr>
              </thead>
              <tbody>
                {produtosFiltrados.map((produto) => (
                  <tr key={obterIdentificadorProduto(produto)} className="border-t">
                    <td className="px-3 py-2">
                      <Checkbox
                        checked={selecionados.includes(obterIdentificadorProduto(produto))}
                        onCheckedChange={() => alternarSelecionado(obterIdentificadorProduto(produto))}
                      />
                    </td>
                    <td className="px-3 py-2 font-mono">{obterIdentificadorProduto(produto)}</td>
                    <td className="px-3 py-2">{produto.descricao}</td>
                    <td className="px-3 py-2 font-mono">{produto.ncm ?? ""}</td>
                    <td className="px-3 py-2 font-mono">{produto.cest ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Grupos existentes</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            {grupos.map((grupo) => (
              <div key={grupo.id_agrupado} className="flex items-center justify-between rounded border p-3 text-sm">
                <div>
                  <p className="font-mono">{grupo.id_agrupado}</p>
                  <p>{grupo.descricao_padrao ?? grupo.descr_padrao ?? ""}</p>
                </div>
                <div className="flex items-center gap-2">
                  <Badge variant="outline">{grupo.qtd_membros} membros</Badge>
                  <Badge variant="outline">{grupo.origem}</Badge>
                  <Button variant="destructive" size="sm" onClick={() => executarDesagregacao(grupo.id_agrupado)}>
                    Desagregar
                  </Button>
                </div>
              </div>
            ))}
            {grupos.length === 0 ? <p className="text-sm text-muted-foreground">Nenhum grupo encontrado.</p> : null}
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
