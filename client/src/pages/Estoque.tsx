import { useEffect, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { toast } from "sonner";
import { useTabelas } from "@/hooks/useAuditApi";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";

const TABELAS_ESTOQUE = [
  "mov_estoque",
  "aba_mensal",
  "aba_anual",
  "resumo_global",
  "produtos_selecionados",
  "id_agrupados",
  "nfe_entrada",
] as const;

export default function Estoque() {
  const { cnpjAtivo } = useCnpj();
  const { ler, loading } = useTabelas();

  const [tabelaAtiva, setTabelaAtiva] =
    useState<(typeof TABELAS_ESTOQUE)[number]>("mov_estoque");
  const [dados, setDados] = useState<Record<string, unknown>[]>([]);
  const [colunas, setColunas] = useState<string[]>([]);
  const [total, setTotal] = useState(0);

  const carregarTabela = async (nomeTabela: string) => {
    if (!cnpjAtivo) return;

    try {
      const resposta = await ler(cnpjAtivo, nomeTabela, "parquets", 1, 1000);
      setDados(resposta?.dados ?? []);
      setColunas(resposta?.colunas ?? []);
      setTotal(resposta?.total_registros ?? 0);
    } catch (erro) {
      toast.error(`Falha ao carregar ${nomeTabela}`, {
        description: (erro as Error).message,
      });
      setDados([]);
      setColunas([]);
      setTotal(0);
    }
  };

  useEffect(() => {
    carregarTabela(tabelaAtiva);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cnpjAtivo, tabelaAtiva]);

  if (!cnpjAtivo) {
    return (
      <Card>
        <CardContent className="py-10 text-center text-sm text-muted-foreground">
          Selecione um CNPJ ativo no cabecalho para visualizar tabelas de
          estoque.
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">
            Estoque e consolidacoes - {formatarCnpj(cnpjAtivo)}
          </CardTitle>
        </CardHeader>
      </Card>

      <Tabs
        value={tabelaAtiva}
        onValueChange={valor =>
          setTabelaAtiva(valor as (typeof TABELAS_ESTOQUE)[number])
        }
      >
        <TabsList className="flex h-auto flex-wrap gap-1 p-1">
          {TABELAS_ESTOQUE.map(tabela => (
            <TabsTrigger key={tabela} value={tabela} className="text-xs">
              {tabela}
            </TabsTrigger>
          ))}
        </TabsList>

        {TABELAS_ESTOQUE.map(tabela => (
          <TabsContent key={tabela} value={tabela} className="mt-4">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center justify-between text-sm font-semibold">
                  <span>{tabela}</span>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-muted-foreground">
                      {total} registros
                    </span>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => carregarTabela(tabela)}
                      disabled={loading}
                    >
                      Atualizar
                    </Button>
                  </div>
                </CardTitle>
              </CardHeader>
              <CardContent>
                {loading ? (
                  <p className="text-sm text-muted-foreground">Carregando...</p>
                ) : dados.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    Tabela sem dados para o CNPJ selecionado.
                  </p>
                ) : (
                  <div className="overflow-x-auto rounded border">
                    <table className="w-full text-xs">
                      <thead className="bg-muted/50">
                        <tr>
                          {colunas.map(coluna => (
                            <th
                              key={coluna}
                              className="px-3 py-2 text-left font-semibold"
                            >
                              {coluna}
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {dados.map((linha, indice) => (
                          <tr key={indice} className="border-t">
                            {colunas.map(coluna => (
                              <td
                                key={`${indice}-${coluna}`}
                                className="px-3 py-2 align-top"
                              >
                                {String(linha[coluna] ?? "")}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </CardContent>
            </Card>
          </TabsContent>
        ))}
      </Tabs>
    </div>
  );
}
