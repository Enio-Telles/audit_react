import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { toast } from "sonner";
import { useConversao } from "@/hooks/useAuditApi";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";
import type { FatorConversao } from "@/types/audit";

export default function Conversao() {
  const { cnpjAtivo } = useCnpj();
  const { loading, editarFator, recalcular, listarFatores } = useConversao();

  const [fatores, setFatores] = useState<FatorConversao[]>([]);
  const [filtroStatus, setFiltroStatus] = useState<"todos" | "ok" | "pendente" | "erro">("todos");
  const [termo, setTermo] = useState("");
  const [edicao, setEdicao] = useState<Record<string, { unid_ref: string; fator_compra_ref: string; fator_venda_ref: string }>>({});

  const carregarFatores = async () => {
    if (!cnpjAtivo) return;
    try {
      const lista = await listarFatores(cnpjAtivo);
      setFatores(lista);
    } catch (erro) {
      const erroComParciais = erro as Error & { dadosParciais?: FatorConversao[] };
      setFatores(erroComParciais.dadosParciais ?? []);
      toast.error("Falha ao carregar fatores", { description: (erro as Error).message });
    }
  };

  useEffect(() => {
    carregarFatores();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cnpjAtivo]);

  const fatoresFiltrados = useMemo(() => {
    return fatores.filter((fator) => {
      const matchStatus = filtroStatus === "todos" || fator.status === filtroStatus;
      const texto = `${fator.id_agrupado} ${fator.descricao_padrao}`.toLowerCase();
      const matchTexto = texto.includes(termo.toLowerCase());
      return matchStatus && matchTexto;
    });
  }, [fatores, filtroStatus, termo]);

  const atualizarCampo = (id: string, campo: "unid_ref" | "fator_compra_ref" | "fator_venda_ref", valor: string) => {
    setEdicao((atual) => ({
      ...atual,
      [id]: {
        unid_ref: atual[id]?.unid_ref ?? "",
        fator_compra_ref: atual[id]?.fator_compra_ref ?? "",
        fator_venda_ref: atual[id]?.fator_venda_ref ?? "",
        [campo]: valor,
      },
    }));
  };

  const salvarFator = async (fator: FatorConversao) => {
    if (!cnpjAtivo) return;

    const alteracao = edicao[fator.id_agrupado];
    if (!alteracao) {
      toast.info("Nenhuma alteracao para salvar");
      return;
    }

    const payload = {
      unid_ref: alteracao.unid_ref || fator.unid_ref,
      fator_compra_ref: alteracao.fator_compra_ref ? Number(alteracao.fator_compra_ref) : fator.fator_compra_ref,
      fator_venda_ref: alteracao.fator_venda_ref ? Number(alteracao.fator_venda_ref) : fator.fator_venda_ref,
    };

    try {
      await editarFator(cnpjAtivo, fator.id_agrupado, payload);
      toast.success("Fator atualizado");
      setEdicao((atual) => {
        const proximo = { ...atual };
        delete proximo[fator.id_agrupado];
        return proximo;
      });
      await carregarFatores();
    } catch (erro) {
      toast.error("Falha ao salvar fator", { description: (erro as Error).message });
    }
  };

  const executarRecalculo = async () => {
    if (!cnpjAtivo) return;
    try {
      await recalcular(cnpjAtivo);
      toast.success("Recalculo concluido");
      await carregarFatores();
    } catch (erro) {
      toast.error("Falha no recalculo", { description: (erro as Error).message });
    }
  };

  if (!cnpjAtivo) {
    return (
      <Card>
        <CardContent className="py-10 text-center text-sm text-muted-foreground">
          Selecione um CNPJ ativo no cabecalho para editar fatores.
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">Fatores de conversao - {formatarCnpj(cnpjAtivo)}</CardTitle>
        </CardHeader>
        <CardContent className="grid grid-cols-1 gap-3 md:grid-cols-4">
          <Input placeholder="Buscar por ID ou descricao" value={termo} onChange={(event) => setTermo(event.target.value)} />
          <Select value={filtroStatus} onValueChange={(value) => setFiltroStatus(value as typeof filtroStatus)}>
            <SelectTrigger>
              <SelectValue placeholder="Status" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="todos">Todos</SelectItem>
              <SelectItem value="ok">OK</SelectItem>
              <SelectItem value="pendente">Pendentes</SelectItem>
              <SelectItem value="erro">Erro</SelectItem>
            </SelectContent>
          </Select>
          <div className="md:col-span-2 flex items-center gap-2 justify-end">
            <Button variant="outline" onClick={carregarFatores} disabled={loading}>
              Atualizar
            </Button>
            <Button onClick={executarRecalculo} disabled={loading}>
              Recalcular derivados
            </Button>
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardContent className="pt-4">
          <div className="overflow-x-auto rounded border">
            <table className="w-full text-xs">
              <thead className="bg-muted/50">
                <tr>
                  <th className="px-3 py-2 text-left">ID</th>
                  <th className="px-3 py-2 text-left">Descricao</th>
                  <th className="px-3 py-2 text-left">Unid ref</th>
                  <th className="px-3 py-2 text-left">Fator compra</th>
                  <th className="px-3 py-2 text-left">Fator venda</th>
                  <th className="px-3 py-2 text-left">Status</th>
                  <th className="px-3 py-2 text-left">Acao</th>
                </tr>
              </thead>
              <tbody>
                {fatoresFiltrados.map((fator) => {
                  const alteracao = edicao[fator.id_agrupado];
                  return (
                    <tr key={fator.id_agrupado} className="border-t">
                      <td className="px-3 py-2 font-mono">{fator.id_agrupado}</td>
                      <td className="px-3 py-2">{fator.descricao_padrao}</td>
                      <td className="px-3 py-2">
                        <Input
                          value={alteracao?.unid_ref ?? fator.unid_ref}
                          onChange={(event) => atualizarCampo(fator.id_agrupado, "unid_ref", event.target.value)}
                          className="h-8"
                        />
                      </td>
                      <td className="px-3 py-2">
                        <Input
                          value={alteracao?.fator_compra_ref ?? String(fator.fator_compra_ref)}
                          onChange={(event) => atualizarCampo(fator.id_agrupado, "fator_compra_ref", event.target.value)}
                          type="number"
                          step="0.0001"
                          className="h-8"
                        />
                      </td>
                      <td className="px-3 py-2">
                        <Input
                          value={alteracao?.fator_venda_ref ?? String(fator.fator_venda_ref)}
                          onChange={(event) => atualizarCampo(fator.id_agrupado, "fator_venda_ref", event.target.value)}
                          type="number"
                          step="0.0001"
                          className="h-8"
                        />
                      </td>
                      <td className="px-3 py-2">
                        <Badge variant={fator.status === "ok" ? "default" : "secondary"}>{fator.status}</Badge>
                      </td>
                      <td className="px-3 py-2">
                        <Button size="sm" onClick={() => salvarFator(fator)} disabled={loading}>
                          Salvar
                        </Button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          {fatoresFiltrados.length === 0 ? (
            <p className="mt-3 text-sm text-muted-foreground">Nenhum fator encontrado para os filtros atuais.</p>
          ) : null}
        </CardContent>
      </Card>
    </div>
  );
}
