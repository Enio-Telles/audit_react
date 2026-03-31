import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { DataTable } from "@/components/DataTable";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Download, RefreshCw } from "lucide-react";
import { toast } from "sonner";
import { useTabelas } from "@/hooks/useAuditApi";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";
import type { CamadaTabela } from "@/types/audit";

export default function Consulta() {
  const { cnpjAtivo } = useCnpj();
  const { tabelas, dadosTabela, loading, error, listar, ler, exportar } =
    useTabelas();

  const [nomeTabela, setNomeTabela] = useState<string>("");
  const [camada, setCamada] = useState<CamadaTabela>("parquets");
  const [pagina, setPagina] = useState(1);
  const [filtroValor, setFiltroValor] = useState("");
  const [filtroColuna, setFiltroColuna] = useState<string>("__todas__");

  useEffect(() => {
    if (!cnpjAtivo) return;
    listar(cnpjAtivo, camada).catch(erro => {
      toast.error("Falha ao listar tabelas", {
        description: (erro as Error).message,
      });
    });
  }, [cnpjAtivo, camada, listar]);

  useEffect(() => {
    if (!cnpjAtivo || !nomeTabela) return;
    ler(
      cnpjAtivo,
      nomeTabela,
      camada,
      pagina,
      50,
      filtroColuna !== "__todas__" ? filtroColuna : undefined,
      filtroValor || undefined
    ).catch(erro => {
      toast.error("Falha ao carregar tabela", {
        description: (erro as Error).message,
      });
    });
  }, [cnpjAtivo, nomeTabela, camada, pagina, filtroColuna, filtroValor, ler]);

  const colunasTabela = useMemo(
    () => dadosTabela?.colunas ?? [],
    [dadosTabela]
  );

  const baixarTabela = async (formato: "xlsx" | "csv" | "parquet") => {
    if (!cnpjAtivo || !nomeTabela) return;
    try {
      await exportar(cnpjAtivo, nomeTabela, formato);
      toast.success("Exportacao concluida", {
        description: `${nomeTabela}.${formato}`,
      });
    } catch (erro) {
      toast.error("Falha ao exportar", {
        description: (erro as Error).message,
      });
    }
  };

  if (!cnpjAtivo) {
    return (
      <Card>
        <CardContent className="py-10 text-center text-sm text-muted-foreground">
          Selecione um CNPJ ativo no cabecalho para consultar tabelas.
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">
            Consulta de tabelas - {formatarCnpj(cnpjAtivo)}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-1 gap-3 md:grid-cols-4">
            <Select
              value={nomeTabela}
              onValueChange={value => {
                setNomeTabela(value);
                setPagina(1);
              }}
            >
              <SelectTrigger>
                <SelectValue placeholder="Selecione a tabela" />
              </SelectTrigger>
              <SelectContent>
                {tabelas.map(tabela => (
                  <SelectItem key={tabela.nome} value={tabela.nome}>
                    {tabela.nome}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select
              value={camada}
              onValueChange={value => {
                setCamada(value as CamadaTabela);
                setNomeTabela("");
                setPagina(1);
              }}
            >
              <SelectTrigger>
                <SelectValue placeholder="Selecione a camada" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="parquets">Gold</SelectItem>
                <SelectItem value="silver">Silver</SelectItem>
                <SelectItem value="extraidos">Extraidos</SelectItem>
              </SelectContent>
            </Select>

            <Select value={filtroColuna} onValueChange={setFiltroColuna}>
              <SelectTrigger>
                <SelectValue placeholder="Coluna de filtro" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="__todas__">Sem filtro de coluna</SelectItem>
                {colunasTabela.map(coluna => (
                  <SelectItem key={coluna} value={coluna}>
                    {coluna}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Input
              placeholder="Valor do filtro"
              value={filtroValor}
              onChange={event => {
                setFiltroValor(event.target.value);
                setPagina(1);
              }}
            />

            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                onClick={() => cnpjAtivo && listar(cnpjAtivo, camada)}
                className="gap-2"
              >
                <RefreshCw className="h-4 w-4" />
                Atualizar
              </Button>
              <Button
                variant="outline"
                onClick={() => baixarTabela("csv")}
                className="gap-2"
                disabled={!nomeTabela}
              >
                <Download className="h-4 w-4" />
                CSV
              </Button>
              <Button
                variant="outline"
                onClick={() => baixarTabela("xlsx")}
                className="gap-2"
                disabled={!nomeTabela}
              >
                XLSX
              </Button>
            </div>
          </div>

          {error ? <p className="text-xs text-destructive">{error}</p> : null}
        </CardContent>
      </Card>

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between text-sm font-semibold">
            <span>{nomeTabela || "Nenhuma tabela selecionada"}</span>
            {dadosTabela ? (
              <Badge variant="outline">
                {dadosTabela.total_registros} registros
              </Badge>
            ) : null}
          </CardTitle>
        </CardHeader>
        <CardContent>
          {!nomeTabela ? (
            <p className="text-sm text-muted-foreground">
              Selecione uma tabela para visualizar.
            </p>
          ) : loading ? (
            <p className="text-sm text-muted-foreground">Carregando...</p>
          ) : !dadosTabela || dadosTabela.dados.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              Tabela sem registros para os filtros informados.
            </p>
          ) : (
            <>
              <DataTable
                dados={dadosTabela.dados}
                colunas={dadosTabela.colunas}
              />

              <div className="mt-3 flex items-center justify-between text-xs">
                <span>
                  Pagina {dadosTabela.pagina} de {dadosTabela.total_paginas}
                </span>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={dadosTabela.pagina <= 1}
                    onClick={() => setPagina(atual => Math.max(1, atual - 1))}
                  >
                    Anterior
                  </Button>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={dadosTabela.pagina >= dadosTabela.total_paginas}
                    onClick={() => setPagina(atual => atual + 1)}
                  >
                    Proxima
                  </Button>
                </div>
              </div>
            </>
          )}
        </CardContent>
      </Card>
    </div>
  );
}
