import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { formatarCnpj, useCnpj } from "@/contexts/CnpjContext";
import Agregacao from "./Agregacao";
import Conversao from "./Conversao";

export default function PainelAgregacaoConversao() {
  const { cnpjAtivo } = useCnpj();

  if (!cnpjAtivo) {
    return (
      <Card>
        <CardContent className="py-10 text-center text-sm text-muted-foreground">
          Selecione um CNPJ ativo no cabecalho para acessar Agregacao e
          Conversao.
        </CardContent>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <CardTitle className="text-sm font-semibold">
            Agregacao e Conversao - {formatarCnpj(cnpjAtivo)}
          </CardTitle>
        </CardHeader>
      </Card>

      <Tabs defaultValue="agregacao" className="w-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="agregacao">Agregacao de Produtos</TabsTrigger>
          <TabsTrigger value="conversao">Conversao de Unidades</TabsTrigger>
        </TabsList>
        <TabsContent value="agregacao">
          <Agregacao />
        </TabsContent>
        <TabsContent value="conversao">
          <Conversao />
        </TabsContent>
      </Tabs>
    </div>
  );
}
